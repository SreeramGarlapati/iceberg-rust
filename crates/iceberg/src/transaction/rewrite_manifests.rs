// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, ENTRIES_PROCESSED, FormatVersion, MANIFESTS_CREATED, MANIFESTS_KEPT,
    MANIFESTS_REPLACED, ManifestContentType, ManifestEntry, ManifestFile, Operation, Struct,
    TableProperties,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    ManifestProcess, ProcessedManifests, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

const FALLBACK_BYTES_PER_ENTRY: u64 = 256;

pub struct RewriteManifestsAction {
    target_size_bytes: Option<u64>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            target_size_bytes: None,
            snapshot_properties: HashMap::new(),
        }
    }

    pub fn set_target_size_bytes(mut self, target_size_bytes: u64) -> Self {
        self.target_size_bytes = Some(target_size_bytes);
        self
    }

    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let Some(current_snapshot) = metadata.current_snapshot() else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "RewriteManifests requires the table to have a current snapshot",
            ));
        };

        let target_size_bytes = self.target_size_bytes.unwrap_or_else(|| {
            metadata
                .properties()
                .get(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT)
        });
        let default_spec_id = metadata.default_partition_spec_id();

        let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;

        let (to_rewrite, kept): (Vec<ManifestFile>, Vec<ManifestFile>) =
            manifest_list.entries().iter().cloned().partition(|m| {
                m.content == ManifestContentType::Data
                    && m.partition_spec_id == default_spec_id
                    && (m.has_added_files() || m.has_existing_files())
            });

        if to_rewrite.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let total_size: u64 = to_rewrite
            .iter()
            .map(|m| u64::try_from(m.manifest_length).unwrap_or(0))
            .sum();
        if to_rewrite.len() == 1 && total_size <= target_size_bytes {
            return Ok(ActionCommit::new(vec![], vec![]));
        }
        let total_input_entries: u64 = to_rewrite
            .iter()
            .map(|m| {
                u64::from(m.added_files_count.unwrap_or(0))
                    + u64::from(m.existing_files_count.unwrap_or(0))
            })
            .sum();
        let bytes_per_entry = total_size
            .checked_div(total_input_entries)
            .map_or(FALLBACK_BYTES_PER_ENTRY, |b| b.max(1));

        let producer = SnapshotProducer::new(
            table,
            Uuid::now_v7(),
            None,
            self.snapshot_properties.clone(),
            vec![],
        );
        producer
            .commit(
                RewriteManifestsOperation { kept },
                RewriteManifestsProcess {
                    to_rewrite,
                    target_size_bytes,
                    bytes_per_entry,
                },
            )
            .await
    }
}

/// Emits a `Replace` snapshot. The manifests the rewrite leaves untouched are
/// carried forward here; the rewritten ones are produced by
/// [`RewriteManifestsProcess`].
struct RewriteManifestsOperation {
    kept: Vec<ManifestFile>,
}

impl SnapshotProduceOperation for RewriteManifestsOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(self.kept.clone())
    }
}

/// Rewrites the selected manifests into new ones rolled by target size,
/// grouping live entries by partition tuple and preserving their
/// `sequence_number`, `file_sequence_number`, and (v3) `first_row_id`.
struct RewriteManifestsProcess {
    to_rewrite: Vec<ManifestFile>,
    target_size_bytes: u64,
    bytes_per_entry: u64,
}

/// A live entry lifted out of a source manifest, carrying the fields
/// [`ManifestWriter::add_existing_file`] needs to re-emit it.
///
/// [`ManifestWriter::add_existing_file`]: crate::spec::ManifestWriter::add_existing_file
struct ExistingEntry {
    data_file: DataFile,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
}

impl ManifestProcess for RewriteManifestsProcess {
    async fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        kept: Vec<ManifestFile>,
    ) -> Result<ProcessedManifests> {
        let table = snapshot_produce.table;
        let format_version = table.metadata().format_version();

        let loaded: Vec<_> = stream::iter(self.to_rewrite.clone())
            .map(|m| {
                let file_io = table.file_io().clone();
                async move {
                    let manifest = m.load_manifest(&file_io).await?;
                    Ok::<_, Error>((m, manifest))
                }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        let mut grouped: Vec<Vec<ExistingEntry>> = Vec::new();
        let mut group_index: HashMap<Struct, usize> = HashMap::new();
        let mut entries_processed: u64 = 0;

        for (manifest_file, manifest) in loaded {
            // Per the v3 first-row-id inheritance rules, an entry with a null
            // `first_row_id` derives it from the manifest's `first_row_id`
            // plus the record counts of the null-`first_row_id` entries that
            // precede it. The derived value must be written explicitly when
            // the entry is copied into a new manifest, where the original
            // base is no longer available.
            let mut inherited_row_id = manifest_file.first_row_id.map(|v| v as i64);
            for entry in manifest.entries() {
                let entry_first_row_id = entry.data_file().first_row_id;
                let effective_first_row_id = entry_first_row_id.or(inherited_row_id);
                if entry_first_row_id.is_none()
                    && let Some(base) = inherited_row_id.as_mut()
                {
                    *base += entry.data_file().record_count() as i64;
                }
                if !entry.is_alive() {
                    continue;
                }
                let snap_id = entry.snapshot_id().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Live manifest entry is missing snapshot_id",
                    )
                })?;
                let seq = entry.sequence_number().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Live manifest entry is missing sequence_number",
                    )
                })?;
                let mut data_file = entry.data_file().clone();
                data_file.first_row_id = effective_first_row_id;
                let idx = match group_index.get(&data_file.partition) {
                    Some(&i) => i,
                    None => {
                        let i = grouped.len();
                        group_index.insert(data_file.partition.clone(), i);
                        grouped.push(Vec::new());
                        i
                    }
                };
                grouped[idx].push(ExistingEntry {
                    data_file,
                    snapshot_id: snap_id,
                    sequence_number: seq,
                    file_sequence_number: entry.file_sequence_number,
                });
                entries_processed += 1;
            }
        }

        let mut new_manifests: Vec<ManifestFile> = Vec::with_capacity(grouped.len());

        for group in grouped {
            let mut writer = snapshot_produce.new_manifest_writer(ManifestContentType::Data)?;
            let mut accumulated: u64 = 0;
            let mut min_first_row_id: Option<u64> = None;

            for entry in group {
                if accumulated > 0
                    && accumulated.saturating_add(self.bytes_per_entry) > self.target_size_bytes
                {
                    let mut written = writer.write_manifest_file().await?;
                    if format_version == FormatVersion::V3 {
                        written.first_row_id = min_first_row_id;
                    }
                    new_manifests.push(written);
                    writer = snapshot_produce.new_manifest_writer(ManifestContentType::Data)?;
                    accumulated = 0;
                    min_first_row_id = None;
                }
                if let Some(frid_u) = entry
                    .data_file
                    .first_row_id
                    .and_then(|f| u64::try_from(f).ok())
                {
                    min_first_row_id = Some(min_first_row_id.map_or(frid_u, |m| m.min(frid_u)));
                }
                writer.add_existing_file(
                    entry.data_file,
                    entry.snapshot_id,
                    entry.sequence_number,
                    entry.file_sequence_number,
                )?;
                accumulated = accumulated.saturating_add(self.bytes_per_entry);
            }
            let mut written = writer.write_manifest_file().await?;
            if format_version == FormatVersion::V3 {
                written.first_row_id = min_first_row_id;
            }
            new_manifests.push(written);
        }

        let manifests_created = new_manifests.len();
        let manifests_kept = kept.len();

        // New manifests first, then the untouched ones, matching the order of
        // the previous standalone commit path.
        let mut manifests = new_manifests;
        manifests.extend(kept);

        Ok(ProcessedManifests::new(manifests)
            .with_property(MANIFESTS_CREATED, manifests_created)
            .with_property(MANIFESTS_REPLACED, self.to_rewrite.len())
            .with_property(MANIFESTS_KEPT, manifests_kept)
            .with_property(ENTRIES_PROCESSED, entries_processed))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Catalog, TableUpdate};

    fn data_file(name: &str, partition: i64, size: u64, records: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{name}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(records)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(partition))]))
            .build()
            .unwrap()
    }

    async fn append_one(catalog: &impl Catalog, table: Table, file: DataFile) -> Table {
        let tx = Transaction::new(&table);
        tx.fast_append()
            .add_data_files(vec![file])
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_no_current_snapshot_errors() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("expected error"),
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::PreconditionFailed),
        }
    }

    #[tokio::test]
    async fn test_single_small_manifest_is_noop() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let original_snapshot_id = table.metadata().current_snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert!(updates.is_empty());

        let table = tx
            .rewrite_manifests()
            .apply(Transaction::new(&table))
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(
            table.metadata().current_snapshot_id(),
            original_snapshot_id,
            "no-op should not change the current snapshot"
        );
    }

    #[tokio::test]
    async fn test_multi_manifest_merge_preserves_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_one(&catalog, table, data_file("a", 1, 1_000, 10)).await;
        let seq_a = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let table = append_one(&catalog, table, data_file("b", 1, 2_000, 20)).await;
        let seq_b = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let table = append_one(&catalog, table, data_file("c", 2, 3_000, 30)).await;
        let seq_c = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        assert!(seq_a < seq_b && seq_b < seq_c);

        let pre_manifest_count = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(pre_manifest_count, 3);

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let post_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let total_entries: usize = {
            let mut n = 0;
            for m in post_list.entries() {
                let manifest = m.load_manifest(table.file_io()).await.unwrap();
                n += manifest.entries().len();
            }
            n
        };
        assert_eq!(total_entries, 3, "all entries preserved across rewrite");

        let mut seen_seqs: Vec<i64> = Vec::new();
        for m in post_list.entries() {
            let manifest = m.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                seen_seqs.push(entry.sequence_number().unwrap());
            }
        }
        seen_seqs.sort();
        assert_eq!(seen_seqs, vec![seq_a, seq_b, seq_c]);

        assert!(post_list.entries().len() < pre_manifest_count);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-records").unwrap(), "60");
        assert_eq!(summary.get("total-data-files").unwrap(), "3");
        assert_eq!(summary.get("entries-processed").unwrap(), "3");
        assert_eq!(summary.get("manifests-replaced").unwrap(), "3");
    }

    #[tokio::test]
    async fn test_target_size_from_table_property() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..6 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 10_000, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .update_table_properties()
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "400".to_string(),
            )
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert!(post_list.entries().len() > 1);
    }

    #[tokio::test]
    async fn test_target_size_rolls_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..6 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 10_000, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .set_target_size_bytes(400)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert!(post_list.entries().len() > 1);

        let mut total = 0;
        for m in post_list.entries() {
            let manifest = m.load_manifest(t.file_io()).await.unwrap();
            let n = manifest.entries().len();
            assert!(
                n <= 2,
                "each rolled manifest should hold at most ~2 entries when target is just above bytes_per_entry"
            );
            total += n;
        }
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn test_oversized_single_manifest_is_split() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let files: Vec<_> = (0..6)
            .map(|i| data_file(&format!("f{i}"), 1, 10_000, 1))
            .collect();

        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let pre_list = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert_eq!(pre_list.entries().len(), 1);

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .set_target_size_bytes(400)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snap = table.metadata().current_snapshot().unwrap();
        let post_list = table.manifest_list_reader(snap).load().await.unwrap();
        assert!(post_list.entries().len() > 1);
        assert_eq!(
            snap.summary()
                .additional_properties
                .get("manifests-replaced")
                .unwrap(),
            "1"
        );

        let mut total = 0;
        for m in post_list.entries() {
            total += m
                .load_manifest(table.file_io())
                .await
                .unwrap()
                .entries()
                .len();
        }
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn test_v3_row_lineage_preserved() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 30)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 17)).await;
        let table = append_one(&catalog, table, data_file("c", 1, 100, 11)).await;

        /// Collects each live entry's (path, effective first_row_id, sequence
        /// numbers), deriving null `first_row_id`s from the manifest's base
        /// per the v3 inheritance rules, so entries compare by their logical
        /// row ids whether stored explicitly or inherited.
        async fn collect(t: &Table) -> Vec<(String, Option<i64>, Option<i64>, Option<i64>)> {
            let list = t
                .manifest_list_reader(t.metadata().current_snapshot().unwrap())
                .load()
                .await
                .unwrap();
            let mut v = Vec::new();
            for m in list.entries() {
                let manifest = m.load_manifest(t.file_io()).await.unwrap();
                let mut inherited_row_id = m.first_row_id.map(|b| b as i64);
                for entry in manifest.entries() {
                    let entry_first_row_id = entry.data_file().first_row_id;
                    let effective_first_row_id = entry_first_row_id.or(inherited_row_id);
                    if entry_first_row_id.is_none()
                        && let Some(base) = inherited_row_id.as_mut()
                    {
                        *base += entry.data_file().record_count() as i64;
                    }
                    v.push((
                        entry.data_file().file_path().to_string(),
                        effective_first_row_id,
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    ));
                }
            }
            v.sort();
            v
        }

        let pre = collect(&table).await;
        let next_row_id_before = table.metadata().next_row_id();

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            table.metadata().next_row_id(),
            next_row_id_before,
            "rewrite must not consume new row ids"
        );
        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(snap.row_range(), Some((next_row_id_before, 0)));

        let post = collect(&table).await;
        assert_eq!(
            pre, post,
            "first_row_id, sequence_number, and file_sequence_number must be preserved per-entry"
        );
    }

    #[tokio::test]
    async fn test_summary_and_replace_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 10)).await;
        let table = append_one(&catalog, table, data_file("b", 2, 200, 20)).await;

        let pre_total_files_size = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get("total-files-size")
            .cloned();

        let tx = Transaction::new(&table);
        let mut commit = Arc::new(
            tx.rewrite_manifests()
                .set_snapshot_properties(HashMap::from([(
                    "trigger".to_string(),
                    "manual".to_string(),
                )])),
        )
        .commit(&table)
        .await
        .unwrap();
        let updates = commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot: snap } = &updates[0] else {
            unreachable!()
        };
        let s = &snap.summary().additional_properties;
        assert_eq!(snap.summary().operation, Operation::Replace);
        assert_eq!(s.get("trigger").unwrap(), "manual");
        assert_eq!(s.get("entries-processed").unwrap(), "2");
        assert_eq!(s.get("manifests-replaced").unwrap(), "2");
        assert_eq!(s.get("manifests-kept").unwrap(), "0");
        assert_eq!(s.get("manifests-created").unwrap(), "2");
        assert_eq!(s.get("total-records").unwrap(), "30");
        assert_eq!(s.get("total-files-size").cloned(), pre_total_files_size);
    }

    #[tokio::test]
    async fn test_idempotent_after_consolidation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..3 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 100, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let post_count = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(post_count, 1, "single partition consolidates to 1 manifest");

        let mut commit2 = Arc::new(Transaction::new(&t).rewrite_manifests())
            .commit(&t)
            .await
            .unwrap();
        assert!(
            commit2.take_updates().is_empty(),
            "second rewrite must no-op when input already fits in one manifest"
        );
    }

    #[tokio::test]
    async fn test_partition_grouping_and_catalog_commit() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("c", 2, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("d", 2, 100, 1)).await;

        let pre_id = table.metadata().current_snapshot_id().unwrap();
        let pre_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(snap.parent_snapshot_id(), Some(pre_id));
        assert_eq!(snap.sequence_number(), pre_seq + 1);
        assert_ne!(snap.snapshot_id(), pre_id);

        let post_list = table.manifest_list_reader(snap).load().await.unwrap();
        assert_eq!(post_list.entries().len(), 2);
        for m in post_list.entries() {
            let manifest = m.load_manifest(table.file_io()).await.unwrap();
            let partitions: std::collections::HashSet<Struct> = manifest
                .entries()
                .iter()
                .map(|e| e.data_file().partition.clone())
                .collect();
            assert_eq!(
                partitions.len(),
                1,
                "all entries within a manifest share one partition tuple"
            );
        }
    }

    /// Returns each live entry's explicitly stored `first_row_id`, keyed by
    /// file path, asserting along the way that every rewritten entry stores
    /// one and that each manifest's `first_row_id` is the minimum of its
    /// entries' values (so the manifest-list writer never assigns the
    /// manifest a fresh row range).
    async fn explicit_row_ids_by_path(table: &Table) -> HashMap<String, i64> {
        let snap = table.metadata().current_snapshot().unwrap();
        let list = table.manifest_list_reader(snap).load().await.unwrap();
        let mut by_path = HashMap::new();
        for m in list.entries() {
            let manifest = m.load_manifest(table.file_io()).await.unwrap();
            let mut min_row_id: Option<i64> = None;
            for entry in manifest.entries() {
                let row_id = entry.data_file().first_row_id.expect(
                    "rewritten entries must store first_row_id explicitly; the new manifest \
                     provides no base to re-derive them from",
                );
                min_row_id = Some(min_row_id.map_or(row_id, |v| v.min(row_id)));
                by_path.insert(entry.data_file().file_path().to_string(), row_id);
            }
            assert_eq!(
                m.first_row_id,
                min_row_id.map(|v| v as u64),
                "a rewritten manifest's first_row_id must be the minimum of its entries' \
                 explicit values"
            );
        }
        by_path
    }

    #[tokio::test]
    async fn test_rewrite_materializes_inherited_first_row_ids() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let base = table.metadata().next_row_id() as i64;

        // A single fast-append of several files produces one manifest whose
        // entries all store a null `first_row_id`: each entry's logical value
        // is derived on read from the manifest's `first_row_id` plus the
        // record counts of the null entries preceding it (a: base, b: base+10,
        // c: base+30).
        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(vec![
                data_file("a", 1, 100, 10),
                data_file("b", 1, 100, 20),
                data_file("c", 1, 100, 30),
            ])
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        // A second manifest so the rewrite is not a single-manifest no-op.
        let table = append_one(&catalog, table, data_file("d", 1, 100, 5)).await;
        let next_row_id_before = table.metadata().next_row_id();

        // Sanity-check the setup: the source entries rely on inheritance, so
        // a rewrite that failed to materialize their `first_row_id` would
        // silently change the rows' logical ids.
        let pre_list = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        for m in pre_list.entries() {
            assert!(m.first_row_id.is_some());
            let manifest = m.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                assert!(entry.data_file().first_row_id.is_none());
            }
        }

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            table.metadata().next_row_id(),
            next_row_id_before,
            "rewrite must not consume new row ids"
        );
        let mut expected: HashMap<String, i64> = HashMap::from([
            ("test/a.parquet".to_string(), base),
            ("test/b.parquet".to_string(), base + 10),
            ("test/c.parquet".to_string(), base + 30),
            ("test/d.parquet".to_string(), base + 60),
        ]);
        assert_eq!(explicit_row_ids_by_path(&table).await, expected);

        // A second rewrite consolidates a mix of explicit entries (from the
        // first rewrite) and a null entry (from a fresh append): explicit
        // values must be taken as stored, never re-derived or re-assigned.
        let table = append_one(&catalog, table, data_file("e", 1, 100, 7)).await;
        let next_row_id_before = table.metadata().next_row_id();
        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(table.metadata().next_row_id(), next_row_id_before);
        expected.insert("test/e.parquet".to_string(), base + 65);
        assert_eq!(explicit_row_ids_by_path(&table).await, expected);
    }
}
