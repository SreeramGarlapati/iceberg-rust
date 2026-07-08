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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct, StructType, Summary,
    TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

pub(crate) fn generate_unique_snapshot_id(table: &Table) -> i64 {
    let generate_random_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        if snapshot_id < 0 {
            -snapshot_id
        } else {
            snapshot_id
        }
    };
    let mut snapshot_id = generate_random_id();

    while table
        .metadata()
        .snapshots()
        .any(|s| s.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_random_id();
    }
    snapshot_id
}

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # When it accomplishes
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the current snapshot should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method is intended for future delete
///    operations to specify which manifest entries should be marked as deleted.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<ProcessedManifests> {
        Ok(ProcessedManifests::new(manifests))
    }
}

/// The output of [`ManifestProcess::process_manifests`].
pub(crate) struct ProcessedManifests {
    /// Manifest files to include in the new snapshot.
    pub(crate) manifests: Vec<ManifestFile>,
    /// Summary properties describing the processing (e.g. `manifests-created`
    /// for a manifest rewrite). Merged into the snapshot summary with
    /// precedence over user-supplied snapshot properties and metrics derived
    /// from added data files.
    pub(crate) summary_properties: HashMap<String, String>,
}

impl ProcessedManifests {
    /// Creates a `ProcessedManifests` with no summary properties.
    pub(crate) fn new(manifests: Vec<ManifestFile>) -> Self {
        Self {
            manifests,
            summary_properties: HashMap::new(),
        }
    }

    /// Adds a summary property describing the processing.
    pub(crate) fn with_property(mut self, key: &str, value: impl ToString) -> Self {
        self.summary_properties
            .insert(key.to_string(), value.to_string());
        self
    }
}

/// A hook that transforms the set of manifest files included in a new snapshot.
///
/// Implementations may rewrite manifests entirely (e.g. manifest compaction),
/// which requires reading and writing manifest files, so the hook is fallible.
/// Writers for new manifests can be obtained from the [`SnapshotProducer`]
/// passed to the hook. Metrics describing the processing are reported back
/// through [`ProcessedManifests::summary_properties`].
pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> impl Future<Output = Result<ProcessedManifests>> + Send;
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Atomic so that writers can be created through a shared reference.
    manifest_counter: AtomicU64,
}

impl<'a> SnapshotProducer<'a> {
    pub(crate) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
    ) -> Self {
        Self {
            table,
            snapshot_id: generate_unique_snapshot_id(table),
            commit_uuid,
            key_metadata,
            snapshot_properties,
            added_data_files,
            manifest_counter: AtomicU64::new(0),
        }
    }

    pub(crate) fn validate_added_data_files(&self) -> Result<()> {
        for data_file in &self.added_data_files {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(());
        };

        let new_files: HashSet<&str> = self
            .added_data_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        let runtime = self.table.runtime();
        let file_io = self.table.file_io();
        let manifest_list = self
            .table
            .manifest_list_reader(current_snapshot)
            .load()
            .await?;

        let new_files_ref = &new_files;
        let referenced_files: Vec<String> = manifest_list
            .consume_entries()
            .into_iter()
            .map(|entry| {
                let file_io = file_io.clone();
                runtime
                    .io()
                    .spawn(async move { entry.load_manifest(&file_io).await })
            })
            .collect::<FuturesUnordered<_>>()
            .try_fold(Vec::new(), |mut acc, manifest| async move {
                acc.extend(
                    manifest?
                        .entries()
                        .iter()
                        .filter(|e| new_files_ref.contains(e.file_path()) && e.is_alive())
                        .map(|e| e.file_path().to_string()),
                );
                Ok(acc)
            })
            .await?;

        if !referenced_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add files that are already referenced by table, files: {}",
                    referenced_files.join(", ")
                ),
            ));
        }

        Ok(())
    }

    pub(crate) fn new_manifest_writer(
        &self,
        content: ManifestContentType,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.fetch_add(1, Ordering::Relaxed),
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            self.table
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone(),
        );
        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }

        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value
                && !field.compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Data)?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<ProcessedManifests> {
        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let mut manifest_files = existing_manifests;

        // Process added entries.
        if !self.added_data_files.is_empty() {
            let added_manifest = self.write_added_manifest().await?;
            manifest_files.push(added_manifest);
        }

        // # TODO
        // Support process delete entries.

        manifest_process
            .process_manifests(self, manifest_files)
            .await
    }

    // Returns summary derived from the data files added in this snapshot. Must
    // be called before manifest production, which consumes `self.added_data_files`.
    fn added_files_summary_properties(&self) -> HashMap<String, String> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        summary_collector.build()
    }

    // Assembles the `Summary` of the new snapshot. Properties are merged with
    // increasing precedence:
    // - user-supplied snapshot properties
    // - properties derived from added data files
    // - properties reported by process_manifest
    // - totals calculated from previous summaries
    // Computed values overwrite colliding user-supplied keys, so a user cannot
    // shadow them with a bad value that would corrupt the  snapshot summary.
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
        added_files_summary: HashMap<String, String>,
        process_summary: HashMap<String, String>,
    ) -> Result<Summary> {
        let mut additional_properties = self.snapshot_properties.clone();
        additional_properties.extend(added_files_summary);
        additional_properties.extend(process_summary);

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        let table_metadata = self.table.metadata_ref();
        let previous_snapshot = table_metadata.current_snapshot();

        update_snapshot_summaries(
            summary,
            previous_snapshot.map(|s| s.summary()),
            snapshot_produce_operation.operation() == Operation::Overwrite,
        )
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        let manifest_list_path = self.generate_manifest_list_file_path(0);
        let next_seq_num = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();
        let writer = self
            .table
            .file_io()
            .new_output(manifest_list_path.clone())?
            .writer()
            .await?;
        let mut manifest_list_writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                writer,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                writer,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                writer,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Metrics for the added data files must be captured before manifest
        // production, which consumes `self.added_data_files`.
        let added_files_summary = self.added_files_summary_properties();
        let new_processed_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;
        let new_manifests = new_processed_manifests.manifests;

        let summary = self.summary(
            &snapshot_produce_operation,
            added_files_summary,
            new_processed_manifests.summary_properties,
        )?;

        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, Literal};
    use crate::transaction::tests::make_v2_minimal_table;

    struct AppendTestOperation;

    impl SnapshotProduceOperation for AppendTestOperation {
        fn operation(&self) -> Operation {
            Operation::Append
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
            Ok(vec![])
        }
    }

    /// A manifest process that reports summary properties, mimicking a
    /// rewrite-style operation whose metrics are only known after processing.
    struct SummaryReportingProcess;

    impl ManifestProcess for SummaryReportingProcess {
        async fn process_manifests(
            &self,
            _snapshot_produce: &SnapshotProducer<'_>,
            manifests: Vec<ManifestFile>,
        ) -> Result<ProcessedManifests> {
            Ok(ProcessedManifests {
                manifests,
                summary_properties: HashMap::from([
                    ("manifests-created".to_string(), "7".to_string()),
                    ("shared-key".to_string(), "process-value".to_string()),
                ]),
            })
        }
    }

    #[tokio::test]
    async fn test_process_summary_properties_reach_snapshot_summary() {
        let table = make_v2_minimal_table();
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let snapshot_properties = HashMap::from([
            ("user-key".to_string(), "user-value".to_string()),
            // Colliding keys must lose to computed metrics and to
            // process-reported properties respectively.
            ("added-data-files".to_string(), "999".to_string()),
            ("shared-key".to_string(), "user-value".to_string()),
        ]);
        let producer =
            SnapshotProducer::new(&table, Uuid::now_v7(), None, snapshot_properties, vec![
                data_file,
            ]);

        let mut action_commit = producer
            .commit(AppendTestOperation, SummaryReportingProcess)
            .await
            .unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            panic!("first update should be AddSnapshot");
        };
        let props = &snapshot.summary().additional_properties;

        // Properties reported by the manifest process are included.
        assert_eq!(props.get("manifests-created").unwrap(), "7");
        // Non-colliding user properties are preserved.
        assert_eq!(props.get("user-key").unwrap(), "user-value");
        // Computed metrics overwrite colliding user-supplied values.
        assert_eq!(props.get("added-data-files").unwrap(), "1");
        // Process-reported properties overwrite colliding user-supplied values.
        assert_eq!(props.get("shared-key").unwrap(), "process-value");
        // Totals are derived against the fully-assembled property map.
        assert_eq!(props.get("total-data-files").unwrap(), "1");
    }
}
