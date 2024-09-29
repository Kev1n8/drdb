use crate::db_datafusion_error;
use crate::df_extension::serialize::{make_meta_key, make_row_key};
use crate::df_extension::table_provider::table_provider::{KVTableMeta, KVTableMetaRef};
use arrow::array::{as_string_array, ArrayRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::insert::DataSink;
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use rocksdb::DB;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub struct KVTableSink {
    id: u64,
    db: Arc<DB>,
}

impl KVTableSink {
    pub fn new(id: u64, src: &Arc<DB>) -> Self {
        Self {
            id,
            db: Arc::clone(src),
        }
    }

    pub fn put_meta(&self, meta: &KVTableMetaRef) -> Result<()> {
        let key = meta.make_key();
        let val = meta.make_value();
        match self.db.put(key, val) {
            Ok(()) => Ok(()),
            Err(e) => Err(db_datafusion_error!(e)),
        }
    }

    fn update_highest(&self, old_meta: &KVTableMetaRef, new_highest: u64) -> Result<()> {
        let new_meta = KVTableMeta {
            id: self.id,
            name: old_meta.name.clone(),
            schema: old_meta.schema.clone(),
            highest: new_highest,
        };

        let meta_key = new_meta.make_key();
        let new_val = new_meta.make_value();
        self.db
            .put(meta_key, new_val)
            .map_err(|e| db_datafusion_error!(e))
    }

    fn put_array(&self, name: &str, arr: &ArrayRef, start: u64) -> Result<()> {
        let mut counter = start;

        let arr = as_string_array(arr);
        for row in arr {
            let key = make_row_key(self.id, name, counter + 1);
            match row {
                Some(str) => match self.db.put(key, str.as_bytes()) {
                    Ok(_) => counter += 1,
                    Err(e) => return Err(db_datafusion_error!(e)),
                },
                None => todo!(),
            }
        }
        Ok(())
    }

    pub async fn put_batch_into_db(&self, batch: &RecordBatch) -> Result<u64> {
        let mut row_added = 0u64;
        let meta_key = make_meta_key(self.id);
        let meta_val = self
            .db
            .get(meta_key)
            .map_err(|e| DataFusionError::External(e.into()))?
            .unwrap();
        let meta = KVTableMeta::from(meta_val);
        let start = meta.highest;
        for (index, arr) in batch.columns().iter().enumerate() {
            let schema = batch.schema();
            let name = schema.fields[index].name();
            if name.eq("row_id") {
                continue; // row_id is stored in the key potentially
            }
            self.put_array(name.as_str(), arr, start)?;
        }
        // Update the highest index
        row_added += batch.num_rows() as u64;
        self.update_highest(&Arc::new(meta), row_added + start)?;
        Ok(row_added)
    }
}

impl DisplayAs for KVTableSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DBTableWrite(table_id={})", self.id)
            }
        }
    }
}

#[async_trait]
impl DataSink for KVTableSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut data = data;
        let mut cnt = 0;
        if let Some(batch) = data.next().await.transpose()? {
            cnt = self.put_batch_into_db(&batch).await?;
        }
        Ok(cnt)
    }
}
