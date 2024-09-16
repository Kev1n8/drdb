use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_common::arrow::array::{ArrayRef, StringArray};
use datafusion_common::{arrow_datafusion_err, internal_err, Result};

use datafusion_common::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::storage::kv::KVTable;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use rocksdb::{IteratorMode, DB};

#[derive(Debug)]
pub struct DBTableScanExec {
    id: u64,
    db: Arc<DB>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl DBTableScanExec {
    pub fn new(id: u64, target: &SchemaRef, src: &KVTable) -> Self {
        let cache = Self::compute_properties(target.clone());
        Self {
            id,
            schema: target.clone(),
            db: Arc::clone(&src.db),
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::RoundRobinBatch(1),
            ExecutionMode::Bounded,
        )
    }

    fn read_columns(&self) -> Vec<ArrayRef> {
        let mut result: Vec<ArrayRef> = Vec::new();

        for col in &self.schema.fields {
            let start_key = format!("t{}_c{}_r000001", self.id, col.name());
            let prefix = format!("t{}_c{}", self.id, col.name());

            let mut iter = self.db.iterator(IteratorMode::From(
                start_key.as_bytes(),
                rocksdb::Direction::Forward,
            ));
            let mut values = Vec::new();

            while let Some(Ok((key, value))) = iter.next() {
                // Safety:
                // All keys and values are supposed to be encoded from utf8
                unsafe {
                    let key_str = String::from_utf8_unchecked(key.to_vec());
                    if !key_str.starts_with(prefix.as_str()) {
                        break;
                    }
                    let value_str = String::from_utf8_unchecked(value.to_vec());
                    values.push(value_str);
                }
            }

            let array: ArrayRef = Arc::new(StringArray::from(values));
            result.push(array);
        }
        result
    }
}

impl DisplayAs for DBTableScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DBTableScan(table_id={})", self.id)
            }
        }
    }
}

impl ExecutionPlan for DBTableScanExec {
    fn name(&self) -> &str {
        "DBTableScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // ValuesExec has a single output partition
        if 0 != partition {
            return internal_err!(
                "ValuesExec invalid partition {partition} (expected 0)"
            );
        }

        let columns = self.read_columns();
        let schema = Arc::clone(&self.schema);
        let batch = RecordBatch::try_new(schema.clone() as SchemaRef, columns)
            .map_err(|e| arrow_datafusion_err!(e));

        let stream = futures::stream::iter(vec![batch]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema as SchemaRef,
            Box::pin(stream),
        )))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::kv::{KVTable, KVTableMeta};
    use arrow::array::as_string_array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use rocksdb::DB;

    #[tokio::test]
    async fn test_read_from_db() {
        let meta = Arc::new(KVTableMeta {
            id: 101,
            name: "a".to_string(),
            schema: Arc::new(Schema::new(vec![Field::new(
                "hello",
                DataType::Utf8,
                false,
            )])),
            highest: 0,
        });

        let key_prefix = "t101_chello_r";
        let target =
            Arc::new(Schema::new(vec![Field::new("hell", DataType::Utf8, false)]));

        let db = DB::open_default("./tmp").unwrap();
        let src = KVTable::new(&meta, Arc::new(db));
        for i in 0..100i32 {
            let key = format!("{key_prefix}{i:0>6}").into_bytes();
            src.db.put(key, format!("{i}").into_bytes()).unwrap();
        }

        let table_exec = DBTableScanExec::new(101, &target, &src);
        let result = table_exec.execute(0, Arc::new(TaskContext::default()));
        let output = match result {
            Ok(out) => out,
            Err(_) => {
                assert_eq!(9, 1);
                return;
            }
        };

        let mut stream = output;
        while let Some(batch) = stream.next().await {
            if let Ok(batch) = batch {
                assert_eq!(batch.num_rows(), 100);

                let col = batch.columns();
                let col = col.first().unwrap();
                for (i, row) in as_string_array(col).iter().enumerate() {
                    assert_eq!(row, Some(format!("{i}").as_str()))
                    // println!("{i} {}", row.unwrap());
                }
            }
        }
    }
}
