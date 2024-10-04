use crate::df_extension::serialize::make_row_key;
use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::cast::as_string_array;
use datafusion_common::DataFusionError;
use datafusion_common::{arrow_datafusion_err, exec_err, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties,
};
use futures::executor::block_on;
use futures::StreamExt;
use rocksdb::DB;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[allow(dead_code)]
pub struct DeleteExec {
    table_id: u64,
    db: Arc<DB>,
    out_schema: SchemaRef, // output schema?
    in_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl DeleteExec {
    pub fn new(
        table_id: u64,
        db: &Arc<DB>,
        input_schema: &SchemaRef,
        output_schema: &SchemaRef,
        input: &Arc<dyn ExecutionPlan>,
    ) -> Self {
        let cache = Self::compute_properties(output_schema);
        Self {
            table_id,
            db: Arc::clone(db),
            in_schema: Arc::clone(input_schema),
            out_schema: Arc::clone(output_schema),
            input: Arc::clone(input),
            cache,
        }
    }

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        let eq_prop = EquivalenceProperties::new(Arc::clone(schema));

        PlanProperties::new(
            eq_prop,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }

    async fn delete_batches(&self, input: SendableRecordBatchStream) -> Result<u64> {
        let mut input = input;
        let mut cnt = 0u64;
        // Currently only support 1 batch
        if let Some(batch) = input.next().await.transpose()? {
            cnt += self.delete_batch_from_table(batch)?;
        }
        Ok(cnt)
    }

    /// This method takes in the batch that is supposed to be deleted, and
    /// delete them from the table of self.table_id, and
    /// return rows deleted
    fn delete_batch_from_table(&self, batch: RecordBatch) -> Result<u64> {
        let target = batch.columns().last().unwrap(); // by design, the `row_id` is the last column

        // Iter over the `target` array to get each `row_id` to delete
        let mut cnt = 0u64;
        let target = as_string_array(target)?;
        for field in self.in_schema.fields.iter() {
            let name = field.name();
            if name.eq("row_id") {
                continue; // break
            }
            for id in target {
                match id {
                    None => {
                        todo!()
                    }
                    Some(id) => {
                        cnt += 1;
                        // TODO: Change type of `row_id` from Utf8 to u64
                        let key = make_row_key(self.table_id, name, id.parse().unwrap());
                        self.db.delete(key).unwrap();
                    }
                }
            }
        }
        Ok(cnt)
    }
}

impl Debug for DeleteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteExec")
    }
}

impl DisplayAs for DeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            // TODO: not sure if it's proper to print self.input
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeleteExec: todo")
            }
        }
    }
}

impl ExecutionPlan for DeleteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeleteExec::new(
            self.table_id,
            &self.db,
            &self.in_schema,
            &self.out_schema,
            &children[0],
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return exec_err!("DeleteExec invalid partition {partition}");
        }

        let to_delete = self.input.execute(0, context)?;

        let row_deleted = match block_on(self.delete_batches(to_delete)) {
            Ok(deleted) => deleted,
            Err(e) => return exec_err!("Internal error of DeleteExec: {e}"),
        };
        let arr = Arc::new(UInt64Array::from(vec![row_deleted]));
        let batch = RecordBatch::try_new(self.out_schema.clone() as SchemaRef, vec![arr])
            .map_err(|e| arrow_datafusion_err!(e));

        let stream = futures::stream::iter(vec![batch]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.out_schema),
            Box::pin(stream),
        )))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::df_extension::planner::query_planner::DeleteQueryPlanner;
    use crate::df_extension::provider::kvtable::{KVTable, KVTableMeta};
    use crate::df_extension::rule::delete_rule::DeleteReplaceRule;
    use arrow::array::StringArray;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::cast::as_uint64_array;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnv;

    #[tokio::test]
    async fn basic_delete_test() {
        let db = Arc::new(DB::open_default("./tmp").unwrap());

        // Registering the custom planners and rule
        let config = SessionConfig::new().with_target_partitions(1);
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::new(DeleteQueryPlanner::new(1003, Arc::clone(&db))))
            .with_optimizer_rule(Arc::new(DeleteReplaceRule {}))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Create table for testing and register
        let schema = Arc::new(Schema::new(Fields::from(vec![
            Arc::new(Field::new("c1", DataType::Utf8, false)),
            Arc::new(Field::new("row_id", DataType::Utf8, false)),
        ])));
        let table_meta = Arc::new(KVTableMeta {
            id: 1003,
            name: "t".to_string(),
            schema: Arc::clone(&schema),
            highest: 0, // default, unknown
        });
        let table_provider = Arc::new(KVTable::new(&table_meta, Arc::clone(&db)));

        // Since create table is not used here, we have to init the meta of table in db first
        let meta_key = table_meta.make_key();
        let meta_val = table_meta.make_value();
        db.put(meta_key, meta_val).unwrap();
        ctx.register_table("t", table_provider).unwrap();

        // Now insert table with init data
        let inserted = ctx
            .sql("INSERT INTO t VALUES('hello', '001'), ('world', '002'), ('!', '003')")
            .await
            .unwrap();
        // Should have inserted 3 row
        let bind = inserted.collect().await.unwrap();
        let batch = bind.first().unwrap();
        let t = as_uint64_array(&batch.columns()[0]).unwrap();
        assert_eq!(t.value(0), 3);

        // Now try to delete from the table
        let deleted = ctx.sql("DELETE FROM t where c1='!'").await.unwrap();
        let _plan = deleted.logical_plan().to_string();
        let bind = deleted.collect().await.unwrap();
        let batch = bind.first().unwrap();
        let t = as_uint64_array(&batch.columns()[0]).unwrap();
        // Should have deleted 1 row
        assert_eq!(t.value(0), 1);

        // Now scan the table and compare
        let expected = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["hello", "world"])),
                Arc::new(StringArray::from(vec!["000001", "000002"])),
            ],
        )
        .unwrap();
        let df = ctx.sql("SELECT c1 FROM t").await.unwrap();
        let bind = df.collect().await.unwrap();
        let res = pretty_format_batches(bind.as_slice()).unwrap();
        println!("{}", res);
        let out = bind.first().unwrap();
        assert_eq!(out, &expected);
    }
}
