use crate::storage::serialize::make_row_key;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
use datafusion_common::cast::as_string_array;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{
    arrow_datafusion_err, exec_err, DFSchemaRef, DataFusionError, Result,
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{DmlStatement, Extension, WriteOp};
use datafusion_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::executor::block_on;
use futures::StreamExt;
use rocksdb::DB;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

#[allow(dead_code)]
struct DeleteQueryPlanner {
    table_id: u64,
    db: Arc<DB>,
}

#[async_trait]
impl QueryPlanner for DeleteQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            DeletePlanner {
                table_id: self.table_id,
                db: Arc::clone(&self.db),
            },
        )]);

        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[allow(dead_code)]
struct DeleteReplaceRule {}

impl OptimizerRule for DeleteReplaceRule {
    fn name(&self) -> &str {
        "supporting delete"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Some(ApplyOrder::TopDown)
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Dml(DmlStatement {
            table_name,
            op: WriteOp::Delete,
            input,         // table source?
            output_schema, // single count
            ..
        }) = plan
        {
            // input should have only 1 item, and
            // it can only be `LogicalPlan::Filter`, or
            // `LogicalPlan::Scan`?
            let _name = table_name.to_string();
            let _schema = input.schema().to_string();
            let _inputt = input.as_ref().to_string();
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(DeletePlanNode {
                    input: input.as_ref().clone(),
                    schema: output_schema,
                    expr: input.expressions(),
                }),
            })))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[derive(Eq, PartialEq, Hash)]
#[allow(dead_code)]
struct DeletePlanNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    expr: Vec<Expr>,
}

impl Debug for DeletePlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for DeletePlanNode {
    fn name(&self) -> &str {
        "Delete"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expr.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        // TODO: check if this print out correctly
        write!(f, "Delete: {:?}", self.input.expressions())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        // TODO: Not sure what `input` and `expr` look like here
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        let _debug = format!("{:?}", &exprs);
        let _fields = self.schema.fields().clone();
        Ok(Self {
            input: inputs.swap_remove(0),
            schema: Arc::clone(&self.schema),
            expr: exprs,
        })
    }
}

#[allow(dead_code)]
struct DeletePlanner {
    table_id: u64,
    db: Arc<DB>,
}

#[async_trait]
impl ExtensionPlanner for DeletePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let node =
            if let Some(delete_node) = node.as_any().downcast_ref::<DeletePlanNode>() {
                assert_eq!(logical_inputs.len(), 1);
                assert_eq!(physical_inputs.len(), 1);
                let output_schema = Arc::new(Schema::from(&(*delete_node.schema)));
                let input_schema = Arc::clone(&physical_inputs[0].schema());
                let _debug = &input_schema.fields;
                Some(Arc::new(DeleteExec::new(
                    self.table_id,
                    &self.db,
                    &input_schema,
                    &output_schema,
                    &physical_inputs[0],
                )))
            } else {
                None
            };
        Ok(node.map(|exec| exec as Arc<dyn ExecutionPlan>))
    }
}

#[allow(dead_code)]
struct DeleteExec {
    table_id: u64,
    db: Arc<DB>,
    out_schema: SchemaRef, // output schema?
    in_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl DeleteExec {
    fn new(
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
        // let _columns = batch.columns();
        // Currently each KVTable must have a `row_id`, should be done in `CREATE` automatically
        // let (target_index, field) = batch.schema().column_with_name("row_id").unwrap();
        // let target = &columns[target_index];
        let target = batch.columns().last().unwrap(); // by design, the `row_id` is the last column
        self.delete_row(target)
    }

    /// This function iter over the `target` array to get each `row_id` to delete
    fn delete_row(&self, target: &ArrayRef) -> Result<u64> {
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
mod tests {
    use super::*;
    use crate::storage::kv::{KVTable, KVTableMeta};
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
            .with_query_planner(Arc::new(DeleteQueryPlanner {
                table_id: 1003,
                db: Arc::clone(&db),
            }))
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
