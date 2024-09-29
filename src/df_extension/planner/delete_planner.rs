use crate::df_extension::exec::delete_exec::DeleteExec;
use crate::df_extension::node::delete_node::DeletePlanNode;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use datafusion_physical_plan::ExecutionPlan;
use rocksdb::DB;
use std::sync::Arc;

#[allow(dead_code)]
pub struct DeletePlanner {
    table_id: u64,
    db: Arc<DB>,
}

impl DeletePlanner {
    pub fn new(table_id: u64, db: Arc<DB>) -> Self {
        Self { table_id, db }
    }
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
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let node =
            if let Some(delete_node) = node.as_any().downcast_ref::<DeletePlanNode>() {
                assert_eq!(logical_inputs.len(), 1);
                assert_eq!(physical_inputs.len(), 1);
                let t = UserDefinedLogicalNodeCore::schema(delete_node)
                    .as_arrow()
                    .clone();
                let output_schema = Arc::new(t);
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

// TODO: add unit tests for delete_planner
