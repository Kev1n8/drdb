use crate::df_extension::planner::delete_planner::DeletePlanner;
use async_trait::async_trait;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::ExecutionPlan;
use rocksdb::DB;
use std::sync::Arc;

#[allow(dead_code)]
pub struct DeleteQueryPlanner {
    table_id: u64,
    db: Arc<DB>,
}

impl DeleteQueryPlanner {
    pub fn new(table_id: u64, db: Arc<DB>) -> Self {
        Self { table_id, db }
    }
}

#[async_trait]
impl QueryPlanner for DeleteQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            DeletePlanner::new(self.table_id, Arc::clone(&self.db)),
        )]);

        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

// TODO: add unit tests for query_planner
