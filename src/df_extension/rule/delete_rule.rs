use crate::df_extension::node::delete_node::DeletePlanNode;
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::DataFusionError;
use datafusion_expr::{DmlStatement, Extension, LogicalPlan, WriteOp};
use std::sync::Arc;

#[allow(dead_code)]
pub struct DeleteReplaceRule {}

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
                node: Arc::new(DeletePlanNode::new(
                    input.as_ref().clone(),
                    output_schema,
                    input.expressions(),
                )),
            })))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

// TODO: add unit tests for delete_rule
