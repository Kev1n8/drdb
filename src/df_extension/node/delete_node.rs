use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub struct DeletePlanNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    expr: Vec<Expr>,
}

impl DeletePlanNode {
    pub fn new(input: LogicalPlan, schema: DFSchemaRef, expr: Vec<Expr>) -> Self {
        Self {
            input,
            schema,
            expr,
        }
    }
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

#[cfg(test)]
mod test {
    // TODO: Add unit tests for `delete_plan_node`
}
