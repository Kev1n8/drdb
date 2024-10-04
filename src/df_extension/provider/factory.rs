use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider, TableProviderFactory};
use datafusion_common::Result;
use datafusion_expr::CreateExternalTable;
use rocksdb::DB;
use std::sync::Arc;

/// This factory makes SessionState able to deal with create KVTable.
/// Specifically, it generates new a `KVTable` for state.
pub struct KVTableProviderFactory {
    db: Arc<DB>,
}

#[async_trait]
impl TableProviderFactory for KVTableProviderFactory {
    /// Create a KVTableProvider based on given cmd.
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(ignore)]
    #[test]
    fn basic_create() {
        todo!()
    }
}
