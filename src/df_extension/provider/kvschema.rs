use async_trait::async_trait;
use datafusion_catalog::{SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use rocksdb::DB;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// This SchemaProvider is used when `create` and `drop` tables(external).
/// Specifically, it puts and erases `KVTableMeta`s from db.
pub struct KVSchemaProvider {
    db: Arc<DB>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

#[async_trait]
impl SchemaProvider for KVSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        todo!()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        todo!()
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        todo!()
    }

    fn table_exist(&self, name: &str) -> bool {
        todo!()
    }
}
