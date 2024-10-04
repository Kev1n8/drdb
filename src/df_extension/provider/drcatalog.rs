use datafusion_catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use rocksdb::DB;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct DRCatalogList {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}

impl CatalogProviderList for DRCatalogList {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        todo!()
    }

    fn catalog_names(&self) -> Vec<String> {
        todo!()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        todo!()
    }
}

/// This is a custom catalog (database).
pub struct DRCatalog {
    db: Arc<DB>,
    // Actually they are `KVSchemaProvider`s
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl DRCatalog {
    pub fn new() -> Self {
        todo!()
    }
}

impl CatalogProvider for DRCatalog {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema_names(&self) -> Vec<String> {
        todo!()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        todo!()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        todo!()
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        todo!()
    }
}
