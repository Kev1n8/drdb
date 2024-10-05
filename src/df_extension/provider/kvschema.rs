use crate::db_datafusion_error;
use crate::df_extension::provider::kvtable::KVTable;
use crate::df_extension::serialize::make_meta_key;
use async_trait::async_trait;
use datafusion::parquet::data_type::AsBytes;
use datafusion_catalog::{SchemaProvider, TableProvider};
use datafusion_common::{exec_err, DataFusionError, Result};
use rocksdb::DB;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Currently, the implementation is using `unwrap` everywhere for convenience.

/// This SchemaProvider is used when `create` and `drop` tables(external).
/// Specifically, it puts and erases `KVTableMeta`s from db.
pub struct KVSchemaProvider {
    db: Arc<DB>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl KVSchemaProvider {
    pub fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Delete `KVTableMeta` and all rows in db.
    fn delete_table(&self, name: &str, table: &KVTable) -> Result<()> {
        let target_meta_key = make_meta_key(table.table_id);
        // Delete meta value.
        self.db
            .delete(target_meta_key)
            .map_err(|e| db_datafusion_error!(e))?;

        let prefix = format!("t{}_c", table.table_id);
        let mut iter = self.db.prefix_iterator(prefix);
        let mut to_del = Vec::new();
        // Collect all keys to delete.
        while let Some(Ok((k, _))) = iter.next() {
            to_del.push(k);
        }
        while let Some(tmp) = to_del.pop() {
            self.db.delete(tmp.as_bytes()).unwrap()
        }
        Ok(())
    }
}

#[async_trait]
impl SchemaProvider for KVSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<String>>()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match self.tables.read().unwrap().get(name) {
            Some(p) => Ok(Some(Arc::clone(p))),
            None => Ok(None),
        }
    }

    /// Simply put the table into HashMap.
    /// The `KVTableMeta` should be set up when factory generating provider
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut guard = self.tables.write().unwrap();
        match guard.get(&name) {
            // Should never happen.
            // Dup-check happen once when creating the `TableProvider`
            None => {
                guard.insert(name, Arc::clone(&table));
                Ok(Some(table))
            }
            Some(_t) => exec_err!("Table already exists"),
        }
    }

    /// Erase `KVTableMeta` from DB here.
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // TODO: Should be atomic operation.
        let mut binding = self.tables.write().unwrap();
        let target = binding.get(name).unwrap().clone();
        binding.remove(name);
        if let Some(kv_table) = target.as_any().downcast_ref::<KVTable>() {
            self.delete_table(name, kv_table)?;
            Ok(Some(target))
        } else {
            // Not suppose to happen
            exec_err!("Expected KVTable")
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().unwrap().get(name).is_some()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::df_extension::provider::kvtable::KVTableMeta;
    use crate::df_extension::serialize::make_meta_value;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::cast::as_uint64_array;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnv;

    // This only tests if `KVSchemaProvider` behaves properly,
    // deleting all relevant key-values in the db.
    #[tokio::test]
    async fn basic_drop() -> Result<()> {
        let db = Arc::new(DB::open_default("./tmp").unwrap());

        let schema = Arc::new(Schema::new(Fields::from(vec![
            Arc::new(Field::new("c1", DataType::Utf8, false)),
            Arc::new(Field::new("row_id", DataType::Utf8, false)),
        ])));
        let meta = KVTableMeta {
            id: 1101,
            name: "BasicDrop".to_string(),
            schema: Arc::clone(&schema),
            highest: 0,
        };
        db.put(make_meta_key(1101), make_meta_value(&meta)).unwrap();

        let kv_table = Arc::new(KVTable::new(&Arc::new(meta), Arc::clone(&db)))
            as Arc<dyn TableProvider>;

        let provider = KVSchemaProvider::new(Arc::clone(&db));
        let res =
            provider.register_table("BasicDrop".to_string(), Arc::clone(&kv_table))?;
        assert!(res.is_some());

        // Take advantage of INSERT to put the data.
        // Registering the custom planners and rule
        let config = SessionConfig::new().with_target_partitions(1);
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("BasicDrop", Arc::clone(&kv_table))?;
        let inserted_num = ctx
            .sql("INSERT INTO BasicDrop VALUES('hello', '001'), ('world', '002'), ('!', '003')")
            .await
            .unwrap();
        let bind = inserted_num.collect().await.unwrap();
        let batch = bind.first().unwrap();
        let t = as_uint64_array(&batch.columns()[0]).unwrap();
        assert_eq!(t.value(0), 3);

        // Now try to drop the table
        let res = provider.deregister_table("BasicDrop")?;
        assert!(res.is_some());
        assert!(!provider.table_exist("BasicDrop"));

        // Since the `SchemaProvider` of `ctx` does not know about the table has been dropped,
        // we have to check it manually by visiting db.
        let meta_exists = db.get(make_meta_key(1101)).unwrap();
        assert!(meta_exists.is_none());
        // Check if any rows left not deleted
        let mut it = db.prefix_iterator("t1101_c");
        assert!(it.next().is_none());

        Ok(())
    }
}
