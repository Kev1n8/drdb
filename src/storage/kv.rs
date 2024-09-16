use crate::data::db_table_scan::DBTableScanExec;
use crate::errors::{db_error_to_datafusion_error, DBError, DBResult};
use crate::storage::serialize::{make_meta_key, make_meta_value, make_row_key};
use arrow::array::{as_string_array, ArrayRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::insert::{DataSink, DataSinkExec};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use rocksdb::DB;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type KVTableRef = Arc<KVTable>;
pub type KVTableMetaRef = Arc<KVTableMeta>;

#[derive(Debug, Clone)]
pub struct KVTableMeta {
    pub(crate) id: u64,
    pub(crate) name: String,
    pub(crate) schema: SchemaRef,
    pub(crate) highest: u64,
}

impl KVTableMeta {
    pub fn make_key(&self) -> Vec<u8> {
        make_meta_key(self.id)
    }

    pub fn make_value(&self) -> Vec<u8> {
        make_meta_value(self)
    }
}

impl Display for KVTableMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let schema_str = self
            .schema
            .fields
            .iter()
            .map(|c| c.name().clone())
            .collect::<Vec<_>>()
            .join("_");

        let str = format!(
            "t{}_{}_{}_c{}_{}",
            self.id,
            self.name.as_str(),
            self.highest,
            self.schema.fields.len(),
            schema_str,
        );
        write!(f, "{}", str)
    }
}

impl From<&str> for KVTableMeta {
    fn from(value: &str) -> Self {
        // Split the encoded string by the delimiters `_` and `#c`
        let parts: Vec<&str> = value.split(&['_']).collect();

        // Extract and parse each part
        let id = parts[0][1..].parse::<u64>().expect("Failed to parse id"); // Skipping 't' prefix
        let name = parts[1].to_string();
        let highest = parts[2].parse::<u64>().expect("Failed to parse highest");

        // Extract and parse the number of fields in the schema
        let num_fields_str = parts[3].trim_start_matches('c'); // Remove 'c' prefix
        let _num_fields = num_fields_str
            .parse::<usize>()
            .expect("Failed to parse number of fields");

        // Extract schema field strings and reconstruct the schema
        let schema_str = parts[4..].join(""); // Join the rest of the parts to get the schema string
        let fields: Vec<Field> = schema_str
            .split('_')
            .map(|f| // tmp implementation
                Field::new(f, DataType::Utf8, false))
            .collect();

        let schema = Schema::new(fields);

        // Reconstruct and return the DBTableMeta object
        KVTableMeta {
            id,
            name,
            highest,
            schema: Arc::new(schema),
        }
    }
}

impl From<String> for KVTableMeta {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<Vec<u8>> for KVTableMeta {
    fn from(value: Vec<u8>) -> Self {
        // Safety: value should be guaranteed in utf8
        Self::from(unsafe { String::from_utf8_unchecked(value) })
    }
}

#[derive(Debug, Clone)]
pub struct KVTable {
    pub db: Arc<DB>,
    pub table_id: u64,
    pub meta: KVTableMetaRef,
}

impl KVTable {
    pub fn new(meta: &KVTableMetaRef, db: Arc<DB>) -> Self {
        Self {
            db,
            table_id: meta.id,
            meta: Arc::clone(meta),
        }
    }

    /// This method should be used when a `KVTable` is created for the first time
    /// and with a batch of data given.
    pub async fn try_new(
        meta: &KVTableMetaRef,
        db: Arc<DB>,
        data: Vec<Vec<RecordBatch>>,
    ) -> Result<Self> {
        // Currently only support a single batch, no partition
        let batch = data.first().unwrap().first().unwrap();
        let sink = KVTableSink::new(meta.id, &db);
        // Put the meta & rows into db first
        sink.put_meta(meta).map_err(db_error_to_datafusion_error)?;
        sink.put_batch_into_db(batch).await?;

        Ok(Self::new(meta, db))
    }

    #[allow(dead_code)]
    fn fetch_meta(&self) -> KVTableMeta {
        let key = format!("mt{}", self.table_id).into_bytes();
        if let Some(val) = self.db.get(key).unwrap() {
            KVTableMeta::from(val)
        } else {
            panic!("table should exists")
        }
    }

    pub(crate) async fn create_scan_physical_plan(
        &self,
        target_table: u64,
        _projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DBTableScanExec::new(target_table, &schema, self)))
    }
}

#[async_trait]
impl TableProvider for KVTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.meta.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.create_scan_physical_plan(self.table_id, projection, self.schema())
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(KVTableSink::new(self.table_id, &self.db));
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            self.meta.schema.clone(),
            None,
        )))
    }
}

#[derive(Debug)]
pub struct KVTableSink {
    id: u64,
    db: Arc<DB>,
}

impl KVTableSink {
    pub fn new(id: u64, src: &Arc<DB>) -> Self {
        Self {
            id,
            db: Arc::clone(src),
        }
    }

    pub fn put_meta(&self, meta: &KVTableMetaRef) -> DBResult<()> {
        let key = meta.make_key();
        let val = meta.make_value();
        match self.db.put(key, val) {
            Ok(()) => Ok(()),
            Err(e) => Err(DBError::KvStorageInternalError(e.to_string())),
        }
    }

    fn update_highest(
        &self,
        old_meta: &KVTableMetaRef,
        new_highest: u64,
    ) -> DBResult<()> {
        let new_meta = KVTableMeta {
            id: self.id,
            name: old_meta.name.clone(),
            schema: old_meta.schema.clone(),
            highest: new_highest,
        };

        let meta_key = new_meta.make_key();
        let new_val = new_meta.make_value();
        self.db
            .put(meta_key, new_val)
            .map_err(|e| DBError::KvStorageInternalError(e.to_string()))
    }

    fn put_array(&self, name: &str, arr: &ArrayRef, start: u64) -> DBResult<()> {
        let mut counter = start;

        let arr = as_string_array(arr);
        for row in arr {
            let key = make_row_key(self.id, name, counter + 1);
            match row {
                Some(str) => match self.db.put(key, str.as_bytes()) {
                    Ok(_) => counter += 1,
                    Err(e) => return Err(DBError::KvStorageInternalError(e.to_string())),
                },
                None => todo!(),
            }
        }
        Ok(())
    }

    async fn put_batch_into_db(&self, batch: &RecordBatch) -> Result<u64> {
        let mut row_added = 0u64;
        let meta_key = make_meta_key(self.id);
        let meta_val = self
            .db
            .get(meta_key)
            .map_err(|e| DataFusionError::External(e.into()))?
            .unwrap();
        let meta = KVTableMeta::from(meta_val);
        let start = meta.highest;
        for (index, arr) in batch.columns().iter().enumerate() {
            let schema = batch.schema();
            let name = schema.fields[index].name();
            self.put_array(name.as_str(), arr, start)
                .map_err(db_error_to_datafusion_error)?;
        }
        // Update the highest index
        row_added += batch.num_rows() as u64;
        self.update_highest(&Arc::new(meta), row_added + start)
            .map_err(db_error_to_datafusion_error)?;
        Ok(row_added)
    }
}

impl DisplayAs for KVTableSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DBTableWrite(table_id={})", self.id)
            }
        }
    }
}

#[async_trait]
impl DataSink for KVTableSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut data = data;
        let mut cnt = 0;
        if let Some(batch) = data.next().await.transpose()? {
            cnt = self.put_batch_into_db(&batch).await?;
        }
        Ok(cnt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, StringArray};
    use arrow::datatypes::UInt64Type;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Fields;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{exec_datafusion_err, ScalarValue};
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_physical_plan::collect;

    #[tokio::test]
    async fn test_meta_encode_decode() {
        let meta = KVTableMeta {
            id: 1002,
            name: "TableTest".to_string(),
            schema: Arc::new(Schema::new(Fields::from(vec![
                Field::new("column1", DataType::Utf8, false),
                Field::new("column2", DataType::Utf8, false),
            ]))),
            highest: 0,
        };
        let key = meta.make_key();
        let val = meta.make_value();

        assert_eq!(key, "mt1002".to_string().into_bytes());
        assert_eq!(
            val,
            "t1002_TableTest_0_c2_column1_column2"
                .to_string()
                .into_bytes()
        );

        let decode = KVTableMeta::from(val);
        assert_eq!(meta.id, decode.id);
        assert_eq!(meta.highest, decode.highest);
        assert_eq!(meta.name, decode.name);
    }

    /// Create a `KVTable` with a single column and `insert into` it
    /// by `values`, check if the data is inserted
    #[tokio::test]
    async fn test_db_write() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["hello", "world", "!"]))],
        )?;
        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table =
            experiment(schema, vec![vec![batch.clone()]], vec![vec![batch.clone()]])
                .await?;
        // Ensure that the table now contains two batches of data in the same partition
        for col in resulting_data_in_table.columns() {
            let arr = as_string_array(col);
            assert_eq!(
                arr,
                &StringArray::from(vec!["hello", "world", "!", "hello", "world", "!"]),
            )
        }

        // todo: remove test table after this test
        Ok(())
    }

    /// This function create a table with `initial_data` to insert `inserted_data` into and
    /// return the final batch of the table.
    async fn experiment(
        schema: SchemaRef,
        initial_data: Vec<Vec<RecordBatch>>,
        inserted_data: Vec<Vec<RecordBatch>>,
    ) -> Result<RecordBatch> {
        let expected_count: u64 = inserted_data
            .iter()
            .flat_map(|batches| batches.iter().map(|batch| batch.num_rows() as u64))
            .sum();

        // Create a new session context
        let session_ctx = SessionContext::new();
        // Create meta of a table
        let dest_meta = Arc::new(KVTableMeta {
            id: 1002,
            name: "Dest".to_string(),
            schema: Arc::new(Schema::new(Fields::from(vec![Field::new(
                "a",
                DataType::Utf8,
                false,
            )]))),
            highest: 0,
        });
        // Create KV store
        let db = DB::open_default("tmp").unwrap();
        let db = Arc::new(db);
        // Create and register the initial table with the provided schema and data
        let initial_table =
            Arc::new(KVTable::try_new(&dest_meta, Arc::clone(&db), initial_data).await?);
        session_ctx.register_table("Dest", initial_table.clone())?;

        let exprs = vec![
            vec![Expr::Literal(ScalarValue::Utf8(Some("hello".to_string())))],
            vec![Expr::Literal(ScalarValue::Utf8(Some("world".to_string())))],
            vec![Expr::Literal(ScalarValue::Utf8(Some("!".to_string())))],
        ];
        let values_plan = LogicalPlanBuilder::values(exprs)?
            .project(vec![Expr::Column("column1".into()).alias("a")])?
            .build()?;

        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(values_plan, "Dest", &schema, false)?
                .build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        assert_eq!(extract_count(res), expected_count);

        let target_schema = Arc::new(Schema::new(Fields::from(vec![Field::new(
            "a",
            DataType::Utf8,
            false,
        )])));
        let exec = DBTableScanExec::new(1002, &target_schema, &initial_table);
        let mut stream = exec.execute(0, session_ctx.task_ctx())?;
        if let Some(batch) = stream.next().await.transpose()? {
            Ok(batch)
        } else {
            Err(exec_datafusion_err!(
                "unknown err when fetching batch from stream"
            ))
        }
    }

    fn extract_count(res: Vec<RecordBatch>) -> u64 {
        assert_eq!(res.len(), 1, "expected one batch, got {}", res.len());
        let batch = &res[0];
        assert_eq!(
            batch.num_columns(),
            1,
            "expected 1 column, got {}",
            batch.num_columns()
        );
        let col = batch.column(0).as_primitive::<UInt64Type>();
        assert_eq!(col.len(), 1, "expected 1 row, got {}", col.len());
        let val = col
            .iter()
            .next()
            .expect("had value")
            .expect("expected non null");
        val
    }
}
