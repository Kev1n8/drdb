use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use crate::errors::DBError;

pub(crate) mod DBTableScan;

pub trait Serializable {
    fn encode(&self) -> Vec<u8>;
}