use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("internal error of kv storage")]
    KvStorageInternalError(String),

    #[error("key not found in storage")]
    KeyNotFoundInStorage,

    #[error("datafusion error")]
    DatafusionError,
}

pub type DBResult<T> = Result<T, DBError>;


pub fn db_error_to_datafusion_error(err: DBError) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

pub fn arrow_error_to_db_error(err: ArrowError) -> DBError {
    DBError::KvStorageInternalError(err.to_string())
}

pub fn arrow_error_to_datafusion_error(err: ArrowError) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

