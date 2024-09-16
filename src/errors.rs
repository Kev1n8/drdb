use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("internal error of kv storage")]
    KvStorageInternalError(String),

    #[error("key not found in storage")]
    KeyNotFoundInStorage,
}

#[macro_export]
macro_rules! db_datafusion_error {
    ($ERR:expr) => {
        DataFusionError::External(Box::new($ERR))
    };
}
