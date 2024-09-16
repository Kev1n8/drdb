pub(crate) mod db_table_scan;

pub trait Serializable {
    fn encode(&self) -> Vec<u8>;
}
