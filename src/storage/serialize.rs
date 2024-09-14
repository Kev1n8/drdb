/// TODO: This file should contain the rules of making keys( of meta, row) value(of meta)

use crate::storage::kv::KVTableMeta;

pub fn make_meta_key(table_id: u64) -> Vec<u8> {
    format!("mt{}", table_id).into_bytes()
}

pub fn make_meta_value(meta: &KVTableMeta) -> Vec<u8> {
    meta.to_string().into_bytes()
}

pub fn make_row_key(table_id: u64, name: &str, row: u64) -> Vec<u8> {
    let s = format!("t{table_id}_c{name}_r{row:0>6}");
    s.into_bytes()
}
