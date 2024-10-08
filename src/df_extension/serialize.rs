use crate::df_extension::provider::kvtable::KVTableMeta;

/// TODO: This file should contain the rules of making keys( of meta, row) value(of meta)

pub fn make_meta_key(table_id: u64) -> Vec<u8> {
    format!("mt{}", table_id).into_bytes()
}

pub fn make_meta_value(meta: &KVTableMeta) -> Vec<u8> {
    let schema_str = meta
        .schema
        .fields
        .iter()
        .map(|c| c.name().clone())
        .collect::<Vec<_>>()
        .join("_");

    let str = format!(
        "t{}_{}_{}_c{}_{}",
        meta.id,
        meta.name.as_str(),
        meta.highest,
        meta.schema.fields.len(),
        schema_str,
    );

    str.into()
}

pub fn make_row_key(table_id: u64, name: &str, row: u64) -> Vec<u8> {
    let s = format!("t{table_id}_c{name}_r{row:0>6}");
    s.into_bytes()
}
