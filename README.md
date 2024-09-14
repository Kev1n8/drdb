# DRDB

This project is built for learning purposes and is not intended for production use.

## Introduction 📋

**DRDB** ([Datafusion](https://github.com/apache/datafusion) + [RocksDB](https://github.com/rust-rocksdb/rust-rocksdb))  leverages DataFusion as its SQL engine and RocksDB as the storage backend.

The main goal of this project is to deepen my understanding of DataFusion and database internals. The ultimate objective is to develop a fully functional columnar database.

Please note that this is a personal learning project, and as I'm still exploring, there may be mistakes or inefficiencies in the implementation. Any feedback or suggestions are welcome!

## Recent Updates 📅

- 2024/09/15: Introduced an abstraction layer between RocksDB and DataFusion's `TableProvider`. Implemented basic `scan` and `insert_into` operations to enable reading and writing to RocksDB, currently supporting only `DataType::Utf8`. Currently ugly code.

## Roadmap 👨‍💻

This project is still in its early stages. Here’s what I plan to work on:

- [ ] Implement a `serialization` layer for converting data between `RecordBatch` and `rocksdb::DB`.
- [ ] Extend DataFusion’s DML capabilities to support SQL commands like `INSERT INTO` and `DELETE FROM`.
- [ ] Extend DataFusion’s DDL to handle SQL commands like `CREATE TABLE` and `DROP TABLE`.
- [ ] More...
