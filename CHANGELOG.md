# CHANGELOG

## 2020-xx-xx v0.1.0

- Added support for partition column based S3 Key pre-filtering based on the SQL WHERE clause using `node-sql-parser` and `sqlite3`. This allows running S3 Select efficiently over partitioned tables (e.g. timeseries) as only the relevant partitions would be used for querying
- Added non-class specific interface option
- More unit tests and improved `src` directory structure

## 2020-05-11 v0.0.12

- Initial public version
