# Selectable - S3 Select over a Glue Table

## TL;DR

This module runs parallel S3 Select over all the S3 Keys of a Glue Table and returns a single merged event stream. The API is the same as for S3 SELECT NodeJS SDK, i.e. params are passed thorugh, but Bucket and Key are repalced from values for the Glue Table S3 Data.

## Single S3 Select stream over multiple files

AWS S3 SELECT is a filtering stream over S3 Objects. Glue Tables are metadata about structured data on S3 that can point to hundreds of different S3 Objects in separate Hive Partitions and Hive Buckets.

S3 SELECT doesn't understand anything about Glue Tables, but it supports high parallelism (up to 5k concurrent requests). This module provides the same `s3.selectObjectContent` method in the `s3SelectOnTable` class, but makes `Bucket` and `Key` optional as those are read from the Glue Table itself. For each S3 Object in the Glue Table data location and partitions, it launches S3 SELECT and returns a single stream as merged stream of all the concurrent S3 SELECT calls.

When the class `s3SelectOnTable` is instantiated it triggers AWS API calls for fetching table metadata and getting all S3 Keys for the table data. You can then issue multiple S3 SELECT calls over the same table, while the metadata is in-memory.

## Example

```javascript
const region = { region: process.env.AWS_REGION || "eu-west-1" };
const glueTable = new s3SelectOnTable({
  s3: new AWS.S3(region),
  glue: new AWS.Glue(region),
  tableName: process.env.TABLE_NAME || "elb_logs",
  databaseName: process.env.DATABASE_NAME || "sampledb",
});
const selectStream = await glueTable.selectObjectContent({
  // Bucket: "BucketNotNeeded",
  // Key: "KeyNotNeeded",
  ExpressionType: "SQL",
  InputSerialization: { CSV: {} },
  // InputSerialization: { Parquet: {} },
  OutputSerialization: { JSON: {} },
  Expression: "SELECT * FROM S3Object LIMIT 2",
});
selectStream.on("data", chunk => {
  if (chunk.Records?.Payload) console.log(Buffer.from(chunk.Records?.Payload).toString());
});
```

## Use Case: Interactive speed Big Data paging

AWS S3 SELECT supports Parquet, which is a popular columnar binary data format for storing data efficiently (compression). Parquet is commonly used as the data storage format for e.g. Glue Tables where the data resides on the S3 (Data Lake). If the S3 SELECT filtering condition matches e.g. ordered row numbers in Parquet files with min/max values for each row group (Hive Bucket), searches over the data becomes much faster.

If you spread your rows (row number column) evenly over all the partitions and buckets, getting a "page" from the data in effect fetches rows from all S3 Objects simultanously and thus becomes really fast in providing results for you.

**NOTE**: Please note that there is a maximum S3 Object size limit for Parquet files with S3 SELECT, so you have to partition and bucket your big data to roughly 256MB files, which is a good ballpark anyway for object size with S3.
