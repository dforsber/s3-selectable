# Selectable - S3 Select over a Glue Table

## TL;DR

This module runs parallel [S3 Select](https://aws.amazon.com/blogs/developer/introducing-support-for-amazon-s3-select-in-the-aws-sdk-for-javascript/) over all the S3 Keys of a [Glue Table](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html) and returns a single [merged event stream](<(https://github.com/grncdr/merge-stream)>). The API is the same as for S3 Select NodeJS SDK, i.e. params are passed thorugh, but `Bucket` and `Key` are replaced from values for the Glue Table S3 Data.

```javascript
import AWS from "aws-sdk";
import s3SelectOnTable from "dforsber/selectable";

// NOTE: Instantiation of the class will start querying AWS Glue and S3 to
//       fetch all S3 Object Keys that corresponds with the Glue Table data.
const glueTable = new s3SelectOnTable({
  s3: new AWS.S3({ region: "eu-west-1" }),
  glue: new AWS.Glue({ region: "eu-west-1" }),
  tableName: "elb_logs",
  databaseName: "sampledb",
});

const selectStream = await glueTable.selectObjectContent({
  // Bucket: "BucketIsOptionalAndNotUsed",
  // Key: "KeyIsOptionalAndNotUsed",
  // ..otherwise the interface is the same.
  ExpressionType: "SQL",
  InputSerialization: { CSV: {} },
  OutputSerialization: { JSON: {} },
  Expression: "SELECT * FROM S3Object LIMIT 2",
});

selectStream.on("data", chunk => console.log(Buffer.from(chunk.Records?.Payload).toString()));
```

## Single S3 Select stream over multiple files

[AWS S3 Select](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html) is a filtering stream over S3 Objects, where filtering is defined with SQL syntax. Glue Tables are metadata about structured data on S3 that can point to hundreds of different S3 Objects in separate Hive Partitions and Hive Buckets.

S3 Select doesn't understand anything about Glue Tables, but it supports high parallelism. This module provides the same `s3.selectObjectContent` method in the `s3SelectOnTable` class, but makes `Bucket` and `Key` optional as those are read from the Glue Table itself. For each S3 Object in the Glue Table data location and partitions, it launches S3 Select and returns a single stream as merged stream of all the concurrent S3 Select calls.

When the class `s3SelectOnTable` is instantiated it triggers AWS API calls for fetching table metadata and getting all S3 Keys for the table data. You can then issue multiple S3 Select calls over the same table, while the metadata is in-memory.

### Usage with Lambda

`s3SelectOnTable` should be instantiated outside the Lambda handler, i.e. during the cold start. This way warm Lambda container has the Glue Table "metadata" already in-memory.

### Known issues

The response data is a combination of response data from all the parallal s3 select calls. Thus, g.e. `LIMIT 10`, will apply to all individual calls. Similarly, if you s3 select sorted table the results will not be sorted as the individual streams are combined as they send data. For the same reason, the merged stream may have multiple events of the same type (like "end") as the source consists of multiple independent streams.

S3 select supports [scan range](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html#AmazonS3-SelectObjectContent-request-ScanRange), so it is possible to parallalize multiple S3 Selects against single S3 Object. Using scan range is good for row based formats like CSV and JSON.

## Use Case: Interactive speed Big Data paging

AWS S3 Select supports Parquet, which is a popular columnar binary data format for storing data efficiently (compression). Parquet is commonly used as the data storage format for e.g. Glue Tables where the data resides on the S3 (Data Lake). If the S3 Select filtering condition matches e.g. ordered row numbers in Parquet files with min/max values for each row group (Hive Bucket), searches over the data becomes much faster.

If you spread your rows (row number column) evenly over all the partitions and buckets, getting a "page" from the data in effect fetches rows from all S3 Objects simultanously and thus becomes much more efficient in delivering the results.

**NOTE**: Please note that the [maximum uncompressed row group size is 256MB for Parquet](https://docs.aws.amazon.com/AmazonS3/latest/dev/selecting-content-from-objects.html) files with S3 Select, so you have to partition and bucket your big data accordingly.
