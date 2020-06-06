# s3-selectable - S3 Select over a Glue Table

![CI](https://github.com/dforsber/s3-selectable/workflows/CI/badge.svg?branch=master)
![statements](badges/badge-statements.svg)
![functions](badges/badge-functions.svg)
![lines](badges/badge-lines.svg)
![branches](badges/badge-branches.svg)
![BuiltBy](https://img.shields.io/badge/TypeScript-Lovers-black.svg "img.shields.io")

This module runs parallel [S3 Select](https://aws.amazon.com/blogs/developer/introducing-support-for-amazon-s3-select-in-the-aws-sdk-for-javascript/) over all the S3 Keys of a [Glue Table](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html) and returns a single [merged event stream](https://github.com/grncdr/merge-stream). The API is the same as for [S3 Select NodeJS SDK (`S3.selectObjectContent`)](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#selectObjectContent-property), i.e. params are passed thorugh, but `Bucket` and `Key` are replaced from values for the Glue Table S3 Data.

```shell
yarn add @dforsber/s3-selectable
```

```javascript
const AWS = require("aws-sdk");
const { S3SelectOnTable } = require("@dforsber/s3-selectable");

async function main() {
  // NOTE: Instantiation of the class will start querying AWS Glue and S3 to
  //       fetch all S3 Object Keys that corresponds with the Glue Table data.
  const glueTable = new S3SelectOnTable({
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

  selectStream.on("data", chunk => {
    if (chunk.Records && chunk.Records.Payload) console.log(Buffer.from(chunk.Records.Payload).toString());
  });
}

main().catch(err => console.log(err));
```

## Single S3 Select stream over multiple files

[AWS S3 Select](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html) is a filtering stream over S3 Objects, where filtering is defined with SQL syntax. Glue Tables are metadata about structured data on S3 that can point to hundreds of different S3 Objects in separate Hive Partitions and Hive Buckets.

S3 Select doesn't understand anything about Glue Tables, but it supports high parallelism. This module provides the same `S3.selectObjectContent` method in the `s3SelectOnTable` class, but makes `Bucket` and `Key` optional as those are read from the Glue Table itself. For each S3 Object in the Glue Table data location and partitions, it launches S3 Select and returns a single stream as merged stream of all the concurrent S3 Select calls.

When the class `s3SelectOnTable` is instantiated it triggers AWS API calls for fetching table metadata and getting all S3 Keys for the table data. You can then issue multiple S3 Select calls over the same table, while the metadata is in-memory.

### Usage with Lambda

`s3SelectOnTable` should be instantiated outside the Lambda handler, i.e. during the cold start. This way warm Lambda container has the Glue Table "metadata" already in-memory.

## Narrowed scope with partition filtering

s3-selectable supports pre-filtering S3 Keys based on Glue Table partitions. The WHERE clause is extracted and matched with table partition columns with `node-sql-parser` and `sqlite3`. If WHERE clause contains any filters based partition columns those will be applied to filter parttions. S3 Keys are only listed for filtered partition list. This allows e.g. to stream events from a specific date range from a timeseries "database".

NOTE: _Before filtering, all non-partition based clauses are set to TRUE. The SQLite database is created in-memory and partitions are added into table where the partition values are put into separate columns. This allows filtering partitions based on their values (e.g. `year`, `month`, and `day`)._

```sql
SELECT * FROM logs WHERE year>=2019 AND month>=12
```

## Scalability with Parquet

If the Glue Table is sorted, partitioned and/or bucketed into a proper sized S3 Objects in Parquet, running this module with filters against the sorted column (e.g. row numbers for paging) will give high performance in terms of low latency and high data throughput. S3 Select is a pushdown closer to where the data is stored and supports thousands of concurrent API calls. This allows processing tables that map to huge amounts of data.

### Improvement ideas

- Add support for compression for the merged stream to benefit of better throughput, especially if running this in a Lambda function e.g. with API Gateway

- Working with tables with thousands of files could be improved with node workers in multiple CPU core environments

- Add support for max concurrent S3 Select streams. If a large table has tens of thousands of objects in S3, it is not possible to launch S3 Select over all of them. Also, if the stream consumption is slow, it makes sense not to launch overly large number of concurrent S3 Select streams. Also, the control plane may become too heavy with overly high concurrency. Doing pre-filtering with partitions avoids these shortcomings in most cases though.

- Find out how long S3 Select stream is consumable and how slow it can be consumed to keep it "open".

- For sorted tables with Parquet files, cache also Parquet metadata and filter out S3 files that do not match with filtering criteria. This reduces the number of concurrent API calls, whilst improving scalability futhermore with big data tables

- Use scan range for row based file formats to improve performance

- `sqlite3` is used to pre-filter partitions. SQLite could be used to add support for regular expression based partition filtering, which are not supported by S3 SELECT. In general, SQLite could be used to do stream post-filtering to allow taking benefit of all SQLite features (like regexps).

### Known issues

- The response data is a combination of response data from all the parallal s3 select calls. Thus, e.g. `LIMIT 10`, will apply to all individual calls. Similarly, if you s3 select sorted table the results will not be sorted as the individual streams are combined as they send data. For the same reason, the merged stream may have multiple events of the same type (like "end") as the source consists of multiple independent streams.

- S3 select supports [scan range](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html#AmazonS3-SelectObjectContent-request-ScanRange), so it is possible to parallalize multiple S3 Selects against single S3 Object. Using scan range is good for row based formats like CSV and JSON. This module does not use scan ranges as it is mainly targeted for Parquet file use cases ("indexed big data").

- Please note that the [maximum uncompressed row group size is 256MB for Parquet](https://docs.aws.amazon.com/AmazonS3/latest/dev/selecting-content-from-objects.html) files with S3 Select, so you have to partition and bucket your big data accordingly.

- S3 Select does not support `Map<>` columns with Parquet files. Thus, instead of e.g. doing "SELECT \* FROM", select columns explicitly and do not include columns with `Map<>` types.
