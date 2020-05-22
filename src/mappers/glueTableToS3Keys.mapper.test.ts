import * as AWSMock from "aws-sdk-mock";
import { GetPartitionsRequest, GetTableRequest } from "aws-sdk/clients/glue";
import { GlueTableToS3Key } from "./glueTableToS3Keys.mapper";
import { ListObjectsV2Request } from "aws-sdk/clients/s3";
import { testTable, testTableKeys, testTablePartitions } from "../common/fixtures/glue-table";
/* eslint-disable max-len */
/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/no-require-imports */
const AWS = require("aws-sdk");

let glueGetTableCalled = 0;
let glueGetPartitionsCalled = 0;
let s3ListObjectsV2Called = 0;
AWSMock.setSDKInstance(AWS);
AWSMock.mock("Glue", "getTable", (params: GetTableRequest, cb: Function) => {
  if (params.Name !== "partitioned_and_bucketed_elb_logs_parquet") cb(new Error("Table not found"), null);
  glueGetTableCalled++;
  cb(null, { Table: testTable });
});
AWSMock.mock("Glue", "getPartitions", (_params: GetPartitionsRequest, cb: Function) => {
  glueGetPartitionsCalled++;
  cb(null, { Partitions: testTablePartitions });
});
AWSMock.mock("S3", "listObjectsV2", (params: ListObjectsV2Request, cb: Function) => {
  const pref = params.Prefix ?? "";
  s3ListObjectsV2Called++;
  cb(null, {
    NextContinuationToken: undefined,
    Contents: testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k })),
  });
});

const s3 = new AWS.S3({ region: "eu-west-1" });
const glue = new AWS.Glue({ region: "eu-west-1" });

let mapper: GlueTableToS3Key;
beforeEach(() => {
  const databaseName = "default";
  const tableName = "partitioned_and_bucketed_elb_logs_parquet";
  glueGetTableCalled = 0;
  glueGetPartitionsCalled = 0;
  s3ListObjectsV2Called = 0;
  mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName });
});

describe("When fetching partitioning information", () => {
  it("correctly lists all S3 Keys", async () => {
    let keys = await mapper.getAllKeys();
    expect(keys.length).toEqual(testTableKeys.length);
    expect(keys.sort()).toEqual(testTableKeys.sort());
    keys = await mapper.getAllKeys();
    expect(keys.length).toEqual(testTableKeys.length);
    expect(keys.sort()).toEqual(testTableKeys.sort());
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1);
    expect(s3ListObjectsV2Called).toEqual(10); // 10 partitions
  });

  it("correctly identifies table information", async () => {
    let info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "PartitionColumns": Array [
          "ssl_protocol",
          "elb_response_code",
        ],
      }
    `);
    info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "PartitionColumns": Array [
          "ssl_protocol",
          "elb_response_code",
        ],
      }
    `);
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(0);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("throws with non-existing table", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName: "nonexisting", tableName: "dummy" });
    await expect(async () => mapper.getTableInfo()).rejects.toThrowError();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(0);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("partition locations", async () => {
    expect(await mapper.getPartitionLocations()).toEqual([
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=500",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=301",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=500",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=302",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=404",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=404",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=200",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=301",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=200",
    ]);
    expect(await mapper.getPartitionLocations()).toEqual([
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=500",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=301",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=500",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=302",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=404",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=404",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=200",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=TLSv1.2/elb_response_code=301",
      "s3://athena-results-dforsber/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=200",
    ]);
    expect(glueGetTableCalled).toEqual(0);
    expect(glueGetPartitionsCalled).toEqual(1);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("getPartitions to return correct partitions", async () => {
    expect((await mapper.getPartitions()).length).toEqual(10);
    expect(glueGetTableCalled).toEqual(0);
    expect(glueGetPartitionsCalled).toEqual(1);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("getPartitionValues to return all partition values correctly", async () => {
    expect((await mapper.getPartitionValues()).sort()).toEqual(
      [
        "/ssl_protocol=-/elb_response_code=302",
        "/ssl_protocol=TLSv1.2/elb_response_code=500",
        "/ssl_protocol=-/elb_response_code=301",
        "/ssl_protocol=-/elb_response_code=500",
        "/ssl_protocol=TLSv1.2/elb_response_code=302",
        "/ssl_protocol=TLSv1.2/elb_response_code=404",
        "/ssl_protocol=-/elb_response_code=404",
        "/ssl_protocol=TLSv1.2/elb_response_code=200",
        "/ssl_protocol=TLSv1.2/elb_response_code=301",
        "/ssl_protocol=-/elb_response_code=200",
      ].sort(),
    );
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("getKeysByPartitions to return only matching keys", async () => {
    const filteredKeys = (await mapper.getKeysByPartitions(["/ssl_protocol=-/elb_response_code=302"])).sort();
    const expectedKeys = testTableKeys
      .filter(k => k.includes("ssl_protocol=-"))
      .filter(k => k.includes("elb_response_code=302"))
      .sort();
    expect(filteredKeys).toEqual(expectedKeys);
  });
});
