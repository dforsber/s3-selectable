import { mockClient } from "aws-sdk-client-mock";
import { GetPartitionsCommand, GetTableCommand, GlueClient } from "@aws-sdk/client-glue";
import { S3Client } from "@aws-sdk/client-s3";
import {
  testTableCsv,
  testTableJSON,
  testTableKeys,
  testTableKeysNoPartitions,
  testTableParquet,
  testTablePartitions,
  testTableUnsupportedJSON,
  testTableWithoutPartitionKeys,
  testTableWithoutStorage,
  testTableWithoutStorageLocation,
  testTableWithoutStorageSerde,
} from "../common/fixtures/glue-table";

import { GlueTableToS3Key } from "./glueTableToS3Keys.mapper";

let glueGetTableCalled = 0;
let glueGetPartitionsCalled = 0;
let s3ListObjectsV2Called = 0;

const glueMock = mockClient(GlueClient);
glueMock
  .on(GetTableCommand)
  .callsFake(params => {
    glueGetTableCalled++;
    if (params.Name === "noStorage") return Promise.resolve({ Table: testTableWithoutStorage });
    if (params.Name === "noStorageSerde") return Promise.resolve({ Table: testTableWithoutStorageSerde });
    if (params.Name === "noStorageLocation") return Promise.resolve({ Table: testTableWithoutStorageLocation });
    if (params.Name === "noPartitionKeys") return Promise.resolve({ Table: testTableWithoutPartitionKeys });
    if (params.Name === "partitioned_and_bucketed_elb_logs_parquet")
      return Promise.resolve({ Table: testTableParquet });
    if (params.Name === "bucketed_elb_logs") return Promise.resolve({ Table: testTableCsv });
    if (params.Name === "bucketed_elb_logs_from_partitioned_2_json") return Promise.resolve({ Table: testTableJSON });
    if (params.Name === "bucketed_elb_logs_unsupported_serde")
      return Promise.resolve({ Table: testTableUnsupportedJSON });
    return Promise.resolve({});
  })
  .on(GetPartitionsCommand)
  .callsFake(params => {
    glueGetPartitionsCalled++;
    if (params.TableName === "noPartitionKeys") return Promise.resolve({});
    return Promise.resolve({ Partitions: testTablePartitions });
  });

const s3Mock = mockClient(S3Client);
s3Mock.callsFake(params => {
  s3ListObjectsV2Called++;
  const pref = params.Prefix ?? "";
  if (params.Bucket === "dummy-test-bucket2") {
    return Promise.resolve({
      NextContinuationToken: undefined,
      Contents: testTableKeysNoPartitions.map(k => ({ Key: k })),
      $metadata: null,
    });
  }
  return Promise.resolve({
    NextContinuationToken: undefined,
    Contents: testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k })),
    $metadata: null,
  });
});

const s3 = new S3Client({ region: "eu-west-1" });
const glue = new GlueClient({ region: "eu-west-1" });

let mapper: GlueTableToS3Key;
const databaseName = "default";
const tableName = "partitioned_and_bucketed_elb_logs_parquet";
beforeEach(() => {
  glueGetTableCalled = 0;
  glueGetPartitionsCalled = 0;
  s3ListObjectsV2Called = 0;
  mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName });
});

describe("Parameter and return value checks", () => {
  it("throws when Glue is not provided", () => {
    mapper = new GlueTableToS3Key({ glue: undefined, s3, databaseName: "db", tableName: "t" });
    expect(() => mapper.getTable()).rejects.toThrowError();
    expect(() => mapper.getPartitions()).rejects.toThrowError();
  });
  it("error handling when Glue Table is not found", () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "nonExisting" });
    expect(() => mapper.getTable()).rejects.toThrowError("Table not found: nonExisting");
  });
  it("no storage descriptor", () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noStorage" });
    expect(() => mapper.getTable()).rejects.toThrowError();
  });
  it("no storage location", () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noStorageLocation" });
    expect(() => mapper.getTable()).rejects.toThrowError();
  });
  it("no storage serde (inpuut serialisation)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noStorageSerde" });
    const inpSer = await mapper.getInputSerialisation();
    expect(inpSer).toBe(undefined);
  });
  it("no storage (inpuut serialisation)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noStorage" });
    const inpSer = await mapper.getInputSerialisation();
    expect(inpSer).toBe(undefined);
  });
  it("no partition keys", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noPartitionKeys" });
    const t = await mapper.getTable();
    expect(t.PartitionKeys).toEqual(undefined);
    const info = await mapper.getTableInfo();
    expect(info.PartitionColumns).toEqual([]);
  });
});

describe("When fetching partitioning information", () => {
  it("correctly lists all S3 Keys", async () => {
    let keys = await mapper.getKeysByFilteredPartitions([]);
    expect(keys.length).toEqual(testTableKeys.length);
    expect(keys.sort()).toEqual(testTableKeys.sort());
    keys = await mapper.getKeysByFilteredPartitions([]);
    expect(keys.length).toEqual(testTableKeys.length);
    expect(keys.sort()).toEqual(testTableKeys.sort());
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1);
    expect(s3ListObjectsV2Called).toEqual(10); // 10 partitions, 1 call
  });

  it("table location keys when no partitions", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noPartitionKeys" });
    const t = await mapper.getTable();
    expect(t.PartitionKeys).toEqual(undefined);
    const keys = await mapper.getKeysByFilteredPartitions([]);
    expect(keys.length).toEqual(0);
  });

  it("correctly identifies table information (Parquet)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "partitioned_and_bucketed_elb_logs_parquet" });
    let info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
    info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(0);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("correctly identifies table information (CSV)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "bucketed_elb_logs" });
    let info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
    info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(0);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("correctly identifies table information (unknown)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "bucketed_elb_logs_unsupported_serde" });
    const info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
  });

  it("correctly identifies table information (JSON)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "bucketed_elb_logs_from_partitioned_2_json" });
    let info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
    info = await mapper.getTableInfo();
    expect(info).toMatchSnapshot();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(0);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("getPartitions to return correct partitions", async () => {
    expect((await mapper.getPartitions()).length).toEqual(10);
    expect(glueGetTableCalled).toEqual(0);
    expect(glueGetPartitionsCalled).toEqual(1);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("getPartitionValues to return all partition values correctly", async () => {
    expect((await mapper.getPartitionsAsPaths()).sort()).toEqual(
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
    const filteredKeys = (await mapper.getKeysByFilteredPartitions(["/ssl_protocol=-/elb_response_code=302"])).sort();
    const expectedKeys = testTableKeys
      .filter(k => k.includes("ssl_protocol=-"))
      .filter(k => k.includes("elb_response_code=302"))
      .sort();
    expect(filteredKeys).toEqual(expectedKeys);
  });
});
