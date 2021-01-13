import { GetPartitionsRequest, GetTableRequest, Glue } from "@aws-sdk/client-glue";
import { ListObjectsV2CommandInput, S3 } from "@aws-sdk/client-s3";
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
} from "../common/fixtures/glue-table";

import { GlueTableToS3Key } from "./glueTableToS3Keys.mapper";

jest.mock("@aws-sdk/client-s3");
jest.mock("@aws-sdk/client-glue");

let glueGetTableCalled = 0;
let glueGetPartitionsCalled = 0;
let s3ListObjectsV2Called = 0;

jest.mock("@aws-sdk/client-glue", () => ({
  Glue: function Glue() {
    return {
      getTable: jest.fn((params: GetTableRequest) => {
        glueGetTableCalled++;
        if (params.Name === "noStorage") return Promise.resolve({ Table: testTableWithoutStorage });
        if (params.Name === "noStorageLocation") return Promise.resolve({ Table: testTableWithoutStorageLocation });
        if (params.Name === "noPartitionKeys") return Promise.resolve({ Table: testTableWithoutPartitionKeys });
        if (params.Name === "partitioned_and_bucketed_elb_logs_parquet")
          return Promise.resolve({ Table: testTableParquet });
        if (params.Name === "bucketed_elb_logs") return Promise.resolve({ Table: testTableCsv });
        if (params.Name === "bucketed_elb_logs_from_partitioned_2_json")
          return Promise.resolve({ Table: testTableJSON });
        if (params.Name === "bucketed_elb_logs_unsupported_serde")
          return Promise.resolve({ Table: testTableUnsupportedJSON });
        return Promise.resolve({});
      }),
      getPartitions: jest.fn((params: GetPartitionsRequest) => {
        glueGetPartitionsCalled++;
        if (params.TableName === "noPartitionKeys") return Promise.resolve({});
        return Promise.resolve({ Partitions: testTablePartitions });
      }),
    };
  },
}));

jest.mock("@aws-sdk/client-s3", () => ({
  S3: function S3() {
    return {
      listObjectsV2: jest.fn((params: ListObjectsV2CommandInput) => {
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
      }),
    };
  },
}));

const s3 = new S3({ region: "eu-west-1" });
const glue = new Glue({ region: "eu-west-1" });

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
  it("error handling when Glue Table is not found", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "nonExisting" });
    await expect(async () => await mapper.getTable()).rejects.toThrowError("Table not found: nonExisting");
  });
  it("no storage descriptor", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noStorage" });
    await expect(async () => await mapper.getTable()).rejects.toThrowError();
  });
  it("no storage location", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "noStorageLocation" });
    await expect(async () => await mapper.getTable()).rejects.toThrowError();
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
    let keys = await mapper.getKeysByPartitions([]);
    expect(keys.length).toEqual(testTableKeys.length);
    expect(keys.sort()).toEqual(testTableKeys.sort());
    keys = await mapper.getKeysByPartitions([]);
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
    const keys = await mapper.getKeysByPartitions([]);
    expect(keys.length).toEqual(0);
  });

  it("correctly identifies table information (Parquet)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "partitioned_and_bucketed_elb_logs_parquet" });
    let info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "InputSerialization": Object {
          "Parquet": Object {},
        },
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
        "InputSerialization": Object {
          "Parquet": Object {},
        },
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

  it("correctly identifies table information (CSV)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "bucketed_elb_logs" });
    let info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "InputSerialization": Object {
          "CSV": Object {},
          "CompressionType": "GZIP",
        },
        "PartitionColumns": Array [],
      }
    `);
    info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "InputSerialization": Object {
          "CSV": Object {},
          "CompressionType": "GZIP",
        },
        "PartitionColumns": Array [],
      }
    `);
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(0);
    expect(s3ListObjectsV2Called).toEqual(0);
  });

  it("correctly identifies table information (unknown)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "bucketed_elb_logs_unsupported_serde" });
    const info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "InputSerialization": undefined,
        "PartitionColumns": Array [],
      }
    `);
  });

  it("correctly identifies table information (JSON)", async () => {
    mapper = new GlueTableToS3Key({ glue, s3, databaseName, tableName: "bucketed_elb_logs_from_partitioned_2_json" });
    let info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "InputSerialization": Object {
          "JSON": Object {
            "Type": "DOCUMENT",
          },
        },
        "PartitionColumns": Array [],
      }
    `);
    info = await mapper.getTableInfo();
    expect(info).toMatchInlineSnapshot(`
      Object {
        "Bucket": "dummy-test-bucket",
        "InputSerialization": Object {
          "JSON": Object {
            "Type": "DOCUMENT",
          },
        },
        "PartitionColumns": Array [],
      }
    `);
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
