import * as AWSMock from "aws-sdk-mock";
import { GetPartitionsRequest, GetTableRequest } from "aws-sdk/clients/glue";
import { Readable, ReadableOptions } from "stream";
import { S3Selectable, s3selectable } from "./s3-selectable";
import { testTable, testTableKeys, testTablePartitions } from "../common/fixtures/glue-table";
/* eslint-disable @typescript-eslint/ban-types */
import {
  InputSerialization,
  ListObjectsV2Request,
  OutputSerialization,
  SelectObjectContentRequest,
} from "aws-sdk/clients/s3";

/* eslint-disable max-len */
/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/no-require-imports */
const AWS = require("aws-sdk");

class MockedSelectStream extends Readable {
  public Payload: Readable = this;

  private rows = [
    { id: 1, value: "test1" },
    { id: 2, value: "test2" },
  ];
  private index: number;

  constructor(opt?: ReadableOptions) {
    super(opt);
    this.index = 0;
  }

  public _read(_size: number): void {
    const i = this.index++;
    if (i >= this.rows.length) {
      this.push(null);
      return;
    }
    const buf = Buffer.from(JSON.stringify(this.rows[i]), "ascii");
    this.push(buf);
  }
}

AWSMock.setSDKInstance(AWS);
AWSMock.mock("Glue", "getTable", (params: GetTableRequest, cb: Function) => {
  if (params.Name !== "partitioned_and_bucketed_elb_logs_parquet")
    cb(new Error(`Table not found: ${params.Name}`), null);
  cb(null, { Table: testTable });
});
AWSMock.mock("Glue", "getPartitions", (_params: GetPartitionsRequest, cb: Function) => {
  cb(null, { Partitions: testTablePartitions });
});
AWSMock.mock("S3", "listObjectsV2", (params: ListObjectsV2Request, cb: Function) => {
  const pref = params.Prefix ?? "";
  cb(null, {
    NextContinuationToken: undefined,
    Contents: testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k })),
  });
});
AWSMock.mock("S3", "selectObjectContent", (_params: SelectObjectContentRequest, cb: Function) => {
  cb(null, new MockedSelectStream());
});

const s3 = new AWS.S3({ region: "eu-west-1" });
const glue = new AWS.Glue({ region: "eu-west-1" });
const [databaseName, tableName] = ["default", "partitioned_and_bucketed_elb_logs_parquet"];
const params = { glue, s3, tableName, databaseName };

describe("Test selected partition filtering", () => {
  const partitionValues = [
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
  ].sort();
  it("when there is no WHERE", async () => {
    const sql = "SELECT * FROM s3Object";
    expect((await new S3Selectable(params).filterPartitionsByQuery(sql)).sort()).toEqual(partitionValues);
  });
  it("when there is single partition (1st) filter", async () => {
    const sql = "SELECT * FROM s3Object WHERE ssl_protocol='-'";
    expect((await new S3Selectable(params).filterPartitionsByQuery(sql)).sort()).toEqual(
      partitionValues.filter(v => v.includes("ssl_protocol=-")).sort(),
    );
  });
  it("when there is single partition (2nd) filter", async () => {
    const sql = "SELECT * FROM s3Object WHERE elb_response_code='302'";
    expect((await new S3Selectable(params).filterPartitionsByQuery(sql)).sort()).toEqual(
      partitionValues.filter(v => v.includes("elb_response_code=302")).sort(),
    );
  });
  it("when using two partitions for filtering", async () => {
    const sql = "SELECT * FROM s3Object WHERE elb_response_code='302' AND ssl_protocol='-'";
    expect((await new S3Selectable(params).filterPartitionsByQuery(sql)).sort()).toEqual(
      partitionValues
        .filter(v => v.includes("elb_response_code=302"))
        .filter(v => v.includes("ssl_protocol=-"))
        .sort(),
    );
  });
  it("when using non-matching filter", async () => {
    const sql = "SELECT * FROM s3Object WHERE elb_response_code='123' AND ssl_protocol='-'";
    expect((await new S3Selectable(params).filterPartitionsByQuery(sql)).sort()).toEqual([]);
  });
});

describe("Test selectObjectContent", () => {
  it("first verify mockedReadable", async () => {
    const readable = new MockedSelectStream();
    const rows = await new Promise(r => {
      const rows: string[] = [];
      readable.on("data", chunk => rows.push(Buffer.from(chunk).toString()));
      readable.on("end", () => r(rows));
    });
    await new Promise(r => setTimeout(r, 1000));
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
      ]
    `);
  });

  it("selectObjectContent provides correct results", async () => {
    const sql = "SELECT * FROM s3Object WHERE elb_response_code='302' AND ssl_protocol='-'";
    const inpSer: InputSerialization = { CSV: {}, CompressionType: "GZIP" };
    const outSer: OutputSerialization = { JSON: {} };
    const rowsStream = await new S3Selectable(params).selectObjectContent({
      Expression: sql,
      ExpressionType: "SQL",
      InputSerialization: inpSer,
      OutputSerialization: outSer,
    });
    const rows = await new Promise(r => {
      const rows: string[] = [];
      rowsStream.on("data", chunk => rows.push(Buffer.from(chunk).toString()));
      rowsStream.on("end", () => r(rows));
    });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
      ]
    `);
  });
});

describe("Non-class based s3selectable returns correct results", () => {
  it("test", async () => {
    const table = "default.partitioned_and_bucketed_elb_logs_parquet";
    const sql = `SELECT * FROM ${table} WHERE elb_response_code='302' AND ssl_protocol='-'`;
    const rows = await s3selectable(sql);
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
      ]
    `);
  });
});
