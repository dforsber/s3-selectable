import { GetPartitionsRequest, GetTableRequest } from "@aws-sdk/client-glue";
import {
  InputSerialization,
  ListObjectsV2CommandInput,
  OutputSerialization,
  S3,
  SelectObjectContentCommandInput,
} from "@aws-sdk/client-s3";
import { Readable, ReadableOptions } from "stream";
import { S3Selectable, s3selectableNonClass } from "./s3-selectable";
import { testTable, testTableKeys, testTablePartitions } from "../common/fixtures/glue-table";

import { Glue } from "@aws-sdk/client-glue";

jest.mock("@aws-sdk/client-s3");
jest.mock("@aws-sdk/client-glue");

const region = "eu-west-1";

class MockedSelectStream extends Readable {
  public Payload: Readable | undefined = this;
  public $metadata: undefined;
  private index = 0;
  private rows = [
    { id: 1, value: "test1" },
    { id: 2, value: "test2" },
  ];

  constructor(opt?: ReadableOptions) {
    super({ ...opt, objectMode: true });
  }

  public _read(_size: number): void {
    this.index >= this.rows.length
      ? this.push(null)
      : this.push({ Records: { Payload: Buffer.from(JSON.stringify(this.rows[this.index++]), "ascii") } });
  }
}

class MockedSelectStreamNoPayload extends MockedSelectStream {
  public Payload = undefined;

  constructor(opt?: ReadableOptions) {
    super(opt);
  }
}

class MockedSelectStreamNoData extends MockedSelectStream {
  public Payload: Readable | undefined = this;
  private sent = false;

  constructor(opt?: ReadableOptions) {
    super(opt);
  }

  public _read(_size: number): void {
    this.sent ? this.push(null) : this.push({});
    this.sent = true;
  }
}

let glueGetTableCalled = 0;
let glueGetPartitionsCalled = 0;
let s3ListObjectsV2Called = 0;
let selectObjectContent = 0;

jest.mock("@aws-sdk/client-glue", () => ({
  Glue: function Glue() {
    return {
      getTable: jest.fn((params: GetTableRequest) => {
        glueGetTableCalled++;
        if (params.Name !== "partitioned_and_bucketed_elb_logs_parquet")
          return Promise.reject(`Table not found: ${params.Name}`);
        return Promise.resolve({ Table: testTable });
      }),
      getPartitions: jest.fn((_params: GetPartitionsRequest) => {
        glueGetPartitionsCalled++;
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
        return Promise.resolve({
          NextContinuationToken: undefined,
          Contents: testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k })),
          $metadata: null,
        });
      }),
      selectObjectContent: jest.fn((params: SelectObjectContentCommandInput) => {
        selectObjectContent++;
        if (params.Expression?.includes("noPayload")) return Promise.resolve(new MockedSelectStreamNoPayload());
        if (params.Expression?.includes("noData")) return Promise.resolve(new MockedSelectStreamNoData());
        return Promise.resolve(new MockedSelectStream());
      }),
    };
  },
}));

const s3 = new S3({ region: "eu-west-1" });
const glue = new Glue({ region: "eu-west-1" });
const [databaseName, tableName] = ["default", "partitioned_and_bucketed_elb_logs_parquet"];
const params = { glue, s3, tableName, databaseName };

describe("Test selectObjectContent", () => {
  beforeEach(() => {
    glueGetTableCalled = 0;
    glueGetPartitionsCalled = 0;
    s3ListObjectsV2Called = 0;
    selectObjectContent = 0;
  });

  it("first verify mockedReadable", async () => {
    const readable = new MockedSelectStream();
    const rows = await new Promise(resolve => {
      const rows: string[] = [];
      readable.on("data", chunk => {
        if (chunk.Records?.Payload) rows.push(Buffer.from(chunk.Records.Payload).toString());
      });
      readable.on("end", () => resolve(rows));
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
    const selectable = new S3Selectable(params);
    await selectable.selectObjectContent({
      Expression: sql,
      ExpressionType: "SQL",
      InputSerialization: inpSer,
      OutputSerialization: outSer,
    });
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
    expect(s3ListObjectsV2Called).toEqual(1); // 1 partition
    expect(selectObjectContent).toEqual(10); // 10 objects
    const rowsStream = await selectable.selectObjectContent({
      Expression: sql,
      ExpressionType: "SQL",
      InputSerialization: inpSer,
      OutputSerialization: outSer,
    });
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
    expect(s3ListObjectsV2Called).toEqual(1); // S3 Keys are cached per partition
    expect(selectObjectContent).toEqual(20); // 2 * 10 objects
    const rows = await new Promise(r => {
      const rows: string[] = [];
      rowsStream.on("data", chunk => {
        if (chunk?.Records?.Payload) rows.push(Buffer.from(chunk.Records.Payload).toString());
      });
      rowsStream.on("end", () => r(rows));
    });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
      ]
    `);
  });

  it("selectObjectContent provides correct results with onDataHandler and onEndHandler", async () => {
    const sql = "SELECT * FROM s3Object WHERE elb_response_code='302' AND ssl_protocol='-'";
    const inpSer: InputSerialization = { CSV: {}, CompressionType: "GZIP" };
    const outSer: OutputSerialization = { JSON: {} };
    const selectable = new S3Selectable(params);
    const rows = await new Promise(r => {
      const rows: string[] = [];
      selectable.selectObjectContent(
        {
          Expression: sql,
          ExpressionType: "SQL",
          InputSerialization: inpSer,
          OutputSerialization: outSer,
        },
        chunk => {
          if (chunk.Records?.Payload) rows.push(Buffer.from(chunk.Records.Payload).toString());
        },
        () => r(rows),
      );
    });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
      ]
    `);
  });

  it("throws when stream does not contain Payload", async () => {
    const s3 = new S3({ region: "eu-west-1" });
    const glue = new Glue({ region: "eu-west-1" });
    const [databaseName, tableName] = ["default", "partitioned_and_bucketed_elb_logs_parquet"];
    const params = { glue, s3, tableName, databaseName };
    const sql = "SELECT * FROM s3Object WHERE noPayload='true'";
    const inpSer: InputSerialization = { CSV: {}, CompressionType: "GZIP" };
    const outSer: OutputSerialization = { JSON: {} };
    const selectable = new S3Selectable(params);
    await expect(
      async () =>
        await selectable.selectObjectContent({
          Expression: sql,
          ExpressionType: "SQL",
          InputSerialization: inpSer,
          OutputSerialization: outSer,
        }),
    ).rejects.toThrowError();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
  });

  it("throws when Expression is missing", async () => {
    const s3 = new S3({ region: "eu-west-1" });
    const glue = new Glue({ region: "eu-west-1" });
    const [databaseName, tableName] = ["default", "partitioned_and_bucketed_elb_logs_parquet"];
    const params = { glue, s3, tableName, databaseName };
    const inpSer: InputSerialization = { CSV: {}, CompressionType: "GZIP" };
    const outSer: OutputSerialization = { JSON: {} };
    const selectable = new S3Selectable(params);
    await expect(
      async () =>
        await selectable.selectObjectContent({
          Expression: undefined,
          ExpressionType: "SQL",
          InputSerialization: inpSer,
          OutputSerialization: outSer,
        }),
    ).rejects.toThrowError();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
  });
});

describe("Non-class based s3selectable returns correct results", () => {
  it("valid output", async () => {
    const table = "default.partitioned_and_bucketed_elb_logs_parquet";
    const sql = `SELECT * FROM ${table} WHERE elb_response_code='302' AND ssl_protocol='-'`;
    const rows = await s3selectableNonClass(sql, new S3({ region }), new Glue({ region }));
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
        "{\\"id\\":2,\\"value\\":\\"test2\\"}",
      ]
    `);
  });

  it("no data payload", async () => {
    const table = "default.partitioned_and_bucketed_elb_logs_parquet";
    const sql = `SELECT * FROM ${table} WHERE noData='true'`;
    const rows = await s3selectableNonClass(sql, new S3({ region: "eu-west-1" }), new Glue({ region: "eu-west-1" }));
    expect(rows).toEqual([]);
  });
});
