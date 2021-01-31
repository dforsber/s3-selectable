import { GetPartitionsRequest, GetTableRequest, Glue } from "@aws-sdk/client-glue";
import { IS3Selectable, S3Selectable } from "./s3-selectable";
import { ISelect, TS3SelectObjectContent } from "./select-types";
import { ListObjectsV2CommandInput, S3, SelectObjectContentCommandInput } from "@aws-sdk/client-s3";
import { Readable, ReadableOptions } from "stream";
import { testTableKeys, testTableParquet, testTablePartitions } from "../common/fixtures/glue-table";

import { IS3selectableNonClass } from "./s3-selectable-nonclass";

export function getClassParams(sql = ""): IS3selectableNonClass {
  const region = { region: "eu-west-1" };
  return { sql, s3: new S3(region), glue: new Glue(region) };
}

jest.mock("@aws-sdk/client-s3");
jest.mock("@aws-sdk/client-glue");

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
        return Promise.resolve({ Table: testTableParquet });
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

function getS3Selectable(params?: Partial<IS3Selectable>): S3Selectable {
  const [databaseName, tableName] = ["default", "partitioned_and_bucketed_elb_logs_parquet"];
  return new S3Selectable({ ...getClassParams(), tableName, databaseName, ...params });
}

function getSimple(params: TS3SelectObjectContent): ISelect {
  return { selectParams: params };
}

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
        if (chunk?.Records?.Payload) rows.push(Buffer.from(chunk.Records.Payload).toString());
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

  it("selectObjectContent throws when Expression is empty", async () => {
    const selectable = getS3Selectable();
    await expect(() => selectable.select(getSimple({ Expression: "" }))).rejects.toThrowError();
  });

  it("selectObjectContent throws when ExpressionType is not SQL", async () => {
    const selectable = getS3Selectable();
    const sql = "SELECT * FROM db.t WHERE elb_response_code='302' AND ssl_protocol='-'";
    await expect(() =>
      selectable.select(getSimple({ Expression: sql, ExpressionType: "PartiQL" })),
    ).rejects.toThrowError();
  });

  it("selectObjectContent provides correct results", async () => {
    const sql = "SELECT * FROM db.t WHERE elb_response_code='302' AND ssl_protocol='-'";
    const selectable = getS3Selectable();
    await selectable.select(getSimple({ Expression: sql }));
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
    expect(s3ListObjectsV2Called).toEqual(1); // 1 partition
    expect(selectObjectContent).toEqual(10); // 10 objects
    const rowsStream = await selectable.select(getSimple({ Expression: sql }));
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
    expect(s3ListObjectsV2Called).toEqual(1); // S3 Keys are cached per partition
    expect(selectObjectContent).toEqual(20); // 2 * 10 objects
    const rows = await new Promise(r => {
      const rows: string[] = [];
      rowsStream?.on("data", chunk => {
        if (chunk?.Records?.Payload) rows.push(Buffer.from(chunk.Records.Payload).toString());
      });
      rowsStream?.on("end", () => r(rows));
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

  it("explain select works in case of filtering does not produce any keys", async () => {
    const sql = "SELECT * FROM db.t WHERE elb_response_code='nonsense' AND ssl_protocol='badInput'";
    const selectable = getS3Selectable();
    const res = await selectable.explainSelect({
      selectParams: { Expression: sql },
    });
    expect(res).toMatchInlineSnapshot(`
      Object {
        "partitionFilter": "SELECT partition FROM partitions WHERE \`elb_response_code\` = 'nonsense' AND \`ssl_protocol\` = 'badInput'",
        "preparedSelect": Object {
          "limit": 0,
          "s3Keys": Array [],
          "selectParams": Object {
            "Bucket": "dummy-test-bucket",
            "Expression": "SELECT * FROM db.t WHERE TRUE AND TRUE",
            "ExpressionType": "SQL",
            "InputSerialization": Object {
              "Parquet": Object {},
            },
            "OutputSerialization": Object {
              "JSON": Object {},
            },
          },
        },
        "tableInfo": Object {
          "Bucket": "dummy-test-bucket",
          "InputSerialization": Object {
            "Parquet": Object {},
          },
          "PartitionColumns": Array [
            "ssl_protocol",
            "elb_response_code",
          ],
        },
      }
    `);
  });

  it("select works in case of filtering does not produce any keys", async () => {
    const sql = "SELECT * FROM db.t WHERE elb_response_code='nonsense' AND ssl_protocol='badInput'";
    const selectable = getS3Selectable();
    const res = await selectable.select({
      selectParams: { Expression: sql },
    });
    expect(res).toBe(undefined);
  });

  it("explain select works", async () => {
    const sql = "SELECT * FROM db.t WHERE elb_response_code='302' AND ssl_protocol='-'";
    const selectable = getS3Selectable();
    const res = await selectable.explainSelect({
      selectParams: { Expression: sql },
    });
    expect(res).toMatchInlineSnapshot(`
      Object {
        "partitionFilter": "SELECT partition FROM partitions WHERE \`elb_response_code\` = '302' AND \`ssl_protocol\` = '-'",
        "preparedSelect": Object {
          "limit": 0,
          "s3Keys": Array [
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00000",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00001",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00002",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00003",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00004",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00005",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00006",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00007",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00008",
            "Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e/ssl_protocol=-/elb_response_code=302/20200525_195025_00011_xgnnv_bucket-00009",
          ],
          "selectParams": Object {
            "Bucket": "dummy-test-bucket",
            "Expression": "SELECT * FROM db.t WHERE TRUE AND TRUE",
            "ExpressionType": "SQL",
            "InputSerialization": Object {
              "Parquet": Object {},
            },
            "OutputSerialization": Object {
              "JSON": Object {},
            },
          },
        },
        "tableInfo": Object {
          "Bucket": "dummy-test-bucket",
          "InputSerialization": Object {
            "Parquet": Object {},
          },
          "PartitionColumns": Array [
            "ssl_protocol",
            "elb_response_code",
          ],
        },
      }
    `);
  });

  it("selectObjectContent provides correct results with onDataHandler and onEndHandler", async () => {
    const sql = "SELECT * FROM db.t WHERE elb_response_code='302' AND ssl_protocol='-'";
    const selectable = getS3Selectable();
    const rows = await new Promise(r => {
      const rows: string[] = [];
      selectable.select({
        selectParams: { Expression: sql },
        onDataHandler: chunk => rows.push(Buffer.from(chunk).toString()),
        onEndHandler: () => r(rows),
      });
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
    const sql = "SELECT * FROM db.t[*] WHERE elb_response_code='302' AND ssl_protocol='-'";
    const selectable = getS3Selectable();
    const rows = await new Promise(r => {
      const rows: string[] = [];
      selectable.select({
        selectParams: { Expression: sql },
        onDataHandler: chunk => rows.push(Buffer.from(chunk).toString()),
        onEndHandler: () => r(rows),
      });
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

  it("selectObjectContent uses LIMIT 2 by returning only 2 rows", async () => {
    const sql = "SELECT * FROM db.t[*] WHERE elb_response_code='302' AND ssl_protocol='-' LIMIT 2";
    const selectable = getS3Selectable({ logLevel: "debug" });
    const rows: string[] = await new Promise(r => {
      const rows: string[] = [];
      selectable.select({
        selectParams: { Expression: sql },
        onEventHandler: event => (!event.Records ? console.log(event) : undefined),
        onDataHandler: chunk => rows.push(Buffer.from(chunk).toString()),
        onEndHandler: () => r(rows),
      });
    });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
        "{\\"id\\":1,\\"value\\":\\"test1\\"}",
      ]
    `);
  });

  it("selectObjectContent uses LIMIT 42 by returning all rows", async () => {
    const sql = "SELECT * FROM db.t[*] WHERE elb_response_code='302' AND ssl_protocol='-' LIMIT 42";
    const selectable = getS3Selectable({ logLevel: "debug" });
    const rows: string[] = await new Promise(r => {
      const rows: string[] = [];
      selectable.select({
        selectParams: { Expression: sql },
        onEventHandler: event => (!event.Records ? console.log(event) : undefined),
        onDataHandler: chunk => rows.push(Buffer.from(chunk).toString()),
        onEndHandler: () => r(rows),
      });
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
    const selectable = new S3Selectable(params);
    await expect(async () => await selectable.select(getSimple({ Expression: sql }))).rejects.toThrowError();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
  });

  it("throws when Expression is missing", async () => {
    const s3 = new S3({ region: "eu-west-1" });
    const glue = new Glue({ region: "eu-west-1" });
    const [databaseName, tableName] = ["default", "partitioned_and_bucketed_elb_logs_parquet"];
    const params = { glue, s3, tableName, databaseName };
    const selectable = new S3Selectable(params);
    await expect(async () => await selectable.select(getSimple({ Expression: undefined }))).rejects.toThrowError();
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
  });
});
