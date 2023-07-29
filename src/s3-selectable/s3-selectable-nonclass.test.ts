import { mockClient } from "aws-sdk-client-mock";
import { GetPartitionsCommand, GetTableCommand, Glue } from "@aws-sdk/client-glue";
import { IS3selectableNonClass, S3SelectableNonClass } from "./s3-selectable-nonclass";
import { ListObjectsV2Command, S3, SelectObjectContentCommand } from "@aws-sdk/client-s3";
import { Readable, ReadableOptions } from "stream";
import { testTableKeys, testTableParquet, testTablePartitions } from "../common/fixtures/glue-table";
import { GlueClient } from "@aws-sdk/client-glue";
import { S3Client } from "@aws-sdk/client-s3";

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

const glueMock = mockClient(GlueClient);
glueMock
  .on(GetTableCommand)
  .callsFake(params => {
    glueGetTableCalled++;
    if (params.Name !== "partitioned_and_bucketed_elb_logs_parquet")
      return Promise.reject(`Table not found: ${params.Name}`);
    return Promise.resolve({ Table: testTableParquet });
  })
  .on(GetPartitionsCommand)
  .callsFake(_params => {
    glueGetPartitionsCalled++;
    return Promise.resolve({ Partitions: testTablePartitions });
  });

const s3Mock = mockClient(S3Client);
s3Mock
  .on(ListObjectsV2Command)
  .callsFake(params => {
    s3ListObjectsV2Called++;
    const pref = params.Prefix ?? "";
    return Promise.resolve({
      NextContinuationToken: undefined,
      Contents: testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k })),
      $metadata: null,
    });
  })
  .on(SelectObjectContentCommand)
  .callsFake(params => {
    selectObjectContent++;
    if (params.Expression?.includes("noPayload")) return Promise.resolve(new MockedSelectStreamNoPayload());
    if (params.Expression?.includes("noData")) return Promise.resolve(new MockedSelectStreamNoData());
    return Promise.resolve(new MockedSelectStream());
  });

export function getNonClassParams(sql = ""): IS3selectableNonClass {
  const region = { region: "eu-west-1" };
  return { sql, s3: new S3(region), glue: new Glue(region) };
}

describe.only("Non-class based s3selectable returns correct results", () => {
  beforeEach(() => {
    glueGetTableCalled = 0;
    glueGetPartitionsCalled = 0;
    s3ListObjectsV2Called = 0;
    selectObjectContent = 0;
  });

  it("valid output", async () => {
    const table = "`default`.`partitioned_and_bucketed_elb_logs_parquet`";
    const sql = `SELECT * FROM ${table} WHERE elb_response_code='302' AND ssl_protocol='-'`;
    const rows = await S3SelectableNonClass(getNonClassParams(sql));
    expect(glueGetTableCalled).toEqual(1);
    expect(glueGetPartitionsCalled).toEqual(1); // 1 table
    expect(s3ListObjectsV2Called).toEqual(1); // 1 partition
    expect(selectObjectContent).toEqual(10); // 10 objects
    expect(rows.map(row => Buffer.from(row).toString())).toMatchSnapshot();
  });

  it("no data payload", async () => {
    const table = "`default`.`partitioned_and_bucketed_elb_logs_parquet`";
    const sql = `SELECT * FROM ${table} WHERE noData='true'`;
    const rows = await S3SelectableNonClass(getNonClassParams(sql));
    expect(rows).toEqual([]);
  });
});
