import mergeStream from "merge-stream";
import { getSQLWhereString, getTableAndDb } from "../utils/sql-query.helper";
import { Glue, S3 } from "aws-sdk";
import { GlueTableToS3Key } from "../mappers/glueTableToS3Keys.mapper";
import { PartitionPreFilter } from "../utils/partition-filterer";
import { StreamingEventStream } from "aws-sdk/lib/event-stream/event-stream";
import {
  ContinuationEvent,
  EndEvent,
  InputSerialization,
  OutputSerialization,
  ProgressEvent,
  RecordsEvent,
  SelectObjectContentRequest,
  StatsEvent,
} from "aws-sdk/clients/s3";

export type PartialBy<TType, TKey extends keyof TType> = Omit<TType, TKey> & Partial<Pick<TType, TKey>>;

export interface IEventStream {
  Records?: RecordsEvent;
  Stats?: StatsEvent;
  Progress?: ProgressEvent;
  Cont?: ContinuationEvent;
  End?: EndEvent;
}

export type SelectStream = StreamingEventStream<IEventStream>;

export interface IS3Selectable {
  tableName: string;
  databaseName: string;
  glue: Glue;
  s3: S3;
}

export class S3Selectable {
  private s3 = this.params.s3;
  private glue = this.params.glue;
  private partitionValues!: string[];
  private partitionColumns!: string[];
  private s3bucket!: string;
  private partitionsFilter!: PartitionPreFilter;
  private mapper = new GlueTableToS3Key({
    s3: this.s3,
    glue: this.glue,
    databaseName: this.params.databaseName,
    tableName: this.params.tableName,
  });

  constructor(private params: IS3Selectable) {}

  public async selectObjectContent(
    params: PartialBy<SelectObjectContentRequest, "Bucket" | "Key">,
  ): Promise<SelectStream> {
    await this.cacheTableMetadata();
    const whereSql = getSQLWhereString(params.Expression, this.partitionColumns);
    const filteredPartitionValues = await this.partitionsFilter.filterPartitions(whereSql);
    const s3Keys = await this.mapper.getKeysByPartitions(filteredPartitionValues);
    const selectStreams = await Promise.all(s3Keys.map((Key: string) => this.getSelectStream({ ...params, Key })));
    return mergeStream(...selectStreams.filter(stream => !!stream));
  }

  private async getSelectStream(queryParams: PartialBy<SelectObjectContentRequest, "Bucket">): Promise<SelectStream> {
    const stream = await this.s3.selectObjectContent({ ...queryParams, Bucket: this.s3bucket }).promise();
    if (stream.Payload === undefined) throw new Error(`No select stream for ${queryParams.Key}`);
    return <SelectStream>stream.Payload;
  }

  /*
   * Increased complexity is due to fetching both getTableInfo and
   * getPartitionValues concurrently (Promise.all) while doing caching
   */
  private async cacheTableMetadata(): Promise<void> {
    const partCols = await this.mapper.getTableInfo();
    [this.s3bucket, this.partitionColumns] = [partCols.Bucket, partCols.PartitionColumns];
    this.partitionValues = await this.mapper.getPartitionValues();
    this.partitionsFilter = new PartitionPreFilter(this.partitionValues, this.partitionColumns);
  }
}

/*
 * Alternative, non-class based interface that does not do any caching of Table data. We recommend
 * to use the class based interface for latency aware use cases where multiple S3 Select commands
 * are run over the same table and in cases where the class can be instantiated beforehand to fill
 * up the cache.
 */
export async function s3selectableNonClass(
  sql: string,
  inpSer: InputSerialization = { CSV: {}, CompressionType: "GZIP" },
  outSer: OutputSerialization = { JSON: {} },
): Promise<string[]> {
  const [databaseName, tableName] = getTableAndDb(sql);
  const Expression = sql.replace(`${databaseName}.${tableName}`, "s3Object");
  const selectable = new S3Selectable({ databaseName, tableName, s3: new S3(), glue: new Glue() });
  const rowsStream = await selectable.selectObjectContent({
    Expression,
    ExpressionType: "SQL",
    InputSerialization: inpSer,
    OutputSerialization: outSer,
  });
  const data: string[] = await new Promise(resolve => {
    const d: string[] = [];
    rowsStream.on("end", () => resolve(d));
    rowsStream.on("data", chunk => d.push(Buffer.from(chunk).toString()));
  });
  return data;
}
