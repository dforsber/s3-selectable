import {
  InputSerialization,
  OutputSerialization,
  S3,
  SelectObjectContentCommandInput,
  SelectObjectContentEventStream,
} from "@aws-sdk/client-s3";
import { getSQLWhereString, getTableAndDbAndExpr } from "../utils/sql-query.helper";
import stream, { Readable } from "stream";

import { Glue } from "@aws-sdk/client-glue";
import { GlueTableToS3Key } from "../mappers/glueTableToS3Keys.mapper";
import { PartitionPreFilter } from "../utils/partition-filterer";
import mergeStream from "merge-stream";

export type PartialBy<TType, TKey extends keyof TType> = Omit<TType, TKey> & Partial<Pick<TType, TKey>>;

export type TS3SelectableParams = PartialBy<
  SelectObjectContentCommandInput,
  "Bucket" | "Key" | "ExpressionType" | "OutputSerialization" | "InputSerialization"
>;

const defaults = {
  ExpressionType: "SQL",
  OutputSerialization: { JSON: {} },
};

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
  private inputSerialisation!: InputSerialization | undefined;
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
    params: TS3SelectableParams,
    onDataHandler?: (event: SelectObjectContentEventStream.RecordsMember) => void,
    onEndHandler?: (event: SelectObjectContentEventStream.RecordsMember) => void,
  ): Promise<stream> {
    await this.cacheTableMetadata();
    if (!params.Expression) throw new Error("S3 Select params Expression is required");
    const whereSql = getSQLWhereString(params.Expression, this.partitionColumns);
    const filteredPartitionValues = await this.partitionsFilter.filterPartitions(whereSql);
    const s3Keys = await this.mapper.getKeysByPartitions(filteredPartitionValues);
    const mergedParams = { ...defaults, InputSerialization: this.inputSerialisation, ...params };
    const selectStreams = await Promise.all(
      s3Keys.map((Key: string) => this.getSelectStream({ ...mergedParams, Key }, onDataHandler, onEndHandler)),
    );
    return mergeStream(selectStreams);
  }

  private async getSelectStream(
    queryParams: PartialBy<SelectObjectContentCommandInput, "Bucket">,
    onDataHandler?: (event: SelectObjectContentEventStream.RecordsMember) => void,
    onEndHandler?: (event: SelectObjectContentEventStream.RecordsMember) => void,
  ): Promise<Readable> {
    const selStream = await this.s3.selectObjectContent({ ...queryParams, Bucket: this.s3bucket });
    if (selStream.Payload === undefined) throw new Error(`No select stream for ${queryParams.Key}`);
    const data = stream.Readable.from(selStream.Payload);
    if (onDataHandler) data.on("data", onDataHandler);
    if (onEndHandler) data.on("end", onEndHandler);
    return data;
  }

  /*
   * Increased complexity is due to fetching both getTableInfo and
   * getPartitionValues concurrently (Promise.all) while doing caching
   */
  public async cacheTableMetadata(): Promise<void> {
    const partCols = await this.mapper.getTableInfo();
    [this.s3bucket, this.partitionColumns, this.inputSerialisation] = [
      partCols.Bucket,
      partCols.PartitionColumns,
      partCols.InputSerialization,
    ];
    this.partitionValues = await this.mapper.getPartitionValues();
    this.partitionsFilter = this.partitionsFilter
      ? this.partitionsFilter
      : new PartitionPreFilter(this.partitionValues, this.partitionColumns);
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
  s3: S3,
  glue: Glue,
  inpSer: InputSerialization = { CSV: {}, CompressionType: "GZIP" },
  outSer: OutputSerialization = { JSON: {} },
): Promise<string[]> {
  const [databaseName, tableName, expr] = getTableAndDbAndExpr(sql);
  const Expression = sql.replace(`${databaseName}.${tableName}`, `s3Object${expr}`);
  const selectable = new S3Selectable({ databaseName, tableName, s3, glue });
  const rowsStream = await selectable.selectObjectContent({
    Expression,
    ExpressionType: "SQL",
    InputSerialization: inpSer,
    OutputSerialization: outSer,
  });
  const data: string[] = await new Promise(resolve => {
    const rows: string[] = [];
    rowsStream.on("data", chunk => {
      if (chunk.Records?.Payload) rows.push(Buffer.from(chunk.Records.Payload).toString());
    });
    rowsStream.on("end", () => resolve(rows));
  });
  return data;
}
