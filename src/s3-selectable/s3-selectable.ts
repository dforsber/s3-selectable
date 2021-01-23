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

export type TS3SelectableParamsVerified = PartialBy<SelectObjectContentCommandInput, "Bucket" | "Key"> & {
  Expression: string;
  ExpressionType: string;
};

type TEvents = SelectObjectContentEventStream;

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
  private partitionValues!: string[];
  private partitionColumns!: string[];
  private inputSerialisation!: InputSerialization | undefined;
  private s3bucket!: string;
  private partitionsFilter!: PartitionPreFilter;
  private mapper = new GlueTableToS3Key({
    s3: this.params.s3,
    glue: this.params.glue,
    databaseName: this.params.databaseName,
    tableName: this.params.tableName,
  });

  constructor(private params: IS3Selectable) {}

  public async selectObjectContent(
    paramsInc: TS3SelectableParams,
    onDataHandler?: (event: Uint8Array) => void,
    onEndHandler?: () => void,
  ): Promise<stream> {
    await this.cacheTableMetadata();
    const params = this.getValidParams(paramsInc);
    const whereSql = getSQLWhereString(params.Expression, this.partitionColumns);
    const filteredPartitionValues = await this.partitionsFilter.filterPartitions(whereSql);
    const s3Keys = await this.mapper.getKeysByPartitions(filteredPartitionValues);
    const merged = mergeStream(
      await Promise.all(s3Keys.map((Key: string) => this.getSelectStream({ ...params, Key }))),
    );
    if (onDataHandler) merged.on("data", this.getDataHandler(onDataHandler));
    if (onEndHandler) merged.on("end", onEndHandler);
    return merged;
  }

  private getValidParams(params: TS3SelectableParams): TS3SelectableParamsVerified {
    const merged = { ...defaults, ...params };
    if (!merged.Expression) throw new Error("S3 Select param Expression is required");
    if (!merged.ExpressionType || merged.ExpressionType !== "SQL")
      throw new Error("S3 Select param ExpressionType must be SQL");
    return {
      ...merged,
      Expression: merged.Expression,
      ExpressionType: merged.ExpressionType,
      InputSerialization: this.inputSerialisation,
    };
  }

  private getDataHandler(onDataHandler: (data: Uint8Array) => void): (event: TEvents) => void {
    return (event: TEvents): void => {
      if (event.Records?.Payload) onDataHandler(event.Records.Payload);
    };
  }

  private async getSelectStream(queryParams: PartialBy<SelectObjectContentCommandInput, "Bucket">): Promise<Readable> {
    const selStream = await this.params.s3.selectObjectContent({ ...queryParams, Bucket: this.s3bucket });
    if (selStream.Payload === undefined) throw new Error(`No select stream for ${queryParams.Key}`);
    return stream.Readable.from(selStream.Payload, { objectMode: true });
  }

  /*
   * Increased complexity is due to fetching both getTableInfo and
   * getPartitionValues concurrently (Promise.all) while doing caching
   */
  public async cacheTableMetadata(): Promise<void> {
    const info = await this.mapper.getTableInfo();
    [this.s3bucket, this.partitionColumns, this.inputSerialisation] = [
      info.Bucket,
      info.PartitionColumns,
      info.InputSerialization,
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
export interface IS3selectableNonClass {
  sql: string;
  s3: S3;
  glue: Glue;
  InputSerialization?: InputSerialization;
  OutputSerialization?: OutputSerialization;
}
export async function s3selectableNonClass(params: IS3selectableNonClass): Promise<Uint8Array[]> {
  const { s3, glue } = params;
  const [databaseName, tableName, expr] = getTableAndDbAndExpr(params.sql);
  const Expression = params.sql.replace(`${databaseName}.${tableName}`, `s3Object${expr}`);
  const selectable = new S3Selectable({ databaseName, tableName, s3, glue });
  const data: Uint8Array[] = await new Promise(resolve => {
    const chunks: Uint8Array[] = [];
    selectable.selectObjectContent(
      { ...params, Expression },
      chunk => chunks.push(chunk),
      () => resolve(chunks),
    );
  });
  return data;
}
