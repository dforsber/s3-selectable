import {
  IS3Selectable,
  IS3selectableNonClass,
  ISelectObjectContent,
  PartialBy,
  TEvents,
  TS3electObjectContentVerified,
  TSelectParamsVerified,
  TSelectaParams,
  defaultS3SelectParms,
} from "./types";
import { InputSerialization, SelectObjectContentCommandInput } from "@aws-sdk/client-s3";
import { getSQLLimit, getSQLWhereString, getTableAndDbAndExpr, setSQLLimit } from "../utils/sql-query.helper";
import stream, { Readable } from "stream";

import { GlueTableToS3Key } from "../mappers/glueTableToS3Keys.mapper";
import { PartitionPreFilter } from "../utils/partition-filterer";
import { createLogger } from "bunyan";
import mergeStream from "merge-stream";

export class S3Selectable {
  private logger = createLogger({ name: "s3-selectable", level: this.props.logLevel ?? "info" });
  private partitionColumns!: string[];
  private inputSerialisation!: InputSerialization | undefined;
  private s3bucket!: string;
  private partitionsFilter!: PartitionPreFilter;
  private merged!: stream;
  private mapper = new GlueTableToS3Key({
    s3: this.props.s3,
    glue: this.props.glue,
    databaseName: this.props.databaseName,
    tableName: this.props.tableName,
  });

  constructor(private props: IS3Selectable) {}

  public async selectObjectContent(params: ISelectObjectContent): Promise<stream> {
    await this.cacheTableMetadata();
    const selectParamsVerified = this.getValidS3SelectParams(params.selectParams);
    const limit = getSQLLimit(selectParamsVerified.Expression);
    const s3Keys = await this.getFilteredS3Keys(selectParamsVerified.Expression, limit);
    this.logger.debug("number of S3 Keys:", s3Keys.length);
    const selectParams = this.setLimitIfNeeded(s3Keys, selectParamsVerified, limit);
    await this.getMergedStream(s3Keys, selectParams);
    this.setHandlers({ ...params, selectParams }, limit);
    return this.merged;
  }

  private setLimitIfNeeded(s3Keys: string[], params: TSelectParamsVerified, limit: number): TSelectParamsVerified {
    if (s3Keys.length >= limit) return { ...params, Expression: setSQLLimit(params.Expression, 1) };
    return { ...params, Expression: setSQLLimit(params.Expression, Math.ceil(limit / s3Keys.length)) };
  }

  private async getMergedStream(s3Keys: string[], s3sel: TSelectParamsVerified): Promise<void> {
    this.logger.info(s3sel.Expression);
    this.merged = mergeStream(await Promise.all(s3Keys.map((Key: string) => this.getSelectStream({ ...s3sel, Key }))));
  }

  private async getFilteredS3Keys(sql: string, limit: number): Promise<string[]> {
    const filteredPartitionValues = await this.getFilteredPartitionValues(sql);
    const keys = await this.mapper.getKeysByPartitions(filteredPartitionValues);
    return limit > 0 ? keys.splice(0, limit) : keys;
  }

  private getFilteredPartitionValues(sql: string): Promise<string[]> {
    const whereSql = getSQLWhereString(sql, this.partitionColumns);
    return this.partitionsFilter.filterPartitions(whereSql);
  }

  private getValidS3SelectParams(params: TSelectaParams): TSelectParamsVerified {
    const merged = { ...defaultS3SelectParms, ...params };
    if (!merged.Expression) throw new Error("S3 Select param Expression is required");
    if (merged.ExpressionType !== "SQL") {
      throw new Error("S3 Select param ExpressionType must be SQL");
    }
    return {
      ...merged,
      Expression: merged.Expression,
      ExpressionType: merged.ExpressionType,
      InputSerialization: this.inputSerialisation,
    };
  }

  private setHandlers(params: TS3electObjectContentVerified, limit: number): void {
    const { onEventHandler, onDataHandler, onEndHandler } = params;
    if (onEventHandler) this.merged.on("data", onEventHandler);
    if (onDataHandler) this.merged.on("data", this.getDataHandler(onDataHandler, limit));
    if (onEndHandler) this.merged.on("end", onEndHandler);
  }

  private getDataHandler(onDataHandler: (data: Uint8Array) => void, limit: number): (event: TEvents) => void {
    // NOTE: We use closure to preserve a counter
    const useLimit = limit > 0; // if 0, limit is disabled
    let left = limit;
    return (event: TEvents): void => {
      if (event.Records?.Payload) {
        if (useLimit) this.logger.debug("total limit:", left);
        if (!useLimit || (useLimit && left-- > 0)) return onDataHandler(event.Records.Payload);
        return;
      }
    };
  }

  private async getSelectStream(queryParams: PartialBy<SelectObjectContentCommandInput, "Bucket">): Promise<Readable> {
    const selStream = await this.props.s3.selectObjectContent({ ...queryParams, Bucket: this.s3bucket });
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
    const partitionValues = await this.mapper.getPartitionValues();
    this.partitionsFilter = this.partitionsFilter
      ? this.partitionsFilter
      : new PartitionPreFilter(partitionValues, this.partitionColumns);
  }
}

/*
 * Alternative, non-class based interface that does not do any caching of Table data. We recommend
 * to use the class based interface for latency aware use cases where multiple S3 Select commands
 * are run over the same table and in cases where the class can be instantiated beforehand to fill
 * up the cache.
 */
export async function s3selectableNonClass(params: IS3selectableNonClass): Promise<Uint8Array[]> {
  const { s3, glue } = params;
  const [databaseName, tableName, expr] = getTableAndDbAndExpr(params.sql);
  const Expression = params.sql.replace(`${databaseName}.${tableName}`, `s3Object${expr}`);
  const selectable = new S3Selectable({ databaseName, tableName, s3, glue });
  const data: Uint8Array[] = await new Promise(resolve => {
    const chunks: Uint8Array[] = [];
    selectable.selectObjectContent({
      selectParams: { ...params, Expression },
      onDataHandler: chunk => chunks.push(chunk),
      onEndHandler: () => resolve(chunks),
    });
  });
  return data;
}
