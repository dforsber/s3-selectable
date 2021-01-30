import {
  IExplainSelect,
  IPreparedSelect,
  ISelect,
  TS3SelectObjectContent,
  TS3SelectObjectContentVerified,
  TS3electObjectContentVerified,
} from "./select-types";
import { InputSerialization, S3, SelectObjectContentCommandInput } from "@aws-sdk/client-s3";
import { TEvents, defaultS3SelectParms } from "./select-types";
import { getNonPartsSQL, getPartsOnlySQLWhereString, getSQLLimit, setSQLLimit } from "../utils/sql-query.helper";
import stream, { Readable } from "stream";

import { Glue } from "@aws-sdk/client-glue";
import { GlueTableToS3Key } from "../mappers/glueTableToS3Keys.mapper";
import { PartitionPreFilter } from "../utils/partition-filterer";
import { createLogger } from "bunyan";
import mergeStream from "merge-stream";

export interface IS3Selectable {
  tableName: string;
  databaseName: string;
  glue: Glue;
  s3: S3;
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
}

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

  constructor(public props: IS3Selectable) {}

  public async select(params: ISelect): Promise<stream> {
    return this.executeSelect(await this.prepareSelect(params));
  }

  public async explainSelect(params: ISelect): Promise<IExplainSelect> {
    this.logger.debug("explain select:", params);
    const preparedSelect = await this.prepareSelect(params);
    const tableInfo = await this.mapper.getTableInfo();
    return { tableInfo, preparedSelect };
  }

  private async prepareSelect(params: ISelect): Promise<IPreparedSelect> {
    await this.cacheTableMetadata();
    const selectParamsVerified = this.getValidS3SelectParams(params.selectParams);
    const limit = getSQLLimit(selectParamsVerified.Expression);
    const s3Keys = await this.getFilteredS3Keys(selectParamsVerified.Expression, limit);
    this.logger.debug("number of S3 Keys:", s3Keys.length);
    const selectParams = this.setLimitIfNeeded(s3Keys, selectParamsVerified, limit);
    selectParams.Expression = getNonPartsSQL(selectParams.Expression, this.partitionColumns);
    return { ...params, limit, s3Keys, selectParams };
  }

  private async executeSelect(params: IPreparedSelect): Promise<stream> {
    const { s3Keys, selectParams, limit } = params;
    await this.getMergedStream(s3Keys, selectParams);
    this.setHandlers({ ...params, selectParams }, limit);
    return this.merged;
  }

  private setLimitIfNeeded(
    s3Keys: string[],
    params: TS3SelectObjectContentVerified,
    limit: number,
  ): TS3SelectObjectContentVerified {
    if (s3Keys.length >= limit) return { ...params, Expression: setSQLLimit(params.Expression, 1) };
    return { ...params, Expression: setSQLLimit(params.Expression, Math.ceil(limit / s3Keys.length)) };
  }

  private async getMergedStream(s3Keys: string[], s3sel: TS3SelectObjectContentVerified): Promise<void> {
    this.logger.info(s3sel.Expression);
    this.merged = mergeStream(await Promise.all(s3Keys.map((Key: string) => this.getSelectStream({ ...s3sel, Key }))));
  }

  private async getFilteredS3Keys(sql: string, limit: number): Promise<string[]> {
    const filteredPartitionValues = await this.getFilteredPartitionValues(sql);
    const keys = await this.mapper.getKeysByPartitions(filteredPartitionValues);
    return limit > 0 ? keys.splice(0, limit) : keys;
  }

  private getFilteredPartitionValues(sql: string): Promise<string[]> {
    const whereSql = getPartsOnlySQLWhereString(sql, this.partitionColumns);
    return this.partitionsFilter.filterPartitions(whereSql);
  }

  private getValidS3SelectParams(params: TS3SelectObjectContent): TS3SelectObjectContentVerified {
    const merged = { ...defaultS3SelectParms, ...params };
    if (!merged.Expression) throw new Error("S3 Select param Expression is required");
    if (merged.ExpressionType !== "SQL") {
      throw new Error("S3 Select param ExpressionType must be SQL");
    }
    return {
      ...merged,
      Bucket: this.s3bucket,
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

  private async getSelectStream(queryParams: SelectObjectContentCommandInput): Promise<Readable> {
    const selStream = await this.props.s3.selectObjectContent(queryParams);
    if (selStream.Payload === undefined) throw new Error(`No select stream for ${queryParams.Key}`);
    return stream.Readable.from(selStream.Payload, { objectMode: true });
  }

  public async cacheTableMetadata(): Promise<void> {
    if (this.s3bucket) return;
    const info = await this.mapper.getTableInfo();
    const partitionValues = await this.mapper.getPartitionValues();
    this.partitionColumns = info.PartitionColumns;
    this.inputSerialisation = info.InputSerialization;
    this.partitionsFilter = new PartitionPreFilter(partitionValues, this.partitionColumns);
    this.s3bucket = info.Bucket;
  }
}
