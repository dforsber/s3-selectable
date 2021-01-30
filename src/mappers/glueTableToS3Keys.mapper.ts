import { GetPartitionsRequest, Partition, Table } from "@aws-sdk/client-glue";
import { notUndefined, verifiedPartition } from "../common/helpers";

import { IS3Selectable } from "../s3-selectable/s3-selectable";
import { InputSerialization } from "@aws-sdk/client-s3";
import { PartialBy } from "../s3-selectable/select-types";
import { S3KeysCache } from "../utils/s3KeysCache";
import { errors } from "../common/errors.enum";

export interface ITableInfo {
  Bucket: string;
  PartitionColumns: string[];
  InputSerialization: InputSerialization | undefined;
}

// Glue get-table response contains PartitionKeys, which lists partition keys and their type
// Glue get-partitions response contains each partition including its Values list.

// test table: default.partitioned_and_bucketed_elb_logs_parquet

export class GlueTableToS3Key {
  private table!: Table;
  private tableLocation!: string;
  private inputSerialization!: InputSerialization | undefined;
  private tableBucket!: string;
  private partitions!: Partition[];
  private partCols!: string[];
  private s3KeysFetcher = new S3KeysCache(this.params.s3);

  constructor(private params: PartialBy<IS3Selectable, "s3" | "glue">) {}

  public async getTable(): Promise<Table> {
    if (this.table) return this.table;
    this.table = await this.getDefinedTable();
    this.tableLocation = await this.getDefinedTableLocation();
    this.inputSerialization = await this.getInputSerialisation();
    this.tableBucket = this.s3KeysFetcher.getBucketAndPrefix(this.tableLocation).Bucket;
    this.partCols = this.table.PartitionKeys?.map(col => col.Name).filter(notUndefined) ?? [];
    return this.table;
  }

  private async getDefinedTable(): Promise<Table> {
    if (this.table) return this.table;
    if (!this.params.glue) throw new Error(errors.noGlue);
    const { databaseName: DatabaseName, tableName: Name } = this.params;
    const table = (await this.params.glue.getTable({ DatabaseName, Name })).Table;
    if (!table) throw new Error(`Table not found: ${Name}`);
    return table;
  }

  private async getDefinedTableLocation(): Promise<string> {
    await this.getDefinedTable();
    const tableLocation = this.table.StorageDescriptor?.Location;
    if (!tableLocation) throw new Error(`No S3 Bucket found for table ${this.params.tableName}`);
    return tableLocation;
  }

  public async getTableInfo(): Promise<ITableInfo> {
    if (!this.tableBucket || !this.partCols) await this.getTable();
    return {
      Bucket: this.tableBucket,
      PartitionColumns: this.partCols,
      InputSerialization: this.inputSerialization,
    };
  }

  public async getKeysByPartitions(values: string[]): Promise<string[]> {
    await this.getTable();
    await this.getPartitions();
    const partitionLocs = this.partitions
      .filter(verifiedPartition)
      .map(p => ({ ...p, colStr: this.getPartColsString(p.Values) }))
      .filter(p => values.length <= 0 || values.some(v => v.includes(p.colStr)))
      .map(p => p.StorageDescriptor.Location)
      .filter(notUndefined);
    const keys = await Promise.all(partitionLocs.map(loc => this.s3KeysFetcher.getKeys(loc)));
    return keys.reduce((acc, curr) => [...acc, ...curr], []);
  }

  public async getPartitions(): Promise<Partition[]> {
    if (this.partitions) return this.partitions;
    if (!this.params.glue) throw new Error(errors.noGlue);
    const { databaseName: DatabaseName, tableName: TableName } = this.params;
    const params: GetPartitionsRequest = { DatabaseName, TableName };
    this.partitions = [];
    do {
      const { Partitions, NextToken } = await this.params.glue.getPartitions(params);
      if (Partitions) this.partitions.push(...Partitions);
      params.NextToken = NextToken;
    } while (params.NextToken);
    return this.partitions;
  }

  private getPartColsString(values: string[]): string {
    return this.partCols.reduce((a, c, i) => `${a}/${c}=${values[i]}`, "");
  }

  public async getPartitionValues(): Promise<string[]> {
    await this.getTable();
    await this.getPartitions();
    return this.partitions.filter(verifiedPartition).map(p => this.getPartColsString(p.Values));
  }

  // This is rudimentary and assumes e.g. that CSV files are GZIP compressed.
  private async getInputSerialisation(): Promise<InputSerialization | undefined> {
    await this.getDefinedTable();
    const desc = this.table.StorageDescriptor;
    if (!desc?.SerdeInfo?.SerializationLibrary) return;
    const serLib = desc.SerdeInfo.SerializationLibrary.toLowerCase();
    if (serLib.includes("json")) return { JSON: { Type: "DOCUMENT" } };
    if (serLib.includes("simple")) return { CSV: {}, CompressionType: "GZIP" };
    if (serLib.includes("parquet")) return { Parquet: {} };
    return;
  }
}
