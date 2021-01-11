import { errors } from "../common/errors.enum";
import { GetPartitionsRequest, Partition, StorageDescriptor, Table } from "@aws-sdk/client-glue";
import { IS3Selectable, PartialBy } from "../s3-selectable/s3-selectable";
import { S3KeysCache } from "../utils/s3KeysCache";
import { InputSerialization } from "@aws-sdk/client-s3";

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
  private partitionColumns!: string[];
  private s3KeysFetcher = new S3KeysCache(this.params.s3);

  constructor(private params: PartialBy<IS3Selectable, "s3" | "glue">) {}

  public async getTable(): Promise<Table> {
    if (this.table) return this.table;
    if (!this.params.glue) throw new Error(errors.noGlue);
    const [DatabaseName, Name] = [this.params.databaseName, this.params.tableName];
    const table = (await this.params.glue.getTable({ DatabaseName, Name })).Table;
    if (!table) throw new Error(`Table not found: ${Name}`);
    const tableLocation = table.StorageDescriptor?.Location;
    if (!tableLocation) throw new Error(`No S3 Bucket found for table ${Name}`);
    this.tableLocation = tableLocation;
    this.inputSerialization = this.getInputSerialisation(table.StorageDescriptor);
    this.tableBucket = this.s3KeysFetcher.getBucketAndPrefix(this.tableLocation).Bucket;
    this.partitionColumns = table.PartitionKeys?.map(col => col.Name || "").filter(e => e) ?? [];
    this.table = table;
    return this.table;
  }

  public async getTableInfo(): Promise<ITableInfo> {
    if (!this.tableBucket || !this.partitionColumns) await this.getTable();
    return {
      Bucket: this.tableBucket,
      PartitionColumns: this.partitionColumns,
      InputSerialization: this.inputSerialization,
    };
  }

  public async getKeysByPartitions(values: string[]): Promise<string[]> {
    await this.getTable();
    await this.getPartitions();
    const partitionLocs = this.partitions
      .map(p => ({
        ...p,
        Value: this.partitionColumns.reduce((a, c, i) => `${a}/${c}=${p.Values ? p.Values[i] : "ValueUndefined"}`, ""),
      }))
      .filter(p => values.length <= 0 || values.some(v => v.includes(p.Value)))
      .map(p => p.StorageDescriptor?.Location)
      .filter(l => !!l);
    const keys = await Promise.all(partitionLocs.map(loc => this.s3KeysFetcher.getKeys(<string>loc)));
    return keys.reduce((acc, curr) => [...acc, ...curr], []);
  }

  public async getPartitions(): Promise<Partition[]> {
    if (this.partitions) return this.partitions;
    if (!this.params.glue) throw new Error(errors.noGlue);
    const params = { DatabaseName: this.params.databaseName, TableName: this.params.tableName };
    this.partitions = [];
    let token: string | undefined;
    do {
      const p: GetPartitionsRequest = token ? { ...params, NextToken: token } : params;
      const { Partitions, NextToken } = await this.params.glue.getPartitions(p);
      if (!Partitions) return [];
      this.partitions.push(...Partitions);
      token = NextToken;
    } while (token);
    return this.partitions;
  }

  public async getPartitionValues(): Promise<string[]> {
    await this.getTable();
    await this.getPartitions();
    return this.partitions
      .map(p => p.Values)
      .map(v => this.partitionColumns.reduce((a, c, i) => `${a}/${c}=${v ? v[i] : "ValueUndefined"}`, ""));
  }

  private getInputSerialisation(desc?: StorageDescriptor): InputSerialization | undefined {
    if (!desc || !desc.SerdeInfo || !desc.SerdeInfo.SerializationLibrary) return;
    const serLib = desc.SerdeInfo.SerializationLibrary.toLowerCase();
    if (serLib.includes("json")) return { JSON: {} };
    if (serLib.includes("simple")) return { CSV: {} };
    if (serLib.includes("parquet")) return { Parquet: {} };
    return;
  }
}
