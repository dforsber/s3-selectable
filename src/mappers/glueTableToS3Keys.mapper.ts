import { errors } from "../common/errors.enum";
import { GetPartitionsRequest, Partition, Table, Token } from "aws-sdk/clients/glue";
import { IS3Selectable, PartialBy } from "../services/s3-selectable";
import { S3LocationToKeys } from "./s3locationToS3Keys.mapper";

export interface ITableInfo {
  Bucket: string;
  PartitionColumns: string[];
}

// Glue get-table response contains PartitionKeys, which lists partition keys and their type
// Glue get-partitions response contains each partition including its Values list.

// test table: default.partitioned_and_bucketed_elb_logs_parquet

export class GlueTableToS3Key {
  private table!: Table;
  private tableLocation!: string;
  private tableBucket!: string;
  private partitions!: Partition[];
  private partitionColumns!: string[];
  private partitionLocations!: string[];
  private s3Keys: string[] | undefined = undefined;

  constructor(private params: PartialBy<IS3Selectable, "s3" | "glue">) {}

  public async getTable(): Promise<Table> {
    if (this.table) return this.table;
    if (!this.params.glue) throw new Error(errors.noGlue);
    const [DatabaseName, Name] = [this.params.databaseName, this.params.tableName];
    const table = (await this.params.glue.getTable({ DatabaseName, Name }).promise()).Table;
    if (!table) throw new Error(`Table not found: ${Name}`);
    const tableLocation = table.StorageDescriptor?.Location;
    if (!tableLocation) throw new Error(`No S3 Bucket found for table ${Name}`);
    this.tableLocation = tableLocation;
    this.tableBucket = new S3LocationToKeys(this.tableLocation, this.params.s3).getBucketAndPrefix().Bucket;
    this.partitionColumns = table.PartitionKeys?.map(col => col.Name) ?? [];
    this.table = table;
    return this.table;
  }

  public async getTableInfo(): Promise<ITableInfo> {
    const [Bucket, PartitionColumns] = [this.tableBucket, this.partitionColumns];
    if (Bucket && PartitionColumns) return { Bucket, PartitionColumns };
    await this.getTable();
    return { Bucket: this.tableBucket, PartitionColumns: this.partitionColumns };
  }

  public async getAllKeys(): Promise<string[]> {
    if (this.s3Keys) return this.s3Keys;
    await this.getTable();
    await this.getPartitionLocations();
    const locations = this.partitionColumns.length ? this.partitionLocations : [this.tableLocation];
    const allKeys = await Promise.all(locations.map(loc => new S3LocationToKeys(loc, this.params.s3).getKeys()));
    this.s3Keys = allKeys.reduce((acc, curr) => [...acc, ...curr], []);
    return this.s3Keys;
  }

  public async getKeysByPartitions(values: string[]): Promise<string[]> {
    await this.getTable();
    await this.getPartitions();
    const partitionLocs = this.partitions
      .map(p => ({
        ...p,
        Value: this.partitionColumns.reduce((a, c, i) => `${a}/${c}=${p.Values ? p.Values[i] : "ValueUndefined"}`, ""),
      }))
      .filter(p => values.some(v => v.includes(p.Value)))
      .map(p => p.StorageDescriptor?.Location);
    const allKeys = await Promise.all(partitionLocs.map(loc => new S3LocationToKeys(loc, this.params.s3).getKeys()));
    this.s3Keys = allKeys.reduce((acc, curr) => [...acc, ...curr], []);
    return this.s3Keys;
  }

  public async getPartitions(): Promise<Partition[]> {
    if (this.partitions) return this.partitions;
    if (!this.params.glue) throw new Error(errors.noGlue);
    const params = { DatabaseName: this.params.databaseName, TableName: this.params.tableName };
    this.partitions = [];
    let token: Token | undefined;
    do {
      const p: GetPartitionsRequest = token ? { ...params, NextToken: token } : params;
      const { Partitions, NextToken } = await this.params.glue.getPartitions(p).promise();
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

  public async getPartitionLocations(): Promise<string[]> {
    if (this.partitionLocations) return this.partitionLocations;
    await this.getPartitions();
    this.partitionLocations = <string[]>this.partitions.map(p => p.StorageDescriptor?.Location).filter(p => !!p);
    return this.partitionLocations;
  }
}
