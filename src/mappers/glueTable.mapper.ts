import Glue, { Table, Token, GetPartitionsRequest } from "aws-sdk/clients/glue";
import { IS3SelectOnTable } from "../s3Selectable";
import { ObjectKey, NextToken, ListObjectsV2Request } from "aws-sdk/clients/s3";

export class GlueTableToS3KeysMapper {
  private glue: Glue = this.params.glue;
  private table: Promise<Table> = this.getTable();

  constructor(private params: IS3SelectOnTable) {}

  private async getTableBucket(): Promise<string> {
    const t = await this.table;
    const loc = t.StorageDescriptor?.Location;
    const bucket = GlueTableToS3KeysMapper.getBucketAndPrefix(loc)[0];
    if (!bucket) throw new Error(`Invalid Table Location, must be S3 (can not find Bucket): ${loc}`);
    return bucket;
  }

  public async getS3Keys(): Promise<{ Bucket: string; Keys: string[] }> {
    return {
      Bucket: await this.getTableBucket(),
      Keys: await this.getKeys(),
    };
  }

  private async getKeys(): Promise<string[]> {
    const tableLoc = await this.getTableLocation();
    const partLocs = await this.getPartitionLocations();
    const allKeys = await Promise.all([tableLoc, ...partLocs].map(loc => this.getKeysOnLocation(loc)));
    return allKeys.reduce((acc, curr) => [...acc, ...curr], []);
  }

  private async getTable(): Promise<Table> {
    const DatabaseName = this.params.databaseName;
    const Name = this.params.tableName;
    const resp = await this.glue.getTable({ DatabaseName, Name }).promise();
    if (resp.Table === undefined) throw new Error(`Table not found: ${Name}`);
    return resp.Table;
  }

  private async getTableLocation(): Promise<string> {
    const t = await this.table;
    const loc = t.StorageDescriptor?.Location;
    if (loc === undefined) throw new Error(`Table Location not found ${JSON.stringify(t)}`);
    return loc;
  }

  private async getPartitionLocations(): Promise<string[]> {
    const DatabaseName = this.params.databaseName;
    const TableName = this.params.tableName;
    const params = { DatabaseName, TableName };
    let allLocations: string[] = [];
    let token: Token | undefined = undefined;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const p: GetPartitionsRequest = token ? { ...params, NextToken: token } : params;
      const { Partitions, NextToken } = await this.glue.getPartitions(p).promise();
      if (!Partitions) throw new Error(`No partitions: ${TableName}`);
      const locations = <string[]>Partitions.map(p => p.StorageDescriptor?.Location).filter(p => p !== undefined);
      allLocations = allLocations.concat(locations);
      if (NextToken === undefined) break;
      token = NextToken;
    }
    return allLocations;
  }

  private async getKeysOnLocation(location: string): Promise<string[]> {
    const [Bucket, Prefix] = GlueTableToS3KeysMapper.getBucketAndPrefix(location);
    const params = { Bucket, Prefix };
    let keys: Array<ObjectKey | undefined> = [];
    let token: NextToken | undefined = undefined;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const p: ListObjectsV2Request = token ? { ...params, ContinuationToken: token } : params;
      const { IsTruncated, Contents, NextContinuationToken } = await this.params.s3.listObjectsV2(p).promise();
      token = NextContinuationToken;
      if (!Contents) throw new Error(`Invalid Contents for location: ${location}`);
      keys = keys.concat(Contents.map(k => k.Key));
      if (!IsTruncated) break;
    }
    return <string[]>keys.filter(k => !!k);
  }

  public static getBucketAndPrefix(location: string | undefined): [string, string] {
    const vals = location?.split("//")?.pop()?.split("/");
    const Bucket = vals?.shift();
    const Prefix = vals?.join("/");
    if (!Bucket || !Prefix) throw new Error(`Invalid S3 path: ${location}`);
    return [Bucket, Prefix];
  }
}
