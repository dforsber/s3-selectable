import { errors } from "../common/errors.enum";
import { ListObjectsV2Request } from "@aws-sdk/client-s3";
import { S3 } from "@aws-sdk/client-s3";

function notUndefined<T>(x: T | undefined): x is T {
  return x !== undefined;
}

export class S3KeysCache {
  private cachedKeys: Map<string, string[]> = new Map();
  constructor(private s3: S3 | undefined = undefined) {}

  public async getKeys(location: string): Promise<string[]> {
    if (!this.s3) throw new Error(errors.noS3);
    if (this.cachedKeys.has(location)) return <string[]>this.cachedKeys.get(location);
    const params: ListObjectsV2Request = this.getBucketAndPrefix(location);
    const keys: string[] = [];
    do {
      const { Contents, NextContinuationToken } = { ...(await this.s3.listObjectsV2(params)) };
      keys.push(...(Contents ?? []).map(k => k.Key).filter(notUndefined));
      params.ContinuationToken = NextContinuationToken;
    } while (params.ContinuationToken);
    this.cachedKeys.set(location, keys);
    return keys;
  }

  public getBucketAndPrefix(location: string): { Bucket: string; Prefix: string } {
    const vals = location.split("//").slice(1).join("//").split("/");
    const Bucket = vals.shift();
    const Prefix = vals.join("/");
    if (!Bucket) throw new Error(`Invalid S3 path: ${location}`);
    return { Bucket, Prefix };
  }
}
