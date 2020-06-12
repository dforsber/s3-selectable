import { errors } from "../common/errors.enum";
import { ListObjectsV2Request, NextToken, ObjectKey } from "aws-sdk/clients/s3";

export class S3LocationToKeys {
  private cachedKeys: Map<string, string[]> = new Map();
  constructor(private s3: AWS.S3 | undefined = undefined) {}

  public async getKeys(location: string): Promise<string[]> {
    if (!this.s3) throw new Error(errors.noS3);
    if (this.cachedKeys.has(location)) return this.cachedKeys.get(location) ?? [];
    const params = this.getBucketAndPrefix(location);
    const keys: Array<ObjectKey | undefined> = [];
    let token: NextToken | undefined = undefined;
    do {
      const p: ListObjectsV2Request = token ? { ...params, ContinuationToken: token } : params;
      const { Contents, NextContinuationToken } = await this.s3.listObjectsV2(p).promise();
      if (!Contents) throw new Error(`Invalid Contents for location: s3://${params.Bucket}/${params.Prefix}`);
      keys.push(...Contents.map(k => k.Key));
      token = NextContinuationToken;
    } while (token);
    this.cachedKeys.set(location, <string[]>(keys.filter(k => !!k) ?? []));
    return this.cachedKeys.get(location) ?? [];
  }

  public getBucketAndPrefix(location: string): { Bucket: string; Prefix: string } {
    const vals = location?.split("//").slice(1).join("//").split("/");
    const Bucket = vals?.shift();
    const Prefix = vals?.join("/");
    if (!Bucket || !Prefix) throw new Error(`Invalid S3 path: ${location}`);
    return { Bucket, Prefix };
  }
}
