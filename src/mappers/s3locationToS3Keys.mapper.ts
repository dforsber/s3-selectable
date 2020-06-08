import { errors } from "../common/errors.enum";
import { ListObjectsV2Request, NextToken, ObjectKey } from "aws-sdk/clients/s3";

export class S3LocationToKeys {
  constructor(private location: string | undefined, private s3: AWS.S3 | undefined = undefined) {}

  public getBucketAndPrefix(): { Bucket: string; Prefix: string } {
    const vals = this.location?.split("//").slice(1).join("//").split("/");
    const Bucket = vals?.shift();
    const Prefix = vals?.join("/");
    if (!Bucket || !Prefix) throw new Error(`Invalid S3 path: ${this.location}`);
    return { Bucket, Prefix };
  }

  public async getKeys(): Promise<string[]> {
    if (!this.s3) throw new Error(errors.noS3);
    const { Bucket, Prefix } = this.getBucketAndPrefix();
    const params = { Bucket, Prefix };
    const keys: Array<ObjectKey | undefined> = [];
    let token: NextToken | undefined = undefined;
    do {
      const p: ListObjectsV2Request = token ? { ...params, ContinuationToken: token } : params;
      const { Contents, NextContinuationToken } = await this.s3.listObjectsV2(p).promise();
      if (!Contents) throw new Error(`Invalid Contents for location: ${this.location}`);
      keys.push(...Contents.map(k => k.Key));
      token = NextContinuationToken;
    } while (token);
    return <string[]>keys.filter(k => !!k);
  }
}
