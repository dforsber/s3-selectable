import { S3LocationToKeys } from "./s3locationToS3Keys.mapper";

describe("getBucketAndPrefix utility method", () => {
  it("gives correct location with multiple delimiters", () => {
    const key = "s3://testbucket-temp/v2/testing/multiple/separators";
    const { Bucket, Prefix } = new S3LocationToKeys(key).getBucketAndPrefix();
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing/multiple/separators");
  });

  it("gives no location with multiple delimiters", () => {
    expect(() => new S3LocationToKeys(undefined).getBucketAndPrefix()).toThrowError();
  });

  it("gives correct location with double // ”empty” folder", () => {
    const key = "s3://testbucket-temp/v2/testing//multiple/separators";
    const { Bucket, Prefix } = new S3LocationToKeys(key).getBucketAndPrefix();
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing//multiple/separators");
  });

  it("gives correct location with double /// ”empty” folders", () => {
    const key = "s3://testbucket-temp/v2/testing//multiple/separators///and_more";
    const { Bucket, Prefix } = new S3LocationToKeys(key).getBucketAndPrefix();
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing//multiple/separators///and_more");
  });
});
