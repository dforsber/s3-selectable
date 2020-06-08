import * as AWSMock from "aws-sdk-mock";
import { errors } from "../common/errors.enum";
import { ListObjectsV2Request } from "aws-sdk/clients/s3";
import { S3LocationToKeys } from "./s3locationToS3Keys.mapper";
import { testTableKeys, testTableKeysNoPartitions } from "../common/fixtures/glue-table";
/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable max-len */
/* eslint-disable @typescript-eslint/no-var-requires */
/* eslint-disable @typescript-eslint/no-require-imports */
const AWS = require("aws-sdk");

AWSMock.setSDKInstance(AWS);
AWSMock.mock("S3", "listObjectsV2", (params: ListObjectsV2Request, cb: Function) => {
  const pref = params.Prefix ?? "";
  if (params.Bucket === "dummy-test-bucket2") {
    return cb(null, { NextContinuationToken: undefined, Contents: testTableKeysNoPartitions.map(k => ({ Key: k })) });
  }
  return cb(null, {
    NextContinuationToken: undefined,
    Contents: testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k })),
  });
});
const s3 = new AWS.S3({ region: "eu-west-1" });

describe("Parameter and return value checks", () => {
  it("throws when S3 is not provided", async () => {
    const key = "s3://testbucket-temp/v2/testing/multiple/separators";
    await expect(async () => await new S3LocationToKeys(key).getKeys()).rejects.toThrowError(errors.noS3);
  });
});

describe("getKeys method", () => {
  it("throws when S3 is not provided", async () => {
    const key = "s3://dummy-test-bucket2/Unsaved/2020/05/25/tables/63e1dd93-76d5-497f-8db7-bab5861fe14e";
    const keys = await new S3LocationToKeys(key, s3).getKeys();
    expect(keys.length).toEqual(10);
  });
});

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
