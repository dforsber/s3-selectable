import { ListObjectsV2CommandInput, S3 } from "@aws-sdk/client-s3";
import { testTableKeys, testTableKeysNoPartitions } from "../common/fixtures/glue-table";

import { S3KeysCache } from "./s3KeysCache";
import { errors } from "../common/errors.enum";

let s3ListObjectsV2Called = 0;

jest.mock("@aws-sdk/client-s3", () => ({
  S3: function S3() {
    return {
      listObjectsV2: jest.fn((params: ListObjectsV2CommandInput) => {
        s3ListObjectsV2Called++;
        const pref = params.Prefix ?? "";
        if (params.Bucket === "dummy-test-bucket2") {
          return Promise.resolve({
            NextContinuationToken: undefined,
            Contents: testTableKeysNoPartitions.map(k => ({ Key: k })),
          });
        }
        if (params.Bucket === "dummy-test-bucket3") {
          return Promise.resolve({
            NextContinuationToken: undefined,
            Contents: undefined,
          });
        }
        const keys = testTableKeys.filter(k => k.includes(pref)).map(k => ({ Key: k }));
        const first = keys.slice(0, keys.length / 2);
        const second = keys.slice(keys.length / 2);
        if (params.ContinuationToken === "token") {
          return Promise.resolve({
            NextContinuationToken: undefined,
            Contents: second,
          });
        }
        return Promise.resolve({
          NextContinuationToken: second.length ? "token" : undefined,
          Contents: first,
        });
      }),
    };
  },
}));

const s3 = new S3({ region: "eu-west-1" });

describe("Parameter and return value checks", () => {
  beforeEach(() => {
    s3ListObjectsV2Called = 0;
  });

  it("throws when S3 is not provided", async () => {
    const key = "s3://testbucket-temp/v2/testing/multiple/separators";
    await expect(async () => await new S3KeysCache().getKeys(key)).rejects.toThrowError(errors.noS3);
  });
  it("Uses continuation token", async () => {
    const key = "s3://testbucket-temp/Unsaved/2020/05/25/";
    const s3loc = new S3KeysCache(s3);
    const keys = await s3loc.getKeys(key);
    await expect(keys.sort()).toEqual(testTableKeys.sort());
    expect(s3ListObjectsV2Called).toEqual(2); // using continuation token, 2 calls
  });
  it("provides cached results consistently", async () => {
    const key = "s3://dummy-test-bucket2/Unsaved/2020/05/25/";
    const s3loc = new S3KeysCache(s3);
    await expect(s3loc.getKeys(key)).resolves.toEqual(testTableKeysNoPartitions);
    expect(s3ListObjectsV2Called).toEqual(1);
    await expect(s3loc.getKeys(key)).resolves.toEqual(testTableKeysNoPartitions);
    expect(s3ListObjectsV2Called).toEqual(1);
  });
  it("provides empty list from cache if no keys", async () => {
    const key = "s3://dummy-test-bucket/NonExisting/";
    const s3loc = new S3KeysCache(s3);
    await expect(s3loc.getKeys(key)).resolves.toEqual([]);
    expect(s3ListObjectsV2Called).toEqual(1);
    await expect(s3loc.getKeys(key)).resolves.toEqual([]);
    expect(s3ListObjectsV2Called).toEqual(1);
  });
});

describe("getBucketAndPrefix utility method", () => {
  it("gives correct location with multiple delimiters", () => {
    const key = "s3://testbucket-temp/v2/testing/multiple/separators";
    const { Bucket, Prefix } = new S3KeysCache(s3).getBucketAndPrefix(key);
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing/multiple/separators");
  });

  it("gives correct location with double // ”empty” folder", () => {
    const key = "s3://testbucket-temp/v2/testing//multiple/separators";
    const { Bucket, Prefix } = new S3KeysCache(s3).getBucketAndPrefix(key);
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing//multiple/separators");
  });

  it("gives correct location with double /// ”empty” folders", () => {
    const key = "s3://testbucket-temp/v2/testing//multiple/separators///and_more";
    const { Bucket, Prefix } = new S3KeysCache(s3).getBucketAndPrefix(key);
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing//multiple/separators///and_more");
  });

  it("throws an error if Bucket and/or Prefix are not parseable", () => {
    const key = "s3:///v2/testing//multiple/separators///and_more";
    expect(() => new S3KeysCache(s3).getBucketAndPrefix(key)).toThrowError();
  });

  it("Gives Bucket and empty prefix if no path", () => {
    const { Bucket, Prefix } = new S3KeysCache(s3).getBucketAndPrefix("s3://testbucket-temp/");
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("");
  });

  it("Gives Bucket and empty prefix if no path and no ending /", () => {
    const { Bucket, Prefix } = new S3KeysCache(s3).getBucketAndPrefix("s3://testbucket-temp");
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("");
  });
});

describe("AWS s3 sdk returns no Contents", () => {
  beforeEach(() => {
    s3ListObjectsV2Called = 0;
  });

  it("Works when AWS SDK returns Contents undefined (sigh)", async () => {
    const key = "s3://dummy-test-bucket3/Unsaved/2020/05/25/";
    const s3loc = new S3KeysCache(new S3({ region: "eu-west-1" }));
    const keys = await s3loc.getKeys(key);
    await expect(keys.sort()).toEqual([]);
    expect(s3ListObjectsV2Called).toEqual(1); // using continuation token, 2 calls
  });
});
