import { GlueTableToS3KeysMapper } from "./glueTable.mapper";

describe("Test getBucketAndPrefix utility method", () => {
  test("Correct location with multiple delimiters", () => {
    const key = "s3://testbucket-temp/v2/testing/multiple/separators";
    const [Bucket, Prefix] = GlueTableToS3KeysMapper.getBucketAndPrefix(key);
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing/multiple/separators");
  });
});
