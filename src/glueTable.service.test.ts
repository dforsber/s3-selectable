import { GlueTableS3Keys } from "./glueTable.service";

describe("Test getBucketAndPrefix utility method", () => {
  test("Correct location with multiple delimiters", () => {
    const [Bucket, Prefix] = GlueTableS3Keys.getBucketAndPrefix("s3://testbucket-temp/v2/testing/multiple/separators");
    expect(Bucket).toEqual("testbucket-temp");
    expect(Prefix).toEqual("v2/testing/multiple/separators");
  });
});
