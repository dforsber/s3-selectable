const { S3 } = require("@aws-sdk/client-s3");
const { Glue } = require("@aws-sdk/client-glue");
const { S3Selectable } = require("@dforsber/s3-selectable");

const region = { region: "eu-west-1" };

async function main() {
  // NOTE: Instantiation of the class will start querying AWS Glue and S3 to
  //       fetch all S3 Object Keys that corresponds with the Glue Table data.
  const selectable = new S3Selectable({
    s3: new S3(region),
    glue: new Glue(region),
    databaseName: "default",
    tableName: "partitioned_elb_logs",
  });

  const onData = chunk => {
    const data = Buffer.from((chunk.Records || {}).Payload || "").toString();
    process.stdout.write(data);
  };

  const onEnd = () => console.log("Stream end");

  const selectParams = {
    // Bucket: "",                        // optional and not used
    // Key: "",                           // optional and not used
    // ExpressionType: "SQL",             // defaults to SQL
    // InputSerialization: { CSV: {},     // some rudimentary autodetection
    //   CompressionType: "GZIP" },       //  from Glue Table metadata
    // OutputSerialization: { JSON: {} }, // defaults to JSON
    Expression: "SELECT * FROM s3Object LIMIT 2",
  };
  await selectable.selectObjectContent(selectParams, onData, onEnd);
}

main().catch(err => console.log(err));
