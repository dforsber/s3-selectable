const { S3 } = require("@aws-sdk/client-s3");
const { Glue } = require("@aws-sdk/client-glue");
const { S3Selectable } = require("@dforsber/s3-selectable");

const region = { region: "eu-west-1" };

function writeDataOut(chunk, mapper = obj => JSON.stringify(obj)) {
  Buffer.from(chunk)
    .toString()
    .split(/(?=\{)/gm)
    .map(s => JSON.parse(s))
    .map(cols => console.log(mapper(cols)));
}

async function main() {
  // NOTE: Instantiation of the class will start querying AWS Glue and S3 to
  //       fetch all S3 Object Keys that corresponds with the Glue Table data.
  const selectable = new S3Selectable({
    s3: new S3(region),
    glue: new Glue(region),
    databaseName: "default",
    tableName: "partitioned_elb_logs",
  });

  const selectParams = {
    // Bucket: "",                        // optional and not used
    // Key: "",                           // optional and not used
    // ExpressionType: "SQL",             // defaults to SQL
    // InputSerialization: { CSV: {},     // some rudimentary autodetection
    //   CompressionType: "GZIP" },       //  from Glue Table metadata
    // OutputSerialization: { JSON: {} }, // defaults to JSON
    Expression: "SELECT _1, _2 FROM s3Object LIMIT 42",
  };

  // explain select
  console.log(await selectable.explainSelect({ selectParams }));

  // NOTE: Returns Promise that resolves to the stream handle
  //return selectable.select(selectParams, onData, onEnd);

  // NOTE: Returns Promise that resolves only when stream ends
  return new Promise(resolve =>
    selectable.select({
      selectParams,
      onDataHandler: writeDataOut,
      onEndHandler: resolve,
    }),
  );
}

(async () => {
  console.log("Running example");
  await main();
  console.log("Example finished");
})().catch(e => {
  console.log(e);
});
