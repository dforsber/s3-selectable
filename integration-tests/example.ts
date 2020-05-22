import AWS from "aws-sdk";
import { IEventStream, IS3Selectable, S3Selectable, s3selectable } from "../dist/";

function writeDataOut(chunk: IEventStream): void {
  if (chunk.Records?.Payload) process.stdout.write(Buffer.from(chunk.Records?.Payload).toString());
}

async function classBasedExample(): Promise<void> {
  console.log("Running with class interface");
  const region = { region: process.env.AWS_REGION ?? "eu-west-1" };
  const tableParams: IS3Selectable = {
    s3: new AWS.S3(region),
    glue: new AWS.Glue(region),
    tableName: process.env.TABLE_NAME ?? "partitioned_elb_logs",
    databaseName: process.env.DATABASE_NAME ?? "default",
  };
  const glueTable = new S3Selectable(tableParams);
  const selectStream = await glueTable.selectObjectContent({
    ExpressionType: "SQL",
    InputSerialization: { CSV: {}, CompressionType: "GZIP" },
    OutputSerialization: { JSON: {} },
    Expression: "SELECT * FROM S3Object LIMIT 1",
  });
  selectStream.on("data", writeDataOut);
}

async function nonClassBasedExample(): Promise<void> {
  console.log("Running with non-class interface");
  const selStream = await s3selectable("SELECT * FROM default.partitioned_elb_logs LIMIT 1");
  selStream.on("data", writeDataOut);
}

classBasedExample().catch(err => console.log(err));
nonClassBasedExample().catch(err => console.log(err));
