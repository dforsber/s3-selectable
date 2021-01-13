import { IS3Selectable, S3Selectable } from "@dforsber/s3-selectable";
import { S3, SelectObjectContentEventStream } from "@aws-sdk/client-s3";

import { Glue } from "@aws-sdk/client-glue";

function writeDataOut(chunk: SelectObjectContentEventStream): void {
  if (chunk.Records?.Payload) process.stdout.write(Buffer.from(chunk.Records?.Payload).toString());
}

async function classBasedExample(): Promise<void> {
  console.log("Running with class interface");
  const region = { region: process.env.AWS_REGION ?? "eu-west-1" };
  const tableParams: IS3Selectable = {
    s3: new S3(region),
    glue: new Glue(region),
    databaseName: process.env.DATABASE_NAME ?? "default",
    tableName: process.env.TABLE_NAME ?? "partitioned_elb_logs",
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

// async function nonClassBasedExample(): Promise<void> {
//   console.log("Running with non-class interface");
//   const data = await s3selectable("SELECT * FROM default.partitioned_elb_logs LIMIT 1");
//   console.log(data);
// }

classBasedExample().catch(err => console.log(err));
// nonClassBasedExample().catch(err => console.log(err));
