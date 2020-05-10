import { s3SelectOnTable, IS3SelectOnTable } from "./src/s3SelectOnTable";
import AWS from "aws-sdk";

async function main(): Promise<void> {
  const region = { region: process.env.AWS_REGION || "eu-west-1" };
  const tableParams: IS3SelectOnTable = {
    s3: new AWS.S3(region),
    glue: new AWS.Glue(region),
    tableName: process.env.TABLE_NAME || "elb_logs",
    databaseName: process.env.DATABASE_NAME || "sampledb",
  };
  const glueTable = new s3SelectOnTable(tableParams);
  const selectStream = await glueTable.selectObjectContent({
    ExpressionType: "SQL",
    InputSerialization: { CSV: {} },
    OutputSerialization: { JSON: {} },
    Expression: "SELECT * FROM S3Object LIMIT 2",
  });
  selectStream.on("data", chunk => {
    if (chunk.Records?.Payload) console.log(Buffer.from(chunk.Records?.Payload).toString());
  });
}

main().catch(err => console.log(err));
