import { S3Selectable, s3selectableNonClass } from "@dforsber/s3-selectable";

import { Glue } from "@aws-sdk/client-glue";
import { IS3selectableNonClass } from "@dforsber/s3-selectable/dist/cjs/s3-selectable/s3-selectable";
import { S3 } from "@aws-sdk/client-s3";

function getCommonParams(sql = ""): IS3selectableNonClass {
  const region = { region: process.env.AWS_REGION ?? "eu-west-1" };
  return { sql, s3: new S3(region), glue: new Glue(region) };
}

function writeDataOut(chunk: Uint8Array): void {
  const dataObj = JSON.parse(Buffer.from(chunk).toString());
  console.log(`${dataObj._1}${dataObj._2}`);
}

async function classBasedExample(): Promise<void> {
  const selectable = new S3Selectable({
    ...getCommonParams(),
    databaseName: process.env.DATABASE_NAME ?? "default",
    tableName: process.env.TABLE_NAME ?? "partitioned_elb_logs",
  });

  // Returns only when the stream ends
  await new Promise<void>(resolve =>
    selectable.selectObjectContent({ Expression: "SELECT * FROM S3Object LIMIT 1" }, writeDataOut, resolve),
  );
}

async function nonClassBasedExample(): Promise<void> {
  // NOTE: Gathers the whole stream into memory and then dumps it out
  const sql = "SELECT * FROM default.partitioned_elb_logs LIMIT 1";
  const data = await s3selectableNonClass(getCommonParams(sql));
  data.map(d => writeDataOut(d));
}

console.log("Class based example: START");
classBasedExample()
  .then(() => console.log("Class based example: DONE"))
  .then(() => console.log("Non-class example: START"))
  .then(() => nonClassBasedExample())
  .then(() => console.log("Non-class example DONE"))
  .catch(err => console.log(err))
  .finally(() => console.log("DONE"));
