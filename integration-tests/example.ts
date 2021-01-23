import { IS3selectableNonClass, S3Selectable, s3selectableNonClass } from "@dforsber/s3-selectable";

import { Glue } from "@aws-sdk/client-glue";
import { S3 } from "@aws-sdk/client-s3";

function getCommonParams(sql = ""): IS3selectableNonClass {
  const region = { region: process.env.AWS_REGION ?? "eu-west-1" };
  return { sql, s3: new S3(region), glue: new Glue(region), loglevel: "debug" };
}

function writeDataOut(chunk: Uint8Array, mapper: (obj: any) => string = obj => JSON.stringify(obj)): void {
  Buffer.from(chunk)
    .toString()
    .split(/(?=\{)/gm)
    .map(s => JSON.parse(s))
    .map(cols => console.log(mapper(cols)));
}

async function classBasedExample(): Promise<void> {
  const selectable = new S3Selectable({
    ...getCommonParams(),
    databaseName: process.env.DATABASE_NAME ?? "default",
    tableName: process.env.TABLE_NAME ?? "partitioned_elb_logs",
    logLevel: "debug",
  });

  // Returns only when the stream ends
  await new Promise<void>(resolve =>
    selectable.selectObjectContent({
      selectParams: { Expression: "SELECT _1, _2 FROM S3Object LIMIT 42" },
      onEventHandler: event => (!event.Records ? console.log(event) : undefined),
      onDataHandler: writeDataOut,
      onEndHandler: resolve,
    }),
  );
}

async function nonClassBasedExample(): Promise<void> {
  // NOTE: Gathers the whole stream into memory and then dumps it out
  const sql = "SELECT _1, _2 FROM default.partitioned_elb_logs LIMIT 42";
  const data = await s3selectableNonClass(getCommonParams(sql));
  const concatTwoCols = obj => obj._1.concat(obj._2);
  data.map(d => writeDataOut(d, concatTwoCols));
}

console.log("Class based example: START");
classBasedExample()
  .then(() => console.log("Class based example: DONE"))
  .then(() => console.log("Non-class example: START"))
  .then(() => nonClassBasedExample())
  .then(() => console.log("Non-class example DONE"))
  .catch(err => console.log(err))
  .finally(() => console.log("DONE"));
