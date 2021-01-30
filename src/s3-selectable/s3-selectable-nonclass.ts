import { InputSerialization, OutputSerialization, S3 } from "@aws-sdk/client-s3";

import { Glue } from "@aws-sdk/client-glue";
import { S3Selectable } from "./s3-selectable";
import { getTableAndDbAndExpr } from "../utils/sql-query.helper";

export interface IS3selectableNonClass {
  sql: string;
  s3: S3;
  glue: Glue;
  InputSerialization?: InputSerialization;
  OutputSerialization?: OutputSerialization;
  loglevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
}

/*
 * Alternative, non-class based interface that does not do any caching of Table data. We recommend
 * to use the class based interface for latency aware use cases where multiple S3 Select commands
 * are run over the same table and in cases where the class can be instantiated beforehand to fill
 * up the cache.
 */
export async function S3SelectableNonClass(params: IS3selectableNonClass): Promise<Uint8Array[]> {
  const { s3, glue } = params;
  const [databaseName, tableName, expr] = getTableAndDbAndExpr(params.sql);
  const Expression = params.sql.replace(`${databaseName}.${tableName}`, `s3Object${expr}`);
  const selectable = new S3Selectable({ databaseName, tableName, s3, glue });
  const data: Uint8Array[] = await new Promise(resolve => {
    const chunks: Uint8Array[] = [];
    selectable.select({
      selectParams: { ...params, Expression },
      onDataHandler: chunk => chunks.push(chunk),
      onEndHandler: () => resolve(chunks),
    });
  });
  return data;
}
