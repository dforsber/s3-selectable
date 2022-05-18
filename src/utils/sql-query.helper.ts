import { astMapper, FromTable, parse, parseFirst, SelectFromStatement, Statement, toSql } from "pgsql-ast-parser";

/*
 * ## How to filter S3 Keys based on table partition keys that map to "folders" on S3
 *
 * 1. Create SQLite table from all S3 Keys with all partition columns as columns. Populate all S3 Keys
 * into the table and parse the partition values into the columns.
 *
 * 2. For filtering the S3 Keys based on Hive Partitioning folders, turn all clauses that do not filter based on
 * partition keys to true so that they wount restrict on selecting the S3 Keys.
 *
 * 3. Run the SQL query with the modified WHERE clause on SQLite table to get the list of S3 Keys that need
 * to be applied. Then run the full query on S3 Select over the S3 Objects.
 */

export function getTableAndDbFromAST(ast: Statement): [string | undefined, string] {
  if (ast.type !== "select") throw new Error("Only SELECT queries are supported");
  if (!ast.from) throw new Error("Only SELECT queries with FROM are supported");
  if (ast.from.length !== 1) throw new Error("Only single table sources supported for now");
  const from = ast.from[0];
  if (from.type !== "table") throw new Error("Only FROM table supported");
  const { schema, name: table } = <FromTable>from;
  return [schema, table];
}

export function getPlainSQLAndExpr(sql: string): [string, string] {
  const regex = /FROM (\w+)(\.*)(\w*)(\S*)\s*(.*)$/im;
  const matches = sql.match(regex);
  const expr = matches && matches.length >= 5 ? matches[4] : "";
  if (expr.trim() === ";") throw new Error("Multiple queries not supported (;)");
  if (expr.trim()[0] === ".") throw new Error("Can not use format FROM a.b.c");
  const plainSql = sql.replace(regex, `FROM $1$2$3 $5`).trim();
  return [plainSql, expr];
}

export function getTableAndDbAndExpr(sql: string): [string, string, string] {
  const [plainSql, expr] = getPlainSQLAndExpr(sql);
  const ast = parse(plainSql);
  if (ast.length !== 1) throw new Error("Multiple queries not supported");
  const [db, table] = getTableAndDbFromAST(ast[0]);
  if (!db || !table) throw new Error("Both db and table needed");
  return [db, table, expr];
}

function partFilter(partCols: string[], column: string): boolean {
  return partCols.some(c => c === column);
}

function nonPartFilter(partCols: string[], column: string): boolean {
  return partCols.every(c => c !== column);
}

function filterParts(
  sql: string,
  partCols: string[],
  filter: (partCols: string[], column: string) => boolean,
): SelectFromStatement | null | undefined {
  return <SelectFromStatement>astMapper(_map => ({
    ref: c => (filter(partCols, c.name) ? null : c),
  })).statement(parseFirst(sql));
}

export function makePartitionSpecificAST(
  sql: string,
  partitionColumns: string[],
): SelectFromStatement | undefined | null {
  return filterParts(sql, partitionColumns, nonPartFilter);
}

export function makeSelectSpecificAST(sql: string, partitionColumns: string[]): SelectFromStatement | undefined | null {
  return filterParts(sql, partitionColumns, partFilter);
}

export function getSQLWhereStringFromAST(selStmt: SelectFromStatement | undefined | null): string {
  return toSql
    .statement({
      where: selStmt?.where,
      type: "select",
      from: [{ type: "table", name: "s3Object" }],
    })
    .substring(25);
}

function replaceWhereInSQL(sql: string, selStm: SelectFromStatement | undefined | null): string {
  if (!selStm) return sql;
  const [plainSql] = getPlainSQLAndExpr(sql);
  const ast = parseFirst(plainSql);
  return toSql.statement(<SelectFromStatement>{ ...ast, where: selStm?.where });
}

export function getSQLLimit(sql: string): number {
  const [plainSql] = getPlainSQLAndExpr(sql);
  const found = plainSql.match(/LIMIT\s+(\d+)/im);
  return found?.length ? parseInt(found[1]) : 0;
}

export function setSQLLimit(sql: string, limit: number): string {
  return getPlainSQLAndExpr(sql)[0].replace(/LIMIT\s+(\d+)/im, () => `LIMIT ${limit}`);
}

export function getPartsOnlySQLWhereString(expression: string, partitionColumns: string[]): string {
  return getSQLWhereStringFromAST(makePartitionSpecificAST(expression, partitionColumns));
}

export function getNonPartsSQL(expression: string, partitionColumns: string[]): string {
  return replaceWhereInSQL(expression, makeSelectSpecificAST(expression, partitionColumns));
}
