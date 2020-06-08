import { AST, From, Parser, Select } from "node-sql-parser";

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

export function getTableAndDbFromAST(ast: AST | AST[]): [string | null, string] {
  if (Array.isArray(ast)) throw new Error("Multiple queries not supported");
  if (ast.type !== "select") throw new Error("Only SELECT queries are supported");
  if (!ast.from) throw new Error("Only SELECT queries with FROM are supported");
  if (ast.from.length !== 1) throw new Error("Only single table sources supported for now");
  const from = ast.from[0];
  if (Object.prototype.hasOwnProperty.call(from, "type")) throw new Error("DUAL not supported");
  const { db, table } = <From>from;
  return [db, table];
}

export function getTableAndDb(sql: string): [string, string] {
  const parser = new Parser();
  const ast = parser.astify(sql);
  const [db, table] = getTableAndDbFromAST(ast);
  if (!db || !table) throw new Error("Both db and table needed");
  return [db, table];
}

export function getSQLWhereAST(sql: string): AST {
  const parser = new Parser();
  const ast = parser.astify(sql);
  getTableAndDbFromAST(ast);
  return (<Select>ast).where;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function filterByPartsOnly(ast: any, partCols: string[]): any {
  const nonFiltering = { type: "bool", value: true };
  if (ast.type === "column_ref") return ast;
  if (ast.left?.type === "column_ref" && !partCols.some(c => c === ast.left?.column)) return nonFiltering;
  if (ast.right?.type === "column_ref" && !partCols.some(c => c === ast.right?.column)) return nonFiltering;
  if (!ast.left || !ast.right) return ast;
  return { ...ast, left: filterByPartsOnly(ast.left, partCols), right: filterByPartsOnly(ast.right, partCols) };
}

export function makePartitionSpecificAST(ast: AST, partitionColumns: string[]): AST {
  if (!ast) return ast;
  return filterByPartsOnly(ast, partitionColumns);
}

export function getSQLWhereStringFromAST(where: AST): string {
  const parser = new Parser();
  return parser
    .sqlify({
      where,
      with: null,
      type: "select",
      options: null,
      distinct: null,
      columns: "*",
      from: [{ db: null, table: "s3Object", as: null }],
      groupby: null,
      having: null,
      orderby: null,
      limit: null,
    })
    .substring(25);
}

export function getSQLWhereString(expression: string, partitionColumns: string[]): string {
  return getSQLWhereStringFromAST(makePartitionSpecificAST(getSQLWhereAST(expression), partitionColumns));
}
