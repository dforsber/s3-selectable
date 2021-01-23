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

const nodeSqlParserOpts = {
  database: "Mysql",
};

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

export function getPlainSQLAndExpr(sql: string): [string, string] {
  const regex = /FROM (\w+)(\.*)(\w*)(\S*)\s*(.*)$/im;
  const matches = sql.match(regex);
  const expr = matches && matches.length >= 5 ? matches[4] : "";
  //const rest = matches && matches.length >= 6 ? matches[5] : "";
  if (expr.trim() === ";") throw new Error("Multiple queries not supported (;)");
  if (expr.trim()[0] === ".") throw new Error("Can not use format FROM a.b.c");
  const plainSql = sql.replace(regex, `FROM $1$2$3 $5`).trim();
  return [plainSql, expr];
}

export function getTableAndDbAndExpr(sql: string): [string, string, string] {
  const [plainSql, expr] = getPlainSQLAndExpr(sql);
  const parser = new Parser();
  const ast = parser.astify(plainSql, nodeSqlParserOpts);
  const [db, table] = getTableAndDbFromAST(ast);
  if (!db || !table) throw new Error("Both db and table needed");
  return [db, table, expr];
}

export function getSQLWhereAST(sql: string): AST {
  const [plainSql] = getPlainSQLAndExpr(sql);
  const parser = new Parser();
  const ast = parser.astify(plainSql, nodeSqlParserOpts);
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
    .sqlify(
      {
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
      },
      nodeSqlParserOpts,
    )
    .substring(25);
}

export function getSQLLimit(sql: string): number {
  const [plainSql] = getPlainSQLAndExpr(sql);
  const found = plainSql.match(/LIMIT\s+(\d+)/im);
  if (found) return parseInt(found.pop() ?? "0");
  return 0;
}

export function setSQLLimit(sql: string, limit: number): string {
  return getPlainSQLAndExpr(sql)[0].replace(/LIMIT\s+(\d+)/im, () => `LIMIT ${limit}`);
}

export function getSQLWhereString(expression: string, partitionColumns: string[]): string {
  return getSQLWhereStringFromAST(makePartitionSpecificAST(getSQLWhereAST(expression), partitionColumns));
}
