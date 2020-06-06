export function getTableAndDb(sql: string): [string, string] {
  const fromTbl = sql
    .split(/ from /i)[1]
    ?.split(" ")
    ?.shift()
    ?.split(".");
  if (!fromTbl || fromTbl.length !== 2 || !fromTbl[0] || !fromTbl[1]) {
    throw new Error(`Can't find <db>.<table> from SQL: ${sql}`);
  }
  return [fromTbl[0], fromTbl[1]];
}
