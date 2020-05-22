import { Database } from "sqlite3";

// TODO: function getKeyPartitionValues(key: string): string[] {}

function getPartitionKeyValue(partition: string, partCol: string): string | undefined {
  const p = partition.split(`${partCol}=`)[1];
  if (!p) throw new Error(`Column "${partCol}" not found from partition "${partition}"`);
  return p.split("/").shift();
}

function sqliteRun(db: Database, sql: string, params: string[] = []): Promise<void> {
  return new Promise((resolve, reject) => {
    db.run(sql, params, err => (err ? reject(err) : resolve()));
  });
}

function sqliteAll(db: Database, sql: string): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => (err ? reject(err) : resolve(rows)));
  });
}

async function createTable(partCols: string[]): Promise<Database> {
  const db = new Database(":memory:");
  const numCols = partCols.length + 1;
  const cols = numCols > 1 ? `, ${partCols.map(c => `${c} STRING`).join(", ")}` : "";
  const sql = `CREATE TABLE partitions (partition STRING${cols})`;
  await sqliteRun(db, sql);
  return db;
}

function dropTable(db: Database): Promise<void> {
  return sqliteRun(db, "DROP TABLE IF EXISTS partitions");
}

async function insertPartitions(db: Database, parts: string[], partCols: string[]): Promise<void> {
  const numCols = partCols.length + 1;
  const sql = `INSERT INTO partitions VALUES (${Array(numCols).fill("?", 0, numCols).join(",")})`;
  await Promise.all(
    parts.map(part => {
      const params = [part, ...partCols.map(col => getPartitionKeyValue(part, col))].filter(k => !!k);
      return sqliteRun(db, sql, <string[]>params);
    }),
  );
  return;
}

export async function filterPartitions(
  parts: string[],
  partCols: string[],
  where: string | null | undefined,
): Promise<string[]> {
  if (!where) return parts;
  const db = await createTable(partCols);
  await insertPartitions(db, parts, partCols);
  const rows = <Array<{ partition: string }>>await sqliteAll(db, `SELECT partition FROM partitions ${where}`);
  await dropTable(db);
  return rows.map(row => row.partition);
}
