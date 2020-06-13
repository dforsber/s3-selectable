import { Database } from "sqlite3";

export function getPartitionKeyValue(partition: string, partCol: string): string | undefined {
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

export async function createTable(partCols: string[], parts: string[]): Promise<Database | undefined> {
  if (!partCols.length || !parts.length) return;
  const db = new Database(":memory:");
  const cols = `, ${partCols.map(c => `${c} STRING`).join(", ")}`;
  const sql = `CREATE TABLE partitions (partition STRING${cols})`;
  await sqliteRun(db, sql);
  await insertPartitions(db, parts, partCols);
  return db;
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

export class PartitionPreFilter {
  private partsTable = createTable(this.partCols, this.parts);

  constructor(private parts: string[], private partCols: string[]) {}

  public async filterPartitions(where: string | null | undefined): Promise<string[]> {
    if (!where) return this.parts;
    const db = await this.partsTable;
    if (!db) return this.parts;
    const rows = <Array<{ partition: string }>>await sqliteAll(db, `SELECT partition FROM partitions ${where}`);
    return rows.map(row => row.partition);
  }
}
