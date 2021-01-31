import { PartitionPreFilter, getPartitionKeyValue, sqliteAll, sqliteRun } from "./partition-filterer";

import { assert } from "console";

const partCols = ["year", "month", "day"];

const partitions = [
  "year=2019/month=1/day=1",
  "year=2019/month=2/day=15",
  "year=2019/month=3/day=1",
  "year=2019/month=4/day=15",
  "year=2019/month=5/day=1",
  "year=2020/month=1/day=15",
  "year=2020/month=2/day=1",
  "year=2020/month=3/day=15",
  "year=2020/month=4/day=1",
  "year=2020/month=5/day=15",
];

const partitions2 = [
  "/year=2019/month=1/day=1",
  "/year=2019/month=2/day=15",
  "/year=2019/month=3/day=1",
  "/year=2019/month=4/day=15",
  "/year=2019/month=5/day=1",
  "/year=2020/month=1/day=15",
  "/year=2020/month=2/day=1",
  "/year=2020/month=3/day=15",
  "/year=2020/month=4/day=1",
  "/year=2020/month=5/day=15",
];

describe("throws error on SQLite failures", () => {
  it("throws error if db.run gets SQLite error", async () => {
    const partFilter = new PartitionPreFilter(partitions, partCols);
    const db = await partFilter.getDb();
    assert(db !== undefined);
    await sqliteRun(db, "DROP TABLE partitions");
    expect(() => sqliteRun(db, "SELECT * FROM partitions")).rejects.toThrow();
    expect(() => sqliteRun(undefined, "SELECT * FROM partitions")).rejects.toThrow();
  });
  it("throws error if db.all gets SQLite error", async () => {
    const partFilter = new PartitionPreFilter(partitions, partCols);
    const db = await partFilter.getDb();
    assert(db !== undefined);
    await sqliteRun(db, "DROP TABLE partitions");
    expect(() => sqliteAll(db, "DROP TABLE partitions")).rejects.toThrow();
    expect(() => sqliteAll(undefined, "DROP TABLE partitions")).rejects.toThrow();
  });
});

describe("When array of partitions are given", () => {
  it(`correctly populates a database table with partition(s)`, async () => {
    expect((await new PartitionPreFilter(partitions, partCols).filterPartitions("WHERE true")).sort()).toEqual(
      partitions.sort(),
    );
  });
  it(`correctly filters year=2019 with partition(s)`, async () => {
    expect((await new PartitionPreFilter(partitions2, partCols).filterPartitions("WHERE year=2019")).sort()).toEqual(
      partitions2.filter(p => p.includes("2019")).sort(),
    );
  });
  it(`correctly filters "year"=2019 with partition(s)`, async () => {
    expect((await new PartitionPreFilter(partitions2, partCols).filterPartitions('WHERE "year"=2019')).sort()).toEqual(
      partitions2.filter(p => p.includes("2019")).sort(),
    );
  });
  it(`correctly filters year=2019 AND day=15 AND monht=2 with partition(s)`, async () => {
    expect(
      (
        await new PartitionPreFilter(partitions2, partCols).filterPartitions("WHERE year=2019 AND month=2 AND day=15")
      ).sort(),
    ).toEqual(partitions2.filter(p => p.includes("2019") && p.includes("month=2") && p.includes("day=15")).sort());
  });
  it(`correctly filters year=2019 AND month=1 AND day=15 with partition(s)`, async () => {
    expect(
      (
        await new PartitionPreFilter(partitions2, partCols).filterPartitions("WHERE year=2019 AND month=2 AND day=15")
      ).sort(),
    ).toEqual(partitions2.filter(p => p.includes("2019") && p.includes("day=15") && p.includes("month=2")).sort());
  });
  it(`correctly filters "year"=2019 AND "day"=1 AND month=1 with partition(s)`, async () => {
    expect(
      (
        await new PartitionPreFilter(partitions2, partCols).filterPartitions("WHERE year=2019 AND day=15 AND month=1")
      ).sort(),
    ).toEqual(partitions2.filter(p => p.includes("year=2019") && p.includes("month=1") && p.includes("day=15")).sort());
  });
  it(`correctly throws with non-existing column parameter`, async () => {
    expect(() => getPartitionKeyValue("year=2020/month=12", "nonexisting")).toThrowError();
  });
  it(`correctly filters "year=2020 AND month>=4 AND month<=5" with partition(s)`, async () => {
    const sql = "WHERE year=2020 AND month>=4 AND month<=5";
    const parts = await new PartitionPreFilter(partitions, partCols).filterPartitions(sql);
    expect(parts.sort()).toEqual(["year=2020/month=4/day=1", "year=2020/month=5/day=15"]);
  });
  it(`correctly returns partitions without filter`, async () => {
    const parts = await new PartitionPreFilter(partitions, partCols).filterPartitions("");
    expect(parts.sort()).toEqual(partitions);
  });
  it(`works even without any partitions`, async () => {
    const sql = "WHERE year=2020 AND month>=4 AND month<=5";
    const parts = await new PartitionPreFilter([], []).filterPartitions(sql);
    expect(parts.sort()).toEqual([]);
  });
});
