import { PartitionPreFilter, getPartitionKeyValue } from "./partition-filterer";

const partCols = ["year", "month"];

const partitions = [
  "year=2019/month=1",
  "year=2019/month=2",
  "year=2019/month=3",
  "year=2019/month=4",
  "year=2019/month=5",
  "year=2020/month=1",
  "year=2020/month=2",
  "year=2020/month=3",
  "year=2020/month=4",
  "year=2020/month=5",
];

const partitions2 = [
  "/year=2019/month=1",
  "/year=2019/month=2",
  "/year=2019/month=3",
  "/year=2019/month=4",
  "/year=2019/month=5",
  "/year=2020/month=1",
  "/year=2020/month=2",
  "/year=2020/month=3",
  "/year=2020/month=4",
  "/year=2020/month=5",
];

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
  it(`correctly throws with non-existing column parameter`, async () => {
    expect(() => getPartitionKeyValue("year=2020/month=12", "nonexisting")).toThrowError();
  });
  it(`correctly filters "year=2020 AND month>=4 AND month<=5" with partition(s)`, async () => {
    const sql = "WHERE year=2020 AND month>=4 AND month<=5";
    const parts = await new PartitionPreFilter(partitions, partCols).filterPartitions(sql);
    expect(parts.sort()).toEqual(["year=2020/month=4", "year=2020/month=5"]);
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
