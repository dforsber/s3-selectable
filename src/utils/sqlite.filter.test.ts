import { filterPartitions } from "./sqlite.filter";

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
    expect((await filterPartitions(partitions, partCols, "WHERE true")).sort()).toEqual(partitions.sort());
  });

  it(`correctly filters year=2019 with partition(s)`, async () => {
    expect((await filterPartitions(partitions2, partCols, "WHERE year=2019")).sort()).toEqual(
      partitions2.filter(p => p.includes("2019")).sort(),
    );
  });

  it(`correctly filters "year=2020 AND month>=4 AND month<=5" with partition(s)`, async () => {
    expect((await filterPartitions(partitions, partCols, "WHERE year=2020 AND month>=4 AND month<=5")).sort()).toEqual([
      "year=2020/month=4",
      "year=2020/month=5",
    ]);
  });
});
