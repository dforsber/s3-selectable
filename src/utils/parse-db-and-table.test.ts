import { getTableAndDb } from "./parse-db-and-table";

describe("Test getting db and table from SQL clause", () => {
  it("should work with correct select", () => {
    expect(getTableAndDb("SELECT * FROM db.table LIMIT 10")).toEqual(["db", "table"]);
  });
  it("should throw on a simple incorrect from db. table", () => {
    expect(() => getTableAndDb("SELECT * FROM db. table LIMIT 10")).toThrowError();
  });
  it("should throw on a simpl incorrect from db .table", () => {
    expect(() => getTableAndDb("SELECT * FROM db .table LIMIT 10")).toThrowError();
  });
});
