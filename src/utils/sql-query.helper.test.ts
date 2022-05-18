import {
  getNonPartsSQL,
  getPartsOnlySQLWhereString,
  getPlainSQLAndExpr,
  getSQLLimit,
  getSQLWhereAST,
  getSQLWhereStringFromAST,
  getTableAndDbAndExpr,
  makePartitionSpecificAST,
  makeSelectSpecificAST,
  setSQLLimit,
} from "./sql-query.helper";

describe("it getting db and table from SQL clause", () => {
  it("should work with correct select", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t")).toEqual(["db", "t", ""]);
  });
  it("should throw with non SQL string", () => {
    expect(() => getTableAndDbAndExpr("SELECT TABLE")).toThrowError();
  });
  it("should throw with non SQL string", () => {
    expect(() => getTableAndDbAndExpr("SELECT (1,2)")).toThrowError("Only SELECT queries with FROM are supported");
  });
  it("should work with correct select", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t LIMIT 10")).toEqual(["db", "t", ""]);
  });
  it("no multiple queries", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM db.t; SELECT * FROM db2.t2")).toThrowError(
      "Multiple queries not supported",
    );
  });
  it("no multiple queries with space", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM db.t ; SELECT * FROM db2.t2")).toThrowError(
      "Multiple queries not supported",
    );
  });
  it("Single table FROM only", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM db.t AS t, db2.t2 as t2")).toThrowError(
      "Only single table sources supported for now",
    );
  });
  it("No FROM DUAL", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM DUAL")).toThrowError("DUAL not supported");
  });
  it("Both db and table must be given", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM t")).toThrowError("Both db and table needed");
  });
  it("should throw on a simple incorrect from db. t", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db. t LIMIT 10")).toEqual(["db", "t", ""]);
  });
  it("should throw on a simple incorrect from db .t", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db .t LIMIT 10")).toEqual(["db", "t", ""]);
  });
  it("should throw on a simple incorrect from catalog.db.t", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM catalog.db.t LIMIT 10")).toThrowError();
  });
  it("should throw on a missing table", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM")).toThrowError();
  });
  it("should throw on a missing table 2", () => {
    expect(() => getTableAndDbAndExpr("SELECT * FROM ")).toThrowError();
  });
  it("should throw when query is not SELECT", () => {
    expect(() => getTableAndDbAndExpr("DROP TABLE t")).toThrowError("Only SELECT queries are supported");
  });
});

describe("ensuring JSON table queries work too", () => {
  it("SELECT * FROM db.t", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t")).toEqual(["db", "t", ""]);
  });
  it("SELECT * FROM db.t[*]", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t[*]")).toEqual(["db", "t", "[*]"]);
  });
  it("SELECT * FROM db.t[*].path1", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t[*].path1")).toEqual(["db", "t", "[*].path1"]);
  });
  it("SELECT * FROM db.t[*].path1[*].id", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t[*].path1[*].id")).toEqual(["db", "t", "[*].path1[*].id"]);
  });
  it("SELECT * FROM db.t[*].path1[*].id AS d", () => {
    expect(getTableAndDbAndExpr("SELECT * FROM db.t[*].path1[*].id AS d")).toEqual(["db", "t", "[*].path1[*].id"]);
  });
  // get plain SQL and expression with s3Object "table"
  it("SELECT * FROM s3Object[*]", () => {
    expect(getPlainSQLAndExpr("SELECT * FROM s3Object[*]")).toEqual(["SELECT * FROM s3Object", "[*]"]);
  });
  it("SELECT * FROM s3Object[*].path1", () => {
    expect(getPlainSQLAndExpr("SELECT * FROM s3Object[*].path1")).toEqual(["SELECT * FROM s3Object", "[*].path1"]);
  });
  it("SELECT * FROM s3Object[*].path1[*].id", () => {
    expect(getPlainSQLAndExpr("SELECT * FROM s3Object[*].path1[*].id")).toEqual([
      "SELECT * FROM s3Object",
      "[*].path1[*].id",
    ]);
  });
  it("SELECT * FROM s3Object[*].path1[*].id AS d", () => {
    expect(getPlainSQLAndExpr("SELECT * FROM s3Object[*].path1[*].id AS d")).toEqual([
      "SELECT * FROM s3Object AS d",
      "[*].path1[*].id",
    ]);
  });
});

describe("S3 Select SQL clauses", () => {
  it("changes partition column filters to TRUE (partition filtering has already been done)", () => {
    const sql = "SELECT * FROM s3Object WHERE part=0";
    const expected = "SELECT * FROM s3Object WHERE TRUE";
    expect(getNonPartsSQL(sql, ["part"])).toEqual(expected);
  });
  it("does not change the query if not partition filters", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = "SELECT * FROM s3Object";
    expect(getNonPartsSQL(sql, ["part"])).toEqual(expected);
  });
  it("does not change the query if not partition filters", () => {
    const sql = "SELECT * FROM s3Object WHERE col=0";
    const expected = "SELECT * FROM s3Object WHERE col = 0";
    expect(getNonPartsSQL(sql, [])).toEqual(expected);
  });
});

describe("SQL WHERE clauses", () => {
  it("with single clause", () => {
    const sql = "SELECT * FROM s3Object WHERE part=0";
    const expected = {
      left: {
        column: "part",
        table: null,
        type: "column_ref",
      },
      operator: "=",
      right: {
        type: "number",
        value: 0,
      },
      type: "binary_expr",
    };
    expect(getSQLWhereAST(sql)).toEqual(expected);
  });

  it("without WHERE", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = null;
    expect(getSQLWhereAST(sql)).toEqual(expected);
  });

  it("without WHERE with partition specific WHERE mutation", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = undefined;
    expect(makePartitionSpecificAST(getSQLWhereAST(sql), [])).toEqual(expected);
  });

  // replaceWhereInSQL
  it("without WHERE with partition specific WHERE mutation", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = undefined;
    expect(makePartitionSpecificAST(getSQLWhereAST(sql), [])).toEqual(expected);
  });

  it("only SELECT queries", () => {
    const sql = "SHOW CATALOGS";
    expect(() => makePartitionSpecificAST(getSQLWhereAST(sql), [])).toThrowError();
  });

  it("only SELECT queries", () => {
    const sql = "UPDATE TABLE";
    expect(() => makePartitionSpecificAST(getSQLWhereAST(sql), [])).toThrowError();
  });

  it("set partition clauses true, no non-partition columns", () => {
    const sql = "SELECT * FROM s3Object WHERE year<=2020 AND 9<=month AND true";
    expect(getSQLWhereStringFromAST(makeSelectSpecificAST(getSQLWhereAST(sql), ["year", "month", "day"]))).toEqual(
      "WHERE TRUE AND TRUE AND TRUE",
    );
  });

  it("set partition clauses true, one non-partition column", () => {
    const sql = "SELECT * FROM s3Object WHERE year<=2020 AND 9<=month AND(foo=1 OR 2=bar) AND true";
    expect(getSQLWhereStringFromAST(makeSelectSpecificAST(getSQLWhereAST(sql), ["year", "month", "day"]))).toEqual(
      "WHERE TRUE AND TRUE AND (`foo` = 1 OR 2 = `bar`) AND TRUE",
    );
  });

  it("set non-partition clauses true, one partition column", () => {
    const sql = "SELECT * FROM s3Object WHERE year<=2020 AND 9<=month AND(foo=1 OR 2=bar) AND true";
    expect(getSQLWhereStringFromAST(makePartitionSpecificAST(getSQLWhereAST(sql), ["year", "month", "day"]))).toEqual(
      "WHERE `year` <= 2020 AND 9 <= `month` AND (TRUE OR TRUE) AND TRUE",
    );
  });

  it("set non-partition clauses true, no partition columns", () => {
    const sql = "SELECT * FROM s3Object WHERE (foo=1 OR bar=2) AND true";
    expect(getSQLWhereStringFromAST(makePartitionSpecificAST(getSQLWhereAST(sql), ["year", "month", "day"]))).toEqual(
      "WHERE (TRUE OR TRUE) AND TRUE",
    );
  });

  it("with two clauses", () => {
    const sql = "SELECT * FROM s3Object WHERE year<=2020 AND month>=2";
    const expected = {
      left: {
        left: {
          column: "year",
          table: null,
          type: "column_ref",
        },
        operator: "<=",
        right: {
          type: "number",
          value: 2020,
        },
        type: "binary_expr",
      },
      operator: "AND",
      right: {
        left: {
          column: "month",
          table: null,
          type: "column_ref",
        },
        operator: ">=",
        right: {
          type: "number",
          value: 2,
        },
        type: "binary_expr",
      },
      type: "binary_expr",
    };
    expect(getSQLWhereAST(sql)).toEqual(expected);
    expect(getPartsOnlySQLWhereString(sql, ["year", "month", "day"])).toEqual("WHERE `year` <= 2020 AND `month` >= 2");
  });

  it("with subclauses", () => {
    const sql =
      "SELECT * FROM s3Object WHERE " +
      "(year<=2020 AND month>=2 AND title='hello') OR " +
      "(year>2020 AND month<10) AND true";
    const expected = {
      left: {
        left: {
          left: {
            left: {
              left: {
                column: "year",
                table: null,
                type: "column_ref",
              },
              operator: "<=",
              right: {
                type: "number",
                value: 2020,
              },
              type: "binary_expr",
            },
            operator: "AND",
            right: {
              left: {
                column: "month",
                table: null,
                type: "column_ref",
              },
              operator: ">=",
              right: {
                type: "number",
                value: 2,
              },
              type: "binary_expr",
            },
            type: "binary_expr",
          },
          operator: "AND",
          parentheses: true,
          right: {
            left: {
              column: "title",
              table: null,
              type: "column_ref",
            },
            operator: "=",
            right: {
              type: "single_quote_string",
              value: "hello",
            },
            type: "binary_expr",
          },
          type: "binary_expr",
        },
        operator: "OR",
        right: {
          left: {
            left: {
              column: "year",
              table: null,
              type: "column_ref",
            },
            operator: ">",
            right: {
              type: "number",
              value: 2020,
            },
            type: "binary_expr",
          },
          operator: "AND",
          parentheses: true,
          right: {
            left: {
              column: "month",
              table: null,
              type: "column_ref",
            },
            operator: "<",
            right: {
              type: "number",
              value: 10,
            },
            type: "binary_expr",
          },
          type: "binary_expr",
        },
        type: "binary_expr",
      },
      operator: "AND",
      right: {
        type: "bool",
        value: true,
      },
      type: "binary_expr",
    };
    expect(getSQLWhereAST(sql)).toEqual(expected);
    expect(getPartsOnlySQLWhereString(sql, ["year", "month", "day"])).toEqual(
      "WHERE (`year` <= 2020 AND `month` >= 2 AND TRUE) OR (`year` > 2020 AND `month` < 10) AND TRUE",
    );
  });
});

describe("get limit", () => {
  it("can find limit", () => {
    const sql = "SELECT * FROM s3Object LIMIT 10";
    const expected = 10;
    expect(getSQLLimit(sql)).toEqual(expected);
  });
  it("if no limit, returns 0 (disabled)", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = 0;
    expect(getSQLLimit(sql)).toEqual(expected);
  });
  it("if no limit value, returns 0 (disabled)", () => {
    const sql = "SELECT * FROM s3Object LIMIT";
    const expected = 0;
    expect(getSQLLimit(sql)).toEqual(expected);
  });
});

describe("set limit", () => {
  it("can set limit", () => {
    const sql = "SELECT * FROM s3Object LIMIT 10";
    const expected = "SELECT * FROM s3Object LIMIT 20";
    expect(setSQLLimit(sql, 20)).toEqual(expected);
  });
  it("set lmit does nothing when no limit present", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = "SELECT * FROM s3Object";
    expect(setSQLLimit(sql, 20)).toEqual(expected);
  });
});
