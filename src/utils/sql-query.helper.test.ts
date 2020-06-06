import {
  getSQLWhereAST,
  getSQLWhereString,
  getSQLWhereStringFromAST,
  makePartitionSpecificAST,
} from "./sql-query.helper";

describe("SQL WHERE clauses", () => {
  test("with single clause", () => {
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

  test("without WHERE", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = null;
    expect(getSQLWhereAST(sql)).toEqual(expected);
  });

  test("without WHERE with partition specific WHERE mutation", () => {
    const sql = "SELECT * FROM s3Object";
    const expected = null;
    expect(makePartitionSpecificAST(getSQLWhereAST(sql), [])).toEqual(expected);
  });

  test("set non-partition clauses true, one partition column", () => {
    const sql = "SELECT * FROM s3Object WHERE year<=2020 AND (foo=1 OR bar=2) AND true";
    expect(getSQLWhereStringFromAST(makePartitionSpecificAST(getSQLWhereAST(sql), ["year", "month", "day"]))).toEqual(
      "WHERE `year` <= 2020 AND (TRUE OR TRUE) AND TRUE",
    );
  });

  test("set non-partition clauses true, no partition columns", () => {
    const sql = "SELECT * FROM s3Object WHERE (foo=1 OR bar=2) AND true";
    expect(getSQLWhereStringFromAST(makePartitionSpecificAST(getSQLWhereAST(sql), ["year", "month", "day"]))).toEqual(
      "WHERE (TRUE OR TRUE) AND TRUE",
    );
  });

  test("with two clauses", () => {
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
    expect(getSQLWhereString(sql, ["year", "month", "day"])).toEqual("WHERE `year` <= 2020 AND `month` >= 2");
  });

  test("with subclauses", () => {
    const sql =
      "SELECT * FROM s3Object WHERE " +
      "(year<=2020 AND month>=2 AND title='hello') OR " +
      "(year>2020 AND month<10) AND true";
    const expected = {
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
            type: "string",
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
        operator: "AND",
        right: {
          type: "bool",
          value: true,
        },
        type: "binary_expr",
      },
      type: "binary_expr",
    };
    expect(getSQLWhereAST(sql)).toEqual(expected);
    expect(getSQLWhereString(sql, ["year", "month", "day"])).toEqual(
      "WHERE (`year` <= 2020 AND `month` >= 2 AND TRUE) OR (`year` > 2020 AND `month` < 10) AND TRUE",
    );
  });
});
