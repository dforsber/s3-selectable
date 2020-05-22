import { getSQLWhereAST, getSQLWhereString } from "./sql-query.helper";

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
    expect(getSQLWhereString(sql)).toEqual("WHERE `year` <= 2020 AND `month` >= 2");
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
    expect(getSQLWhereString(sql)).toEqual(
      "WHERE (`year` <= 2020 AND `month` >= 2 AND `title` = 'hello') OR (`year` > 2020 AND `month` < 10) AND TRUE",
    );
  });
});
