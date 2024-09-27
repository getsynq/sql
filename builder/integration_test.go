package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SqlSuite struct {
	suite.Suite
}

func TestSqlSuite(t *testing.T) {
	suite.Run(t, new(SqlSuite))
}

func (s *SqlSuite) TestBuilder() {

	for _, tt := range []struct {
		title   string
		skip    bool
		q       *Select
		dialect Dialect

		expectedSql string
		expectedErr string
	}{
		{
			title:   "no_table",
			q:       NewSelect(),
			dialect: NewClickHouseDialect(),

			expectedErr: "table is required",
		},

		{
			title:   "simple",
			q:       NewSelect().From(Sql("table")),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table",
		},

		{
			title:   "simple_with_columns",
			q:       NewSelect().Cols(Sql("a"), Sql("b")).From(Sql("table")),
			dialect: NewClickHouseDialect(),

			expectedSql: "select a, b from table",
		},

		{
			title:   "simple_with_where",
			q:       NewSelect().From(Sql("table")).Where(Eq(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a = 1",
		},

		{
			title:   "simple_with_order",
			q:       NewSelect().From(Sql("table")).OrderBy(Asc(Sql("a"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table order by a",
		},

		{
			title:   "simple_with_order_desc",
			q:       NewSelect().From(Sql("table")).OrderBy(Desc(Sql("a"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table order by a desc",
		},

		{
			title:   "simple_with_limit",
			q:       NewSelect().From(Sql("table")).Limit(Limit(Int64(10))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table limit 10",
		},

		//
		// WHERE
		//

		{
			title: "where_eq",
			q: NewSelect().
				From(Sql("table")).
				Where(Eq(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a = 1",
		},

		{
			title: "where_ne",
			q: NewSelect().
				From(Sql("table")).
				Where(Neq(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a != 1",
		},

		{
			title: "where_gt",
			q: NewSelect().
				From(Sql("table")).
				Where(Gt(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a > 1",
		},

		{
			title: "where_gte",
			q: NewSelect().
				From(Sql("table")).
				Where(Gte(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a >= 1",
		},

		{
			title: "where_lt",
			q: NewSelect().
				From(Sql("table")).
				Where(Lt(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a < 1",
		},

		{
			title: "where_lte",
			q: NewSelect().
				From(Sql("table")).
				Where(Lte(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a <= 1",
		},

		{
			title: "where_and",
			q: NewSelect().
				From(Sql("table")).
				Where(Eq(Sql("a"), Int64(1)), Eq(Sql("b"), Int64(2))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a = 1 and b = 2",
		},

		{
			title: "where_or",
			q: NewSelect().
				From(Sql("table")).
				Where(Or(Eq(Sql("a"), Int64(1)), Eq(Sql("b"), Int64(2)))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where (a = 1 or b = 2)",
		},

		{
			title: "where_not",
			q: NewSelect().
				From(Sql("table")).
				Where(Not(Eq(Sql("a"), Int64(1)))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where not a = 1",
		},

		{
			title: "where_and_or",
			q: NewSelect().
				From(Sql("table")).
				Where(
					And(
						Eq(Sql("a"), Int64(1)),
						Or(
							Eq(Sql("b"), Int64(2)),
							Eq(Sql("c"), Int64(3)),
						),
					),
				),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where (a = 1 and (b = 2 or c = 3))",
		},

		{
			title: "where_and_or_not",
			q: NewSelect().
				From(Sql("table")).
				Where(
					And(
						Eq(Sql("a"), Int64(1)),
						Or(
							Eq(Sql("b"), Int64(2)),
							Not(Eq(Sql("c"), Int64(3))),
						),
					),
				),

			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where (a = 1 and (b = 2 or not c = 3))",
		},

		{
			title: "where_between",
			q: NewSelect().
				From(Sql("table")).
				Where(Between(Sql("a"), Int64(1), Int64(2))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a between 1 and 2",
		},

		{
			title: "between_dates",
			q: NewSelect().
				From(Sql("table")).
				Where(Between(Sql("date"), String("2020-01-01"), String("2020-01-02"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where date between '2020-01-01' and '2020-01-02'",
		},

		{
			title: "where_in",
			q: NewSelect().
				From(Sql("table")).
				Where(In(Sql("a"), Int64(1), Int64(2), Int64(3))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a in (1, 2, 3)",
		},

		//
		// CTE
		//

		{
			title: "simple_cte",
			q: NewSelect().
				Cte("cte", NewSelect().From(Sql("table"))).
				From(Sql("cte")),
			dialect: NewClickHouseDialect(),

			expectedSql: "with cte as (select * from table) select * from cte",
		},

		{
			title: "simple_cte_with_columns",
			q: NewSelect().
				Cte("cte", NewSelect().Cols(Sql("a"), Sql("b")).From(Sql("table"))).
				From(Sql("cte")),
			dialect: NewClickHouseDialect(),

			expectedSql: "with cte as (select a, b from table) select * from cte",
		},

		{
			title: "multiple_cte",
			q: NewSelect().
				Cte("cte1", NewSelect().From(Sql("table1"))).
				Cte("cte2", NewSelect().From(Sql("cte1"))).
				From(Sql("cte2")),
			dialect: NewClickHouseDialect(),

			expectedSql: "with cte1 as (select * from table1), cte2 as (select * from cte1) select * from cte2",
		},
		{
			title: "append_cte",
			q: NewSelect().
				Cte("cte1", NewSelect().From(Sql("table1"))).
				CteAppend("cte2", NewSelect().From(Sql("cte1"))).
				From(Sql("cte2")),
			dialect: NewClickHouseDialect(),

			expectedSql: "with cte1 as (select * from table1), cte2 as (select * from cte1) select * from cte2",
		},
		{
			title: "prepend_cte",
			q: NewSelect().
				Cte("cte2", NewSelect().From(Sql("cte1"))).
				CtePrepend("cte1", NewSelect().From(Sql("table1"))).
				From(Sql("cte2")),
			dialect: NewClickHouseDialect(),

			expectedSql: "with cte1 as (select * from table1), cte2 as (select * from cte1) select * from cte2",
		},
		{
			title: "insert_before_cte",
			q: NewSelect().
				Cte("cte2", NewSelect().From(Sql("cte1"))).
				CteInsertBefore("cte2", "cte1", NewSelect().From(Sql("table1"))).
				From(Sql("cte2")),
			dialect: NewClickHouseDialect(),

			expectedSql: "with cte1 as (select * from table1), cte2 as (select * from cte1) select * from cte2",
		},

		{
			title: "int_lit",
			q: NewSelect().
				From(Sql("table")).
				Where(Eq(Sql("a"), Int64(1))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a = 1",
		},

		{
			title: "int_lit_int_8",
			q: NewSelect().
				From(Sql("table")).
				Where(Eq(Sql("a"), Int64(int8(1)))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table where a = 1",
		},

		//
		// JOIN
		//

		{
			title: "simple_join",
			q: NewSelect().
				From(Sql("table1")).
				Join(Sql("table2"), On(Eq(Sql("table1.id"), Sql("table2.id")))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 join table2 on table1.id = table2.id",
		},

		{
			title: "simple_left_join",
			q: NewSelect().
				From(Sql("table1")).
				Join(Sql("table2"), On(Eq(Sql("table1.id"), Sql("table2.id"))), WithJoinKind(LeftJoin)),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 left join table2 on table1.id = table2.id",
		},

		{
			title: "simple_right_join",
			q: NewSelect().
				From(Sql("table1")).
				Join(Sql("table2"), On(Eq(Sql("table1.id"), Sql("table2.id"))), WithJoinKind(RightJoin)),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 right join table2 on table1.id = table2.id",
		},

		{
			title: "simple_full_join",
			q: NewSelect().
				From(Sql("table1")).
				Join(Sql("table2"), On(Eq(Sql("table1.id"), Sql("table2.id"))), WithJoinKind(FullJoin)),

			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 full join table2 on table1.id = table2.id",
		},

		{
			title: "simple_cross_join",
			q: NewSelect().
				From(Sql("table1")).
				Join(Sql("table2"), On(Eq(Sql("table1.id"), Sql("table2.id"))), WithJoinKind(CrossJoin)),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 cross join table2 on table1.id = table2.id",
		},

		{
			title: "join_using",
			q: NewSelect().
				From(Sql("table1")).
				Join(Sql("table2"), Using(Sql("id"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 join table2 using (id)",
		},

		//
		// SET
		//

		{
			title: "union",
			q: NewSelect().
				From(Sql("table1")).
				Union(NewSelect().From(Sql("table2"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 union select * from table2",
		},

		{
			title: "union_all",
			q: NewSelect().
				From(Sql("table1")).
				UnionAll(NewSelect().From(Sql("table2"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 union all select * from table2",
		},

		{
			title: "intersect",
			q: NewSelect().
				From(Sql("table1")).
				Intersect(NewSelect().From(Sql("table2"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 intersect select * from table2",
		},

		{
			title: "except",
			q: NewSelect().
				From(Sql("table1")).
				Except(NewSelect().From(Sql("table2"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 except select * from table2",
		},

		{
			title: "intersect_all",
			q: NewSelect().
				From(Sql("table1")).
				IntersectAll(NewSelect().From(Sql("table2"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 intersect all select * from table2",
		},

		{
			title: "except_all",
			q: NewSelect().
				From(Sql("table1")).
				ExceptAll(NewSelect().From(Sql("table2"))),
			dialect: NewClickHouseDialect(),

			expectedSql: "select * from table1 except all select * from table2",
		},
	} {
		if tt.skip {
			continue
		}

		s.Run(tt.title, func() {
			sql, err := tt.q.ToSql(tt.dialect)

			if tt.expectedErr != "" {
				require.ErrorContains(s.T(), err, tt.expectedErr)
			} else {
				require.NoError(s.T(), err)
				require.Equal(s.T(), tt.expectedSql, sql)
			}
		})
	}
}

func (s *SqlSuite) TestGetters() {

	q := NewSelect().
		From(Sql("table")).
		Cols(Sql("a")).
		Where(Eq(Sql("b"), Int64(1)))

	s.Run("get_table", func() {
		require.Equal(s.T(), Sql("table"), q.GetTable())
	})

	s.Run("get_cols", func() {
		require.Equal(s.T(), []Expr{Sql("a")}, q.GetCols())
	})

	s.Run("get_where", func() {
		require.Equal(s.T(), []CondExpr{Eq(Sql("b"), Int64(1))}, q.GetWhere())
	})

	s.Run("has_empty_cols", func() {
		require.False(s.T(), q.HasEmptyCols())
	})

}
