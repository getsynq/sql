package sql

import (
	"fmt"
	"time"
)

var _ Dialect = (*SqliteDialect)(nil)

type SqliteDialect struct{}

func NewSqliteDialect() *SqliteDialect {
	return &SqliteDialect{}
}

func (d *SqliteDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	// SQLite doesn't support schema, so we just return the table name
	return fmt.Sprintf("%s", fqn.tableId), nil
}

func (d *SqliteDialect) CountIf(expr Expr) Expr {
	// SQLite equivalent of COUNT with condition is SUM with a case expression
	return Fn("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *SqliteDialect) Count(expr Expr) Expr {
	return Fn("COUNT", expr)
}

func (d *SqliteDialect) Median(expr Expr) Expr {
	// SQLite doesn't have a built-in MEDIAN function, so we can simulate it
	return WrapSql(`
	(SELECT AVG(x) FROM (
		SELECT %s AS x
		FROM my_table
		ORDER BY x
		LIMIT 2 - (SELECT COUNT(*) FROM my_table) % 2 
		OFFSET (SELECT (COUNT(*) - 1) / 2 FROM my_table)
	))`, expr)
}

func (d *SqliteDialect) Stddev(expr Expr) Expr {
	// SQLite uses a user-defined function or a workaround for standard deviation
	return Fn("STDDEV", expr)
}

func (d *SqliteDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *SqliteDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *SqliteDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	// SQLite doesn't support DATE_TRUNC, so we can approximate it with strftime
	unit, _ := getTimeUnitWithInterval(interval)

	return WrapSql("strftime(%s, %s)", timeUnitString(unit), expr)
}

func (d *SqliteDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATETIME(%s, '-%d %s')", expr, Int64(-1*interval), timeUnitString(unit))
}

func (d *SqliteDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATETIME(%s, '+%d %s')", expr, Int64(interval), timeUnitString(unit))
}

func (d *SqliteDialect) Regexp(expr Expr, pattern string) Expr {
	// SQLite supports REGEXP through an extension, or you need to use LIKE
	return Fn("REGEXP", expr, String(pattern))
}

func (d *SqliteDialect) Identifier(identifier string) string {
	// For SQLite, we typically quote identifiers using backticks or square brackets
	return fmt.Sprintf("`%s`", identifier)
}

func (d *SqliteDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS TEXT)", expr)
}

func (d *SqliteDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS REAL)", expr)
}

func (d *SqliteDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *SqliteDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}
