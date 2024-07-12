package sql

import (
	"fmt"
	"time"
)

var _ Dialect = (*PostgresDialect)(nil)

type PostgresDialect struct{}

func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{}
}

func (d *PostgresDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *PostgresDialect) CountIf(expr Expr) Expr {
	return Fn("SUM(CASE WHEN %s THEN 1 ELSE 0 END)", expr)
}

func (d *PostgresDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *PostgresDialect) Median(expr Expr) Expr {
	return Fn("MEDIAN", expr)
}

func (d *PostgresDialect) Stddev(expr Expr) Expr {
	return Fn("STDDEV", expr)
}

func (d *PostgresDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *PostgresDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *PostgresDialect) RoundTime(expr Expr, interval time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(interval)

	return Fn("DATE_TRUNC", timeUnitString(unit), expr)
}

func (d *PostgresDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATEADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *PostgresDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("DATEADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *PostgresDialect) Regexp(expr Expr, pattern string) Expr {
	return Fn("REGEXP_LIKE", expr, String(pattern))
}

func (d *PostgresDialect) Identifier(identifier string) string {
	return identifier
}

func (d *PostgresDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS VARCHAR)", expr)
}

func (d *PostgresDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *PostgresDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *PostgresDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return expression
}
