package sql

import (
	"fmt"
	"time"
)

var _ Dialect = (*BigQueryDialect)(nil)

type BigQueryDialect struct{}

func NewBigQueryDialect() *BigQueryDialect {
	return &BigQueryDialect{}
}

func (d *BigQueryDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("`%s.%s.%s`", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *BigQueryDialect) CountIf(expr Expr) Expr {
	return Fn("countif", expr)
}

func (d *BigQueryDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *BigQueryDialect) Median(expr Expr) Expr {
	return WrapSql("approx_quantiles(%s, 2)[offset(1)]", expr)
}

func (d *BigQueryDialect) Stddev(expr Expr) Expr {
	return Fn("stddev_samp", expr)
}

func (d *BigQueryDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("timestamp '%s'", t.Format(time.RFC3339)), nil
}

func (d *BigQueryDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return fmt.Sprintf("timestamp(%s)", expr.name), nil
}

func (d *BigQueryDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(duration)

	return Fn("timestamp_trunc", expr, timeUnitSql(unit))
}

func (d *BigQueryDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMP_ADD(%s, INTERVAL %s %s)", expr, Int64(-1*interval), timeUnitSql(unit))
}

func (d *BigQueryDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMP_ADD(%s, INTERVAL %s %s)", expr, Int64(interval), timeUnitSql(unit))
}

func (d *BigQueryDialect) Regexp(expr Expr, pattern string) Expr {
	return Fn("REGEXP_CONTAINS", expr, String(pattern))
}

func (d *BigQueryDialect) Identifier(identifier string) string {
	return identifier
}

func (d *BigQueryDialect) ToString(expr Expr) Expr {
	return WrapSql("SAFE_CAST(%s AS STRING)", expr)
}

func (d *BigQueryDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT64)", expr)
}

func (d *BigQueryDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *BigQueryDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}
