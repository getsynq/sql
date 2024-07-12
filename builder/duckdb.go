package sql

import (
	"fmt"
	"time"
)

var _ Dialect = (*DuckdbDialect)(nil)

type DuckdbDialect struct{}

func NewDuckdbDialect() *DuckdbDialect {
	return &DuckdbDialect{}
}

func (d *DuckdbDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s", fqn.datasetId, fqn.tableId), nil
}

func (d *DuckdbDialect) CountIf(expr Expr) Expr {
	return Fn("countIf", expr)
}

func (d *DuckdbDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *DuckdbDialect) Median(expr Expr) Expr {
	return Fn("median", expr)
}

func (d *DuckdbDialect) Stddev(expr Expr) Expr {
	return Fn("stddev", expr)
}

func (d *DuckdbDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *DuckdbDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *DuckdbDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, _ := getTimeUnitWithInterval(duration)

	return Fn("date_trunc", timeUnitString(unit), expr)
}

func (d *DuckdbDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *DuckdbDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *DuckdbDialect) Regexp(expr Expr, pattern string) Expr {
	return Fn("REGEXP_LIKE", expr, String(pattern))
}

func (d *DuckdbDialect) Identifier(identifier string) string {
	return fmt.Sprintf("%q", identifier)
}

func (d *DuckdbDialect) ToString(expr Expr) Expr {
	return Fn("CAST", expr, String("VARCHAR"))
}

func (d *DuckdbDialect) ToFloat64(expr Expr) Expr {
	return Fn("CAST", expr, String("FLOAT"))
}

func (d *DuckdbDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *DuckdbDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}
