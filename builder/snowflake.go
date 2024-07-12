package sql

import (
	"fmt"
	"time"
)

var _ Dialect = (*SnowflakeDialect)(nil)

type SnowflakeDialect struct{}

func NewSnowflakeDialect() *SnowflakeDialect {
	return &SnowflakeDialect{}
}

func (d *SnowflakeDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	return fmt.Sprintf("%s.%s.%s", fqn.projectId, fqn.datasetId, fqn.tableId), nil
}

func (d *SnowflakeDialect) CountIf(expr Expr) Expr {
	return Fn("count_if", expr)
}

func (d *SnowflakeDialect) Count(expr Expr) Expr {
	return Fn("count", expr)
}

func (d *SnowflakeDialect) Median(expr Expr) Expr {
	return Fn("median", expr)
}

func (d *SnowflakeDialect) Stddev(expr Expr) Expr {
	return Fn("stddev", expr)
}

func (d *SnowflakeDialect) ResolveTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339)), nil
}

func (d *SnowflakeDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return expr.name, nil
}

func (d *SnowflakeDialect) RoundTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return Fn("time_slice", Fn("to_timestamp_ntz", expr), Int64(interval), timeUnitString(unit))
}

func (d *SnowflakeDialect) SubTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(-1*interval), expr)
}

func (d *SnowflakeDialect) AddTime(expr Expr, duration time.Duration) Expr {
	unit, interval := getTimeUnitWithInterval(duration)

	return WrapSql("TIMESTAMPADD(%s, %s, %s)", timeUnitSql(unit), Int64(interval), expr)
}

func (d *SnowflakeDialect) Regexp(expr Expr, pattern string) Expr {
	return Fn("REGEXP_LIKE", expr, String(pattern))
}

func (d *SnowflakeDialect) Identifier(identifier string) string {
	return fmt.Sprintf("%q", identifier)
}

func (d *SnowflakeDialect) ToString(expr Expr) Expr {
	return Fn("to_varchar", expr)
}

func (d *SnowflakeDialect) ToFloat64(expr Expr) Expr {
	return WrapSql("CAST(%s AS FLOAT)", expr)
}

func (d *SnowflakeDialect) Coalesce(exprs ...Expr) Expr {
	return Fn("COALESCE", exprs...)
}

func (d *SnowflakeDialect) AggregationColumnReference(expression Expr, alias string) Expr {
	return Identifier(alias)
}
