package sql

import (
	"time"
)

type Dialect interface {
	ResolveFqn(fqn *TableFqnExpr) (string, error)

	Count(expr Expr) Expr
	CountIf(Expr) Expr
	Median(Expr) Expr
	Stddev(Expr) Expr
	RoundTime(Expr, time.Duration) Expr
	SubTime(Expr, time.Duration) Expr
	AddTime(Expr, time.Duration) Expr

	Regexp(Expr, string) Expr

	Identifier(string) string
	ToString(Expr) Expr
	Coalesce(exprs ...Expr) Expr
	ToFloat64(Expr) Expr

	ResolveTime(time.Time) (string, error)
	ResolveTimeColumn(col *TimeColExpr) (string, error)
	AggregationColumnReference(expression Expr, alias string) Expr
}
