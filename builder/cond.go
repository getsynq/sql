package sql

import (
	"fmt"
	"strings"
)

//
// CompareExpr
//

type CompareFn string

const (
	COMPARE_EQ  CompareFn = "="
	COMPARE_NEQ CompareFn = "!="
	COMPARE_LT  CompareFn = "<"
	COMPARE_GT  CompareFn = ">"
	COMPARE_LTE CompareFn = "<="
	COMPARE_GTE CompareFn = ">="
)

type CompareExpr struct {
	a  Expr
	b  Expr
	fn CompareFn
}

func compare(a Expr, fn CompareFn, b Expr) *CompareExpr {
	return &CompareExpr{a: a, b: b, fn: fn}
}

func (e *CompareExpr) ToSql(dialect Dialect) (string, error) {
	aSql, err := e.a.ToSql(dialect)
	if err != nil {
		return "", err
	}

	bSql, err := e.b.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s %s %s", aSql, e.fn, bSql), nil
}

func (e *CompareExpr) String() {
	fmt.Printf("%s %s %s", e.a, e.fn, e.b)
}

func (e *CompareExpr) IsCondExpr() {}

//
// BetweenExpr
//

type BetweenExpr struct {
	col  Expr
	from Expr
	to   Expr
}

func Between(col, from, to Expr) *BetweenExpr {
	return &BetweenExpr{col: col, from: from, to: to}
}

func (e *BetweenExpr) ToSql(dialect Dialect) (string, error) {
	colSql, err := e.col.ToSql(dialect)
	if err != nil {
		return "", err
	}

	fromSql, err := e.from.ToSql(dialect)
	if err != nil {
		return "", err
	}

	toSql, err := e.to.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s between %s and %s", colSql, fromSql, toSql), nil
}

func (e *BetweenExpr) IsCondExpr() {}

//
// InExpr
//

type InExpr struct {
	col  Expr
	list []Expr
}

func In(col Expr, list ...Expr) *InExpr {
	return &InExpr{col: col, list: list}
}

func (e *InExpr) ToSql(dialect Dialect) (string, error) {
	colSql, err := e.col.ToSql(dialect)
	if err != nil {
		return "", err
	}

	inSql := []string{}
	for _, expr := range e.list {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		inSql = append(inSql, exprSql)
	}

	return fmt.Sprintf("%s in (%s)", colSql, strings.Join(inSql, ", ")), nil
}

func (e *InExpr) IsCondExpr() {}

type AndExpr struct {
	exprs []Expr
}

func And(exprs ...Expr) *AndExpr {
	return &AndExpr{exprs: exprs}
}

func (e *AndExpr) ToSql(dialect Dialect) (string, error) {
	exprSqls := []string{}
	for _, expr := range e.exprs {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		exprSqls = append(exprSqls, exprSql)
	}

	return fmt.Sprintf("(%s)", strings.Join(exprSqls, " and ")), nil
}

func (e *AndExpr) IsCondExpr() {}

//
// OR
//

type OrExpr struct {
	exprs []Expr
}

func Or(exprs ...Expr) *OrExpr {
	return &OrExpr{exprs: exprs}
}

func (e *OrExpr) ToSql(dialect Dialect) (string, error) {
	exprSqls := []string{}
	for _, expr := range e.exprs {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		exprSqls = append(exprSqls, exprSql)
	}

	return fmt.Sprintf("(%s)", strings.Join(exprSqls, " or ")), nil
}

func (e *OrExpr) IsCondExpr() {}

//
// NOT
//

type NotExpr struct {
	expr Expr
}

func Not(expr Expr) *NotExpr {
	return &NotExpr{expr: expr}
}

func (e *NotExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("not %s", exprSql), nil
}

func (e *NotExpr) IsCondExpr() {}

//
// Shortcuts
//

func Eq(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_EQ, b)
}

func Neq(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_NEQ, b)
}

func Gt(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_GT, b)
}

func Lt(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_LT, b)
}

func Gte(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_GTE, b)
}

func Lte(a, b Expr) *CompareExpr {
	return compare(a, COMPARE_LTE, b)
}

func Like(a, b Expr) *CompareExpr {
	return compare(a, "like", b)
}
