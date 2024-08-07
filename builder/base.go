package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
)

//
// Base
//

type Expr interface {
	ToSql(dialect Dialect) (string, error)
}

type TextExpr interface {
	Expr
	IsTextExpr()
}

type NumericExpr interface {
	Expr
	IsNumericExpr()
}

type TimeExpr interface {
	Expr
	IsTimeExpr()
}

type CondExpr interface {
	Expr
	IsCondExpr()
}

type TableExpr interface {
	Expr
	IsTableExpr()
}

type LimitExpr struct {
	rows *IntLitExpr
}

func Limit(rows *IntLitExpr) *LimitExpr {
	return &LimitExpr{rows}
}

func (e *LimitExpr) ToSql(dialect Dialect) (string, error) {
	rowsSql, err := e.rows.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("limit %s", rowsSql), nil
}

type NotImplementedExpr struct {
	msg string
}

func NotImplementedWithExplanation(msg string) *NotImplementedExpr {
	return &NotImplementedExpr{msg: msg}
}

func NotImplemented() *NotImplementedExpr {
	return &NotImplementedExpr{}
}

func (e *NotImplementedExpr) ToSql(dialect Dialect) (string, error) {
	return "", fmt.Errorf("not implemented %s", e.msg)
}

// IdentifierExpr
type IdentifierExpr struct {
	identifier string
}

func Identifier(identifier string) *IdentifierExpr {
	return &IdentifierExpr{identifier: identifier}
}

func (i *IdentifierExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.Identifier(i.identifier), nil
}

func (s *IdentifierExpr) IsTextExpr() {}

//
// OrderExpr
//

type OrderExpr struct {
	expr Expr
	desc bool
}

func Asc(expr Expr) *OrderExpr {
	return &OrderExpr{expr: expr, desc: false}
}

func Desc(expr Expr) *OrderExpr {
	return &OrderExpr{expr: expr, desc: true}
}

func (e *OrderExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	if e.desc {
		return fmt.Sprintf("%s desc", exprSql), nil
	}

	return exprSql, nil
}

//
// SelectExpr
//

type Select struct {
	cols    []Expr
	table   TableExpr
	joins   []*JoinExpr
	where   []CondExpr
	groupBy []Expr
	orderBy []*OrderExpr
	limit   *LimitExpr
	having  []CondExpr
	cte     map[string]Expr

	setOp *SetOpExpr
}

func NewSelect() *Select {
	return &Select{}
}

func (s *Select) HasEmptyCols() bool {
	return len(s.cols) == 0
}

func (s *Select) From(table TableExpr) *Select {
	s.table = table
	return s
}

func (s *Select) Where(conds ...CondExpr) *Select {
	s.where = append(s.where, conds...)
	return s
}

func (s *Select) Cols(cols ...Expr) *Select {
	s.cols = append(s.cols, cols...)
	return s
}

func (s *Select) Join(other TableExpr, how JoinDefExpr, opts ...func(*JoinExpr) *JoinExpr) *Select {
	join := Join(other, how)

	for _, opt := range opts {
		join = opt(join)
	}

	s.joins = append(s.joins, join)
	return s
}

func (s *Select) GroupBy(groupBy ...Expr) *Select {
	s.groupBy = append(s.groupBy, groupBy...)
	return s
}

func (s *Select) OrderBy(orderBy ...*OrderExpr) *Select {
	s.orderBy = append(s.orderBy, orderBy...)
	return s
}

func (s *Select) Having(conds ...CondExpr) *Select {
	s.having = append(s.having, conds...)
	return s
}

func (s *Select) Limit(limit *LimitExpr) *Select {
	s.limit = limit
	return s
}

func (s *Select) Union(right *Select) *Select {
	s.setOp = NewSetOp(right, Union)
	return s
}

func (s *Select) UnionAll(right *Select) *Select {
	s.setOp = NewSetOp(right, UnionAll)
	return s
}

func (s *Select) Intersect(right *Select) *Select {
	s.setOp = NewSetOp(right, Intersect)
	return s
}

func (s *Select) IntersectAll(right *Select) *Select {
	s.setOp = NewSetOp(right, IntersectAll)
	return s
}

func (s *Select) Except(right *Select) *Select {
	s.setOp = NewSetOp(right, Except)
	return s
}

func (s *Select) ExceptAll(right *Select) *Select {
	s.setOp = NewSetOp(right, ExceptAll)
	return s
}

func (s *Select) Cte(alias string, selectExpr Expr) *Select {
	if s.cte == nil {
		s.cte = map[string]Expr{}
	}

	s.cte[alias] = selectExpr
	return s
}

func (s *Select) ColNames() []string {
	names := []string{}
	for _, col := range s.cols {
		switch e := col.(type) {
		case *AsExpr:
			name, _ := e.alias.ToSql(nil)
			names = append(names, name)

		default:
			name, _ := e.ToSql(nil)
			names = append(names, name)
		}
	}

	return lo.Uniq(names)
}

func (s *Select) ToSql(dialect Dialect) (string, error) {

	if len(s.cols) == 0 {
		s.cols = append(s.cols, Star())
	}

	colsSql, err := exprsToSql(s.cols, dialect)
	if err != nil {
		return "", err
	}

	if s.table == nil {
		return "", fmt.Errorf("table is required")
	}

	tableSql, err := s.table.ToSql(dialect)
	if err != nil {
		return "", err
	}

	whereSql, err := exprsToSql(s.where, dialect)
	if err != nil {
		return "", err
	}

	groupBySql, err := exprsToSql(s.groupBy, dialect)
	if err != nil {
		return "", err
	}

	orderBySql, err := exprsToSql(s.orderBy, dialect)
	if err != nil {
		return "", err
	}

	joinsSql, err := exprsToSql(s.joins, dialect)
	if err != nil {
		return "", err
	}

	havingSql, err := exprsToSql(s.having, dialect)
	if err != nil {
		return "", err
	}

	limitSql := ""
	if s.limit != nil {
		limitSql, err = s.limit.ToSql(dialect)
		if err != nil {
			return "", err
		}
	}

	cteSql := ""
	if len(s.cte) > 0 {
		cteSqls := []string{}
		for alias, selectExpr := range s.cte {
			selectSql, err := selectExpr.ToSql(dialect)
			if err != nil {
				return "", err
			}

			cteSqls = append(cteSqls, fmt.Sprintf("%s as (%s)", alias, selectSql))
		}

		cteSql = "with " + strings.Join(cteSqls, ", ") + " "
	}

	controls := strings.Join(lo.Filter([]string{
		buildListSegment("", " ", joinsSql),
		buildListSegment("where", " and ", whereSql),
		buildListSegment("group by", ", ", groupBySql),
		buildListSegment("order by", ", ", orderBySql),
		buildListSegment("having", ", ", havingSql),
		limitSql,
	}, func(s string, _ int) bool { return s != "" }), " ")

	setOpSql := ""

	if s.setOp != nil {
		sql, err := s.setOp.ToSql(dialect)
		if err != nil {
			return "", err
		}

		setOpSql = " " + sql
	}

	return fmt.Sprintf(`%s%s from %s%s`,
		cteSql,
		buildListSegment("select", ", ", colsSql),
		strings.Join(lo.Filter([]string{
			tableSql,
			controls,
		}, func(s string, _ int) bool { return s != "" }), " "),
		setOpSql,
	), nil
}

func buildListSegment(segmentId string, separator string, sqls []string) string {
	if len(sqls) == 0 {
		return ""
	}

	return strings.Join(lo.Filter([]string{
		segmentId,
		strings.Join(sqls, separator),
	}, func(s string, _ int) bool { return s != "" }), " ")
}

func exprsToSql[T Expr](exprs []T, dialect Dialect) ([]string, error) {
	sqls := []string{}
	for _, expr := range exprs {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return nil, err
		}

		sqls = append(sqls, exprSql)
	}

	return sqls, nil
}

//
// Expr
//

type StarExpr struct {
	except []Expr
}

func Star(except ...Expr) *StarExpr {
	return &StarExpr{except: except}
}

func (s *StarExpr) ToSql(dialect Dialect) (string, error) {
	return "*", nil
}

//
// SqlExpr
//

type SqlExpr struct {
	sql string
}

func Sql(sql string) *SqlExpr {
	return &SqlExpr{sql: sql}
}

func (s *SqlExpr) ToSql(dialect Dialect) (string, error) {
	return s.sql, nil
}

func (s *SqlExpr) IsTextExpr()    {}
func (s *SqlExpr) IsNumericExpr() {}
func (s *SqlExpr) IsTimeExpr()    {}
func (s *SqlExpr) IsCondExpr()    {}
func (s *SqlExpr) IsTableExpr()   {}
func (s *SqlExpr) IsJoinExpr()    {}

//
// Regexp
//

type RegexpExpr struct {
	expr    Expr
	pattern string
}

func Regexp(expr Expr, pattern string) *RegexpExpr {
	return &RegexpExpr{expr: expr, pattern: pattern}
}

func (s *RegexpExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := dialect.Regexp(s.expr, s.pattern).ToSql(dialect)
	if err != nil {
		return "", err
	}

	return exprSql, nil
}

//
// Col
//

type ColBaseExpr struct {
	sql string
}

func (s *ColBaseExpr) ToSql(dialect Dialect) (string, error) {
	return s.sql, nil
}

type TextColExpr struct {
	ColBaseExpr
}

func TextCol(name string) *TextColExpr {
	return &TextColExpr{ColBaseExpr: ColBaseExpr{sql: name}}
}

func (s *TextColExpr) IsTextExpr() {}

type TimeColExpr struct {
	name string
}

func TimeCol(name string) *TimeColExpr {
	return &TimeColExpr{name: name}
}

func (t *TimeColExpr) IsTimeExpr() {}

func (t *TimeColExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveTimeColumn(t)
}

type NumericColExpr struct {
	ColBaseExpr
}

func NumericCol(name string) *NumericColExpr {
	return &NumericColExpr{ColBaseExpr: ColBaseExpr{sql: name}}
}

func (s *NumericColExpr) IsNumericExpr() {}

//
// TableFqn
//

type TableFqnExpr struct {
	projectId string
	datasetId string
	tableId   string
}

func TableFqn(projectId, datasetId, tableId string) *TableFqnExpr {
	return &TableFqnExpr{
		projectId: projectId,
		datasetId: datasetId,
		tableId:   tableId,
	}
}

func (t *TableFqnExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveFqn(t)
}

func (t *TableFqnExpr) IsTableExpr() {}

//
// JoinExpr
//

type JoinKind string

const (
	InnerJoin      JoinKind = "inner"
	LeftJoin       JoinKind = "left"
	RightJoin      JoinKind = "right"
	FullJoin       JoinKind = "full"
	CrossJoin      JoinKind = "cross"
	LeftOuterJoin  JoinKind = "left outer"
	RightOuterJoin JoinKind = "right outer"
	FullOuterJoin  JoinKind = "full outer"
	CrossOuterJoin JoinKind = "cross outer"
)

type JoinExpr struct {
	kind  JoinKind
	other TableExpr
	how   JoinDefExpr
}

func WithJoinKind(kind JoinKind) func(*JoinExpr) *JoinExpr {
	return func(e *JoinExpr) *JoinExpr {
		e.kind = kind
		return e
	}
}

func Join(other TableExpr, how JoinDefExpr) *JoinExpr {
	return &JoinExpr{
		other: other,
		how:   how,
	}
}

func (t *JoinExpr) ToSql(dialect Dialect) (string, error) {
	tableSql, err := t.other.ToSql(dialect)
	if err != nil {
		return "", err
	}

	howSql, err := t.how.ToSql(dialect)
	if err != nil {
		return "", err
	}

	sql := ""
	switch t.kind {
	case InnerJoin:
		sql = "inner join "
	case LeftJoin:
		sql = "left join "

	case RightJoin:
		sql = "right join "

	case FullJoin:
		sql = "full join "

	case CrossJoin:
		sql = "cross join "

	case LeftOuterJoin:
		sql = "left outer join "

	case RightOuterJoin:
		sql = "right outer join "

	case FullOuterJoin:
		sql = "full outer join "

	case CrossOuterJoin:
		sql = "cross outer join "

	default:
		sql = "join "
	}

	return fmt.Sprintf("%s%s %s", sql, tableSql, howSql), nil
}

func (t *JoinExpr) IsJoinExpr() {}

type JoinDefExpr interface {
	Expr
	IsJoinDefExpr()
}

type JoinOnExpr struct {
	conds []CondExpr
}

func On(conds ...CondExpr) *JoinOnExpr {
	return &JoinOnExpr{conds: conds}
}

func (t *JoinOnExpr) ToSql(dialect Dialect) (string, error) {
	sqls, err := exprsToSql(t.conds, dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("on %s", strings.Join(sqls, " and ")), nil
}

func (t *JoinOnExpr) IsJoinDefExpr() {}

type JoinUsingExpr struct {
	exprs []Expr
}

func Using(exprs ...Expr) *JoinUsingExpr {
	return &JoinUsingExpr{exprs: exprs}
}

func (t *JoinUsingExpr) ToSql(dialect Dialect) (string, error) {
	sqls, err := exprsToSql(t.exprs, dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("using (%s)", strings.Join(sqls, ", ")), nil
}

func (t *JoinUsingExpr) IsJoinDefExpr() {}

//
// Placeholder
//

type ArgExpr struct {
	pos int
}

func Arg(pos int) *ArgExpr {
	return &ArgExpr{pos: pos}
}

func (e *ArgExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("$%d", e.pos), nil
}

//
// LitExpr
//

type StringLitExpr struct {
	val string
}

func String(val string) *StringLitExpr {
	return &StringLitExpr{val: val}
}

func Stringf(format string, val ...any) *StringLitExpr {
	return &StringLitExpr{val: fmt.Sprintf(format, val...)}
}

func (e *StringLitExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("'%v'", e.val), nil
}

func (e *StringLitExpr) IsTableExpr() {}

//
// IntLitExpr
//

type Integer interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

type IntLitExpr struct {
	val int64
}

func Int64[T Integer](val T) *IntLitExpr {
	return &IntLitExpr{val: int64(val)}
}

func (e *IntLitExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("%d", e.val), nil
}

func (e *IntLitExpr) IsNumericExpr() {}

//
// FloatLitExpor
//

type FloatLitExpr struct {
	val float64
}

func Float64(val float64) *FloatLitExpr {
	return &FloatLitExpr{val: val}
}

func (e *FloatLitExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("%f", e.val), nil
}

//
// TimeLitExpr
//

type TimeLitExpr struct {
	val *time.Time
}

func Time(val time.Time) *TimeLitExpr {
	return &TimeLitExpr{val: &val}
}

func (e *TimeLitExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveTime(*e.val)
}

func (e *TimeLitExpr) IsTimeExpr() {}

//
// BoolLitExpr
//

type BoolLitExpr struct {
	val bool
}

func Bool(val bool) *BoolLitExpr {
	return &BoolLitExpr{val: val}
}

func (e *BoolLitExpr) ToSql(dialect Dialect) (string, error) {
	return fmt.Sprintf("%t", e.val), nil
}

//
// NullLitExpr
//

type NullLitExpr struct{}

func Null() *NullLitExpr {
	return &NullLitExpr{}
}

func (e *NullLitExpr) ToSql(dialect Dialect) (string, error) {
	return "null", nil
}

//
// AsExpr
//

type AsExpr struct {
	expr  Expr
	alias TextExpr
}

func As(expr Expr, alias TextExpr) *AsExpr {
	return &AsExpr{expr: expr, alias: alias}
}

func (e *AsExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	aliasSql, err := e.alias.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s as %s", exprSql, aliasSql), nil
}

//
// DistinctExpr
//

type DistinctExpr struct {
	expr Expr
}

func Distinct(expr Expr) *DistinctExpr {
	return &DistinctExpr{expr: expr}
}

func (e *DistinctExpr) ToSql(dialect Dialect) (string, error) {
	exprSql, err := e.expr.ToSql(dialect)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("distinct %s", exprSql), nil
}

//
// ToStringExpr
//

type ToStringExpr struct {
	expr Expr
}

func ToString(expr Expr) *ToStringExpr {
	return &ToStringExpr{expr: expr}
}

func (e *ToStringExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ToString(e.expr).ToSql(dialect)
}

type ToFloat64Expr struct {
	expr Expr
}

func ToFloat64(expr Expr) *ToFloat64Expr {
	return &ToFloat64Expr{expr: expr}
}

func (e *ToFloat64Expr) ToSql(dialect Dialect) (string, error) {
	return dialect.ToFloat64(e.expr).ToSql(dialect)
}

//
// FnExpr
//

type FnExpr struct {
	name string
	ops  []Expr
}

func Fn(name string, ops ...Expr) *FnExpr {
	return &FnExpr{name: name, ops: ops}
}

func (e *FnExpr) ToSql(dialect Dialect) (string, error) {
	ops := []string{}
	for _, op := range e.ops {
		opSql, err := op.ToSql(dialect)
		if err != nil {
			return "", err
		}

		ops = append(ops, opSql)
	}

	return fmt.Sprintf("%s(%s)", e.name, strings.Join(ops, ", ")), nil
}

//
// WrapSqlExpr
//

type WrapSqlExpr struct {
	sql     string
	wrapped []Expr
}

func WrapSql(sql string, wrapped ...Expr) *WrapSqlExpr {
	return &WrapSqlExpr{sql: sql, wrapped: wrapped}
}

func (e *WrapSqlExpr) ToSql(dialect Dialect) (string, error) {
	anySql := []interface{}{}
	for _, expr := range e.wrapped {
		exprSql, err := expr.ToSql(dialect)
		if err != nil {
			return "", err
		}

		anySql = append(anySql, exprSql)
	}

	return fmt.Sprintf(e.sql, anySql...), nil
}

//
// AggregationColumnReferenceExpr
// Not all databases support referencing aggregation columns by alias in WHERE.
// E.g. postgresql does not support it.
//

type AggregationColumnReferenceExpr struct {
	expression Expr
	alias      string
}

func AggregationColumnReference(expression Expr, alias string) *AggregationColumnReferenceExpr {
	return &AggregationColumnReferenceExpr{expression: expression, alias: alias}
}

func (e *AggregationColumnReferenceExpr) ToSql(dialect Dialect) (string, error) {
	refExpr := dialect.AggregationColumnReference(e.expression, e.alias)

	return refExpr.ToSql(dialect)
}

type SetOpOperand int

const (
	Union SetOpOperand = iota
	UnionAll
	Intersect
	IntersectAll
	Except
	ExceptAll
)

type SetOpExpr struct {
	right   *Select
	operand SetOpOperand
}

func NewSetOp(right *Select, operand SetOpOperand) *SetOpExpr {
	return &SetOpExpr{right: right, operand: operand}
}

func (e *SetOpExpr) ToSql(dialect Dialect) (string, error) {
	rightSql, err := e.right.ToSql(dialect)
	if err != nil {
		return "", err
	}

	var operand string
	switch e.operand {
	case Union:
		operand = "union"
	case UnionAll:
		operand = "union all"
	case Intersect:
		operand = "intersect"
	case IntersectAll:
		operand = "intersect all"
	case Except:
		operand = "except"
	case ExceptAll:
		operand = "except all"
	}

	return fmt.Sprintf("%s %s", operand, rightSql), nil

}
