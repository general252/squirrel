package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type relateData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	In                string
	Edge              string
	Out               string
	ContentValue      interface{}
	SetClauses        []setClause
	Suffixes          []Sqlizer
	Timeout           string
	Parallel          bool
}

func (d *relateData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *relateData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *relateData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *relateData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.In) == 0 || len(d.Edge) == 0 || len(d.Out) == 0 {
		err = fmt.Errorf("relate statements must specify in, edge and out")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = appendToSql(d.Prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	sql.WriteString("RELATE ")
	sql.WriteString(d.In)
	sql.WriteString("->")
	sql.WriteString(d.Edge)
	sql.WriteString("->")
	sql.WriteString(d.Out)

	if d.ContentValue != nil {
		sql.WriteString(" CONTENT ")
		if vs, ok := d.ContentValue.(Sqlizer); ok {
			vsql, vargs, err := vs.ToSql()
			if err != nil {
				return "", nil, err
			}
			sql.WriteString(vsql)
			args = append(args, vargs...)
		} else {
			sql.WriteString("?")
			args = append(args, d.ContentValue)
		}
	} else if len(d.SetClauses) > 0 {
		sql.WriteString(" SET ")
		setSqls := make([]string, len(d.SetClauses))
		for i, setClause := range d.SetClauses {
			var valSql string
			if vs, ok := setClause.value.(Sqlizer); ok {
				vsql, vargs, err := vs.ToSql()
				if err != nil {
					return "", nil, err
				}
				if _, ok := vs.(SelectBuilder); ok {
					valSql = fmt.Sprintf("(%s)", vsql)
				} else {
					valSql = vsql
				}
				args = append(args, vargs...)
			} else {
				valSql = "?"
				args = append(args, setClause.value)
			}
			setSqls[i] = fmt.Sprintf("%s = %s", setClause.column, valSql)
		}
		sql.WriteString(strings.Join(setSqls, ", "))
	}

	if len(d.Timeout) > 0 {
		sql.WriteString(" TIMEOUT ")
		sql.WriteString(d.Timeout)
	}

	if d.Parallel {
		sql.WriteString(" PARALLEL")
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.Suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Builder

// RelateBuilder builds SQL RELATE statements (SurrealDB).
type RelateBuilder builder.Builder

func init() {
	builder.Register(RelateBuilder{}, relateData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b RelateBuilder) PlaceholderFormat(f PlaceholderFormat) RelateBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(RelateBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b RelateBuilder) RunWith(runner BaseRunner) RelateBuilder {
	return setRunWith(b, runner).(RelateBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b RelateBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(relateData)
	return data.Exec()
}

func (b RelateBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(relateData)
	return data.Query()
}

func (b RelateBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(relateData)
	return data.QueryRow()
}

func (b RelateBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b RelateBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(relateData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b RelateBuilder) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b RelateBuilder) Prefix(sql string, args ...interface{}) RelateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b RelateBuilder) PrefixExpr(expr Sqlizer) RelateBuilder {
	return builder.Append(b, "Prefixes", expr).(RelateBuilder)
}

// In sets the "in" part of the RELATE statement.
func (b RelateBuilder) In(in string) RelateBuilder {
	return builder.Set(b, "In", in).(RelateBuilder)
}

// Edge sets the "edge" part of the RELATE statement.
func (b RelateBuilder) Edge(edge string) RelateBuilder {
	return builder.Set(b, "Edge", edge).(RelateBuilder)
}

// Out sets the "out" part of the RELATE statement.
func (b RelateBuilder) Out(out string) RelateBuilder {
	return builder.Set(b, "Out", out).(RelateBuilder)
}

// Content sets the CONTENT clause to the query.
func (b RelateBuilder) Content(content interface{}) RelateBuilder {
	return builder.Set(b, "ContentValue", content).(RelateBuilder)
}

// Set adds SET clauses to the query.
func (b RelateBuilder) Set(column string, value interface{}) RelateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column, value: value}).(RelateBuilder)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b RelateBuilder) SetMap(clauses map[string]interface{}) RelateBuilder {
	for _, v := range setMap(clauses) {
		b = b.Set(v.column, v.value)
	}
	return b
}

// Timeout sets a TIMEOUT clause on the query (SurrealDB).
func (b RelateBuilder) Timeout(duration string) RelateBuilder {
	return builder.Set(b, "Timeout", duration).(RelateBuilder)
}

// Parallel sets a PARALLEL clause on the query (SurrealDB).
func (b RelateBuilder) Parallel() RelateBuilder {
	return builder.Set(b, "Parallel", true).(RelateBuilder)
}

// Suffix adds an expression to the end of the query
func (b RelateBuilder) Suffix(sql string, args ...interface{}) RelateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b RelateBuilder) SuffixExpr(expr Sqlizer) RelateBuilder {
	return builder.Append(b, "Suffixes", expr).(RelateBuilder)
}
