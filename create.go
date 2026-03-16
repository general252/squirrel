package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/lann/builder"
)

type createData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	Targets           string
	ContentValue      interface{}
	SetClauses        []setClause
	Timeout           string
	Parallel          bool
	Suffixes          []Sqlizer
}

func (d *createData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *createData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *createData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *createData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.Targets) == 0 {
		err = fmt.Errorf("create statements must specify targets")
		return
	}
	if d.ContentValue != nil && len(d.SetClauses) > 0 {
		err = fmt.Errorf("create statements cannot use both Content and Set clauses")
		return
	}
	if d.ContentValue == nil && len(d.SetClauses) == 0 {
		err = fmt.Errorf("create statements must have at least one Set clause or Content")
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

	sql.WriteString("CREATE ")
	sql.WriteString(d.Targets)

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

	if d.PlaceholderFormat == Surreal {
		if len(d.Timeout) > 0 {
			sql.WriteString(" TIMEOUT ")
			sql.WriteString(d.Timeout)
		}

		if d.Parallel {
			sql.WriteString(" PARALLEL")
		}
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

// CreateBuilder builds SQL CREATE statements.
type CreateBuilder builder.Builder

func init() {
	builder.Register(CreateBuilder{}, createData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b CreateBuilder) PlaceholderFormat(f PlaceholderFormat) CreateBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(CreateBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b CreateBuilder) RunWith(runner BaseRunner) CreateBuilder {
	return setRunWith(b, runner).(CreateBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b CreateBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(createData)
	return data.Exec()
}

func (b CreateBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(createData)
	return data.Query()
}

func (b CreateBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(createData)
	return data.QueryRow()
}

func (b CreateBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b CreateBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(createData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b CreateBuilder) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b CreateBuilder) Prefix(sql string, args ...interface{}) CreateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b CreateBuilder) PrefixExpr(expr Sqlizer) CreateBuilder {
	return builder.Append(b, "Prefixes", expr).(CreateBuilder)
}

// Targets sets the targets to be created.
func (b CreateBuilder) Targets(targets string) CreateBuilder {
	return builder.Set(b, "Targets", targets).(CreateBuilder)
}

// Content sets the CONTENT clause to the query.
func (b CreateBuilder) Content(content interface{}) CreateBuilder {
	return builder.Set(b, "ContentValue", content).(CreateBuilder)
}

// Set adds SET clauses to the query.
func (b CreateBuilder) Set(column string, value interface{}) CreateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column, value: value}).(CreateBuilder)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b CreateBuilder) SetMap(clauses map[string]interface{}) CreateBuilder {
	for _, v := range setMap(clauses) {
		b = b.Set(v.column, v.value)
	}
	return b
}

func setMap(clauses map[string]interface{}) []setClause {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	setClauses := make([]setClause, len(keys))
	for i, key := range keys {
		setClauses[i] = setClause{column: key, value: clauses[key]}
	}
	return setClauses
}

// Timeout sets a TIMEOUT clause on the query (SurrealDB).
func (b CreateBuilder) Timeout(duration string) CreateBuilder {
	return builder.Set(b, "Timeout", duration).(CreateBuilder)
}

// Parallel sets a PARALLEL clause on the query (SurrealDB).
func (b CreateBuilder) Parallel() CreateBuilder {
	return builder.Set(b, "Parallel", true).(CreateBuilder)
}

// Suffix adds an expression to the end of the query
func (b CreateBuilder) Suffix(sql string, args ...interface{}) CreateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b CreateBuilder) SuffixExpr(expr Sqlizer) CreateBuilder {
	return builder.Append(b, "Suffixes", expr).(CreateBuilder)
}
