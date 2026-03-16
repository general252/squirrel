package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type updateData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	Table             string
	SetClauses        []setClause
	ContentValue      interface{}
	Merge             bool
	From              Sqlizer
	WhereParts        []Sqlizer
	OrderBys          []string
	Limit             string
	Offset            string
	Parallel          bool
	Timeout           string
	Suffixes          []Sqlizer
}

type setClause struct {
	column string
	value  interface{}
}

func (d *updateData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *updateData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *updateData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *updateData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.Table) == 0 {
		err = fmt.Errorf("update statements must specify a table")
		return
	}
	if len(d.SetClauses) == 0 && d.ContentValue == nil {
		err = fmt.Errorf("update statements must have at least one Set clause or Content")
		return
	}
	if d.ContentValue != nil && len(d.SetClauses) > 0 {
		err = fmt.Errorf("update statements cannot use both Content and Set clauses")
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

	sql.WriteString("UPDATE ")
	sql.WriteString(d.Table)

	if d.PlaceholderFormat == Surreal && d.ContentValue != nil {
		if d.Merge {
			sql.WriteString(" MERGE ")
		} else {
			sql.WriteString(" CONTENT ")
		}
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

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSql([]Sqlizer{d.From}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(d.OrderBys, ", "))
	}

	if len(d.Limit) > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(d.Limit)
	}

	if len(d.Offset) > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(d.Offset)
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

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder builder.Builder

func init() {
	builder.Register(UpdateBuilder{}, updateData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) UpdateBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(UpdateBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b UpdateBuilder) RunWith(runner BaseRunner) UpdateBuilder {
	return setRunWith(b, runner).(UpdateBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b UpdateBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(updateData)
	return data.Exec()
}

func (b UpdateBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(updateData)
	return data.Query()
}

func (b UpdateBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(updateData)
	return data.QueryRow()
}

func (b UpdateBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b UpdateBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(updateData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b UpdateBuilder) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b UpdateBuilder) Prefix(sql string, args ...interface{}) UpdateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b UpdateBuilder) PrefixExpr(expr Sqlizer) UpdateBuilder {
	return builder.Append(b, "Prefixes", expr).(UpdateBuilder)
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return builder.Set(b, "Table", table).(UpdateBuilder)
}

// Content sets the CONTENT clause to the query.
func (b UpdateBuilder) Content(content interface{}) UpdateBuilder {
	return builder.Set(b, "ContentValue", content).(UpdateBuilder)
}

// Set adds SET clauses to the query.
func (b UpdateBuilder) Set(column string, value interface{}) UpdateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column, value: value}).(UpdateBuilder)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilder) SetMap(clauses map[string]interface{}) UpdateBuilder {
	for _, v := range setMap(clauses) {
		b = b.Set(v.column, v.value)
	}
	return b
}

// From adds FROM clause to the query
// FROM is valid construct in postgresql only.
func (b UpdateBuilder) From(from string) UpdateBuilder {
	return builder.Set(b, "From", newPart(from)).(UpdateBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b UpdateBuilder) FromSelect(from SelectBuilder, alias string) UpdateBuilder {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	return builder.Set(b, "From", Alias(from, alias)).(UpdateBuilder)
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilder) Where(pred interface{}, args ...interface{}) UpdateBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(UpdateBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b UpdateBuilder) OrderBy(orderBys ...string) UpdateBuilder {
	return builder.Extend(b, "OrderBys", orderBys).(UpdateBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b UpdateBuilder) Limit(limit uint64) UpdateBuilder {
	return builder.Set(b, "Limit", fmt.Sprintf("%d", limit)).(UpdateBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b UpdateBuilder) Offset(offset uint64) UpdateBuilder {
	return builder.Set(b, "Offset", fmt.Sprintf("%d", offset)).(UpdateBuilder)
}

// Timeout sets a TIMEOUT clause on the query (SurrealDB).
func (b UpdateBuilder) Timeout(duration string) UpdateBuilder {
	return builder.Set(b, "Timeout", duration).(UpdateBuilder)
}

// Parallel sets a PARALLEL clause on the query (SurrealDB).
func (b UpdateBuilder) Parallel() UpdateBuilder {
	return builder.Set(b, "Parallel", true).(UpdateBuilder)
}

// Merge sets the MERGE clause on the query (SurrealDB).
func (b UpdateBuilder) Merge() UpdateBuilder {
	return builder.Set(b, "Merge", true).(UpdateBuilder)
}

// Suffix adds an expression to the end of the query
func (b UpdateBuilder) Suffix(sql string, args ...interface{}) UpdateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b UpdateBuilder) SuffixExpr(expr Sqlizer) UpdateBuilder {
	return builder.Append(b, "Suffixes", expr).(UpdateBuilder)
}
