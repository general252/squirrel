package squirrel

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type DBStub struct {
	err error

	LastPrepareSql string
	PrepareCount   int

	LastExecSql  string
	LastExecArgs []interface{}

	LastQuerySql  string
	LastQueryArgs []interface{}

	LastQueryRowSql  string
	LastQueryRowArgs []interface{}
}

var StubError = fmt.Errorf("this is a stub; this is only a stub")

func (s *DBStub) Prepare(query string) (*sql.Stmt, error) {
	s.LastPrepareSql = query
	s.PrepareCount++
	return nil, nil
}

func (s *DBStub) Exec(query string, args ...interface{}) (sql.Result, error) {
	s.LastExecSql = query
	s.LastExecArgs = args
	return nil, nil
}

func (s *DBStub) Query(query string, args ...interface{}) (*sql.Rows, error) {
	s.LastQuerySql = query
	s.LastQueryArgs = args
	return nil, nil
}

func (s *DBStub) QueryRow(query string, args ...interface{}) RowScanner {
	s.LastQueryRowSql = query
	s.LastQueryRowArgs = args
	return &Row{RowScanner: &RowStub{}}
}

var sqlizer = Select("test")
var sqlStr = "SELECT test"

func TestExecWith(t *testing.T) {
	db := &DBStub{}
	ExecWith(db, sqlizer)
	assert.Equal(t, sqlStr, db.LastExecSql)
}

func TestQueryWith(t *testing.T) {
	db := &DBStub{}
	QueryWith(db, sqlizer)
	assert.Equal(t, sqlStr, db.LastQuerySql)
}

func TestQueryRowWith(t *testing.T) {
	db := &DBStub{}
	QueryRowWith(db, sqlizer)
	assert.Equal(t, sqlStr, db.LastQueryRowSql)
}

func TestWithToSqlErr(t *testing.T) {
	db := &DBStub{}
	sqlizer := Select()

	_, err := ExecWith(db, sqlizer)
	assert.Error(t, err)

	_, err = QueryWith(db, sqlizer)
	assert.Error(t, err)

	err = QueryRowWith(db, sqlizer).Scan()
	assert.Error(t, err)
}

var testDebugUpdateSQL = Update("table").SetMap(Eq{"x": 1, "y": "val"})
var expectedDebugUpateSQL = "UPDATE table SET x = '1', y = 'val'"

func TestDebugSqlizerUpdateColon(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

func TestDebugSqlizerUpdateAtp(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

func TestDebugSqlizerUpdateDollar(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

func TestDebugSqlizerUpdateQuestion(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

var testDebugDeleteSQL = Delete("table").Where(And{
	Eq{"column": "val"},
	Eq{"other": 1},
})
var expectedDebugDeleteSQL = "DELETE FROM table WHERE (column = 'val' AND other = '1')"

func TestDebugSqlizerDeleteColon(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

func TestDebugSqlizerDeleteAtp(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

func TestDebugSqlizerDeleteDollar(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

func TestDebugSqlizerDeleteQuestion(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

var testDebugInsertSQL = Insert("table").Values(1, "test")
var expectedDebugInsertSQL = "INSERT INTO table VALUES ('1','test')"

func TestDebugSqlizerInsertColon(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

func TestDebugSqlizerInsertAtp(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

func TestDebugSqlizerInsertDollar(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

func TestDebugSqlizerInsertQuestion(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

var testDebugSelectSQL = Select("*").From("table").Where(And{
	Eq{"column": "val"},
	Eq{"other": 1},
})
var expectedDebugSelectSQL = "SELECT * FROM table WHERE (column = 'val' AND other = '1')"

func TestDebugSqlizerSelectColon(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizerSelectAtp(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizerSelectDollar(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizerSelectQuestion(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizer(t *testing.T) {
	sqlizer := Expr("x = ? AND y = ? AND z = '??'", 1, "text")
	expectedDebug := "x = '1' AND y = 'text' AND z = '?'"
	assert.Equal(t, expectedDebug, DebugSqlizer(sqlizer))
}

func TestDebugSqlizerErrors(t *testing.T) {
	errorMsg := DebugSqlizer(Expr("x = ?", 1, 2)) // Not enough placeholders
	assert.True(t, strings.HasPrefix(errorMsg, "[DebugSqlizer error: "))

	errorMsg = DebugSqlizer(Expr("x = ? AND y = ?", 1)) // Too many placeholders
	assert.True(t, strings.HasPrefix(errorMsg, "[DebugSqlizer error: "))

	errorMsg = DebugSqlizer(Lt{"x": nil}) // Cannot use nil values with Lt
	assert.True(t, strings.HasPrefix(errorMsg, "[ToSql error: "))
}

// ========== RELATE Builder Tests ==========

var testDebugRelateSQL = Relate("user:john", "wrote", "article:1").Set("when", "now()")
var expectedDebugRelateSQL = "RELATE user:john->wrote->article:1 SET when = 'now()'"

func TestDebugSqlizerRelateColon(t *testing.T) {
	testDebugRelateSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugRelateSQL, DebugSqlizer(testDebugRelateSQL))
}

func TestDebugSqlizerRelateAtp(t *testing.T) {
	testDebugRelateSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugRelateSQL, DebugSqlizer(testDebugRelateSQL))
}

func TestDebugSqlizerRelateDollar(t *testing.T) {
	testDebugRelateSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugRelateSQL, DebugSqlizer(testDebugRelateSQL))
}

func TestDebugSqlizerRelateQuestion(t *testing.T) {
	testDebugRelateSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugRelateSQL, DebugSqlizer(testDebugRelateSQL))
}

func TestExecWithRelate(t *testing.T) {
	db := &DBStub{}
	relate := Relate("user:john", "wrote", "article:1").Set("when", "now()")
	ExecWith(db, relate)
	assert.Contains(t, db.LastExecSql, "RELATE user:john->wrote->article:1")
	assert.Contains(t, db.LastExecSql, "SET when = ?")
}

func TestQueryWithRelate(t *testing.T) {
	db := &DBStub{}
	relate := Relate("user:john", "wrote", "article:1").Set("when", "now()")
	QueryWith(db, relate)
	assert.Contains(t, db.LastQuerySql, "RELATE user:john->wrote->article:1")
	assert.Contains(t, db.LastQuerySql, "SET when = ?")
}

func TestQueryRowWithRelate(t *testing.T) {
	db := &DBStub{}
	relate := Relate("user:john", "wrote", "article:1").Set("when", "now()")
	QueryRowWith(db, relate)
	assert.Contains(t, db.LastQueryRowSql, "RELATE user:john->wrote->article:1")
	assert.Contains(t, db.LastQueryRowSql, "SET when = ?")
}

func TestRelateBuilderContent(t *testing.T) {
	content := Expr(`{"timestamp": now(), "ip": "192.168.1.1"}`)
	b := Relate("user:1", "owns", "document:abc").Content(content)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql, "RELATE user:1->owns->document:abc")
	assert.Contains(t, sql, `CONTENT {"timestamp": now(), "ip": "192.168.1.1"}`)
}

func TestRelateBuilderSetMap(t *testing.T) {
	b := Relate("customer:A", "purchased", "product:X").
		SetMap(map[string]interface{}{
			"quantity":  5,
			"total":     149.95,
			"completed": true,
		})
	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql, "RELATE customer:A->purchased->product:X")
	assert.Contains(t, sql, "SET")
	assert.Contains(t, sql, "quantity = ?")
	assert.Contains(t, sql, "total = ?")
	assert.Contains(t, sql, "completed = ?")
	// Check that all expected values are present in args (order doesn't matter for maps)
	assert.Contains(t, args, 5)
	assert.Contains(t, args, 149.95)
	assert.Contains(t, args, true)
	assert.Len(t, args, 3)
}
