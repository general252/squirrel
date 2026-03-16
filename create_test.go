package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateBuilderToSql(t *testing.T) {
	b := Create("users").
		Prefix("/* batch */").
		Set("name", "John").
		Set("age", 30).
		Suffix("RETURN AFTER")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "/* batch */ CREATE users SET name = ?, age = ? RETURN AFTER"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"John", 30}
	assert.Equal(t, expectedArgs, args)
}

func TestCreateBuilderToSqlContent(t *testing.T) {
	b := Create("users").
		Content(map[string]interface{}{"name": "John", "age": 30})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE users CONTENT ?"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{map[string]interface{}{"name": "John", "age": 30}}, args)
}

func TestCreateBuilderZeroTargets(t *testing.T) {
	b := Create("").Set("x", 1)
	_, _, err := b.ToSql()
	assert.Error(t, err)
}

func TestCreateBuilderNoSetOrContent(t *testing.T) {
	b := Create("users")
	_, _, err := b.ToSql()
	assert.Error(t, err)
}

func TestCreateBuilderContentAndSetConflict(t *testing.T) {
	b := Create("users").
		Content(map[string]interface{}{"name": "John"}).
		Set("age", 30)

	_, _, err := b.ToSql()
	assert.Error(t, err)
}

func TestCreateBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCreateBuilderMustSql should have panicked!")
		}
	}()
	Create("").MustSql()
}

func TestCreateBuilderPlaceholders(t *testing.T) {
	b := Create("users").Set("x", 1).Set("y", 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "CREATE users SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "CREATE users SET x = $1, y = $2", sql)
}

func TestCreateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Create("test").Set("x", 1).RunWith(db)

	expectedSql := "CREATE test SET x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestCreateBuilderNoRunner(t *testing.T) {
	b := Create("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestCreateBuilderNoRunnerQuery(t *testing.T) {
	b := Create("test").Set("x", 1)

	_, err := b.Query()
	assert.Equal(t, RunnerNotSet, err)
}

func TestCreateBuilderNoRunnerQueryRow(t *testing.T) {
	b := Create("test").Set("x", 1)

	err := b.QueryRow().Scan()
	assert.Equal(t, RunnerNotSet, err)
}

func TestCreateBuilderQueryRowNoQueryRunner(t *testing.T) {
	b := Create("test").Set("x", 1).RunWith(fakeBaseRunner{})

	err := b.Scan()
	assert.Equal(t, RunnerNotQueryRunner, err)
}

func TestCreateBuilderSetMap(t *testing.T) {
	b := Create("users").SetMap(map[string]interface{}{
		"name": "John",
		"age":  30,
	})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE users SET age = ?, name = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{30, "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestCreateBuilderSetSqlizer(t *testing.T) {
	b := Create("users").
		Set("name", Expr("UPPER(?)", "john")).
		Set("role", "admin")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE users SET name = UPPER(?), role = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"john", "admin"}
	assert.Equal(t, expectedArgs, args)
}

func TestCreateBuilderSetSelectSubquery(t *testing.T) {
	b := Create("users").
		Set("default_role", Select("name").From("roles").Where("is_default = ?", true))

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE users SET default_role = (SELECT name FROM roles WHERE is_default = ?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{true}
	assert.Equal(t, expectedArgs, args)
}

func TestCreateBuilderContentSqlizer(t *testing.T) {
	b := Create("users").
		Content(Expr("$data"))

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE users CONTENT $data"
	assert.Equal(t, expectedSql, sql)
	assert.Empty(t, args)
}

func TestCreateBuilderQueryRunner(t *testing.T) {
	db := &DBStub{}
	b := Create("test").Set("x", 1).RunWith(db)

	expectedSql := "CREATE test SET x = ?"

	b.Query()
	assert.Equal(t, expectedSql, db.LastQuerySql)

	b.QueryRow()
	assert.Equal(t, expectedSql, db.LastQueryRowSql)
}
