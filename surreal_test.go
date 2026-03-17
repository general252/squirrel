package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ========== SELECT Builder Surreal Tests ==========

func TestSelectBuilderSurreal(t *testing.T) {
	b := Select("*").
		From("user:john").
		Version("2022-07-03T07:18:52Z").
		Where(Eq{"name": "John"}).
		Limit(20).
		Start(40).
		Fetch("address", "projects").
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT * FROM user:john VERSION 2022-07-03T07:18:52Z WHERE name = $p1 LIMIT 20 START 40 FETCH address, projects TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"John"}, args)
}

func TestSelectBuilderSurrealNoVersion(t *testing.T) {
	b := Select("id", "name").
		From("book").
		Where(Eq{"published": true}).
		OrderBy("title").
		Limit(10).
		Start(0).
		Fetch("author").
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT id, name FROM book WHERE published = $p1 ORDER BY title LIMIT 10 START 0 FETCH author"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{true}, args)
}

func TestSelectBuilderSurrealWithRemoveMethods(t *testing.T) {
	b := Select("*").
		From("test").
		Limit(100).
		Offset(10).
		Start(5).
		Fetch("field1").
		Timeout("10s").
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql, "LIMIT 100")
	assert.Contains(t, sql, "START 5")
	assert.Contains(t, sql, "FETCH field1")
	assert.Contains(t, sql, "TIMEOUT 10s")
}

func TestSelectBuilderSurrealStartRemoval(t *testing.T) {
	b := Select("*").
		From("test").
		Start(5).
		RemoveStart().
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.NotContains(t, sql, "START")
}

func TestSelectBuilderSurrealFetchRemoval(t *testing.T) {
	b := Select("*").
		From("test").
		Fetch("a", "b").
		RemoveFetch().
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.NotContains(t, sql, "FETCH")
}

func TestSelectBuilderSurrealDistinct(t *testing.T) {
	b := Select("*").
		Distinct().
		From("user").
		Where(Eq{"status": "active"}).
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT DISTINCT * FROM user WHERE status = $p1 PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"active"}, args)
}

// ========== CREATE Builder Surreal Tests ==========

func TestCreateBuilderSurreal(t *testing.T) {
	b := Create("user:john").
		Set("name", "John").
		Set("age", 30).
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE user:john SET name = $p1, age = $p2 TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"John", 30}, args)
}

func TestCreateBuilderSurrealContent(t *testing.T) {
	content := Expr(`{"id": "user:123", "name": "John", "role": "admin"}`)
	b := Create("user").
		Content(content).
		Timeout("3s").
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE user CONTENT {\"id\": \"user:123\", \"name\": \"John\", \"role\": \"admin\"} TIMEOUT 3s"
	assert.Equal(t, expectedSql, sql)
	assert.NotContains(t, sql, "$") // no positional placeholders
}

func TestCreateBuilderSurrealTable(t *testing.T) {
	b := Create("table:student").
		SetMap(map[string]interface{}{
			"name":    "Alice",
			"grade":   95,
			"active":  true,
		}).
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE table:student SET active = $p1, grade = $p2, name = $p3 PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{true, 95, "Alice"}, args)
}

func TestCreateBuilderSurrealInvalidArgs(t *testing.T) {
	// No Set or Content should error
	b := Create("test")
	_, _, err := b.ToSql()
	assert.Error(t, err)

	// Both Set and Content should error
	b2 := Create("test").Set("a", 1).Content(Expr("{}"))
	_, _, err2 := b2.ToSql()
	assert.Error(t, err2)
}

// ========== UPDATE Builder Surreal Tests ==========

func TestUpdateBuilderSurreal(t *testing.T) {
	b := Update("user:john").
		Set("name", "John").
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE user:john SET name = $p1 TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"John"}, args)
}

func TestUpdateBuilderSurrealContent(t *testing.T) {
	content := Expr(`{"id": "user:456", "name": "Jane", "email": "jane@example.com"}`)
	b := Update("user:jane").
		Content(content).
		Timeout("2s").
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE user:jane CONTENT {\"id\": \"user:456\", \"name\": \"Jane\", \"email\": \"jane@example.com\"} TIMEOUT 2s"
	assert.Equal(t, expectedSql, sql)
	assert.NotContains(t, sql, "$") // no positional placeholders
}

func TestUpdateBuilderSurrealMerge(t *testing.T) {
	content := Expr(`{"id": "user:789", "name": "Bob", "email": "bob@example.com"}`)
	b := Update("user:bob").
		Content(content).
		Merge().
		Timeout("3s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE user:bob MERGE {\"id\": \"user:789\", \"name\": \"Bob\", \"email\": \"bob@example.com\"} TIMEOUT 3s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.NotContains(t, sql, "$") // no positional placeholders
}

func TestUpdateBuilderSurrealWithFrom(t *testing.T) {
	subquery := Select("avg_score").From("scores").Where(Eq{"student_id": "user:789"})
	b := Update("user:789").
		Set("average", subquery).
		From("students").
		Where(Eq{"verified": true}).
		OrderBy("created_at").
		Limit(1).
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE user:789 SET average = (SELECT avg_score FROM scores WHERE student_id = $p1) FROM students WHERE verified = $p2 ORDER BY created_at LIMIT 1 TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"user:789", true}, args)
}

func TestUpdateBuilderSurrealSetMap(t *testing.T) {
	b := Update("product:123").
		SetMap(map[string]interface{}{
			"price":  29.99,
			"stock":  100,
			"on_sale": false,
		}).
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE product:123 SET on_sale = $p1, price = $p2, stock = $p3 PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{false, 29.99, 100}, args)
}

// ========== DELETE Builder Surreal Tests ==========

func TestDeleteBuilderSurreal(t *testing.T) {
	b := Delete("user:john").
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE FROM user:john TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
}

func TestDeleteBuilderSurrealMultiple(t *testing.T) {
	b := Delete("session:*").
		Where(Eq{"expires_at": nil}).
		Timeout("1m").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE FROM session:* WHERE expires_at IS NULL TIMEOUT 1m PARALLEL"
	assert.Equal(t, expectedSql, sql)
}

// ========== RELATE Builder Surreal Tests ==========

func TestRelateBuilderSurreal(t *testing.T) {
	b := Relate("user:john", "wrote", "article:1").
		Set("when", "now()").
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "RELATE user:john->wrote->article:1 SET when = $p1 TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"now()"}, args)
}

func TestRelateBuilderSurrealContent(t *testing.T) {
	content := Expr(`{"timestamp": now(), "ip": "192.168.1.1"}`)
	b := Relate("user:1", "owns", "document:abc").
		Content(content).
		Timeout("3s").
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "RELATE user:1->owns->document:abc CONTENT {\"timestamp\": now(), \"ip\": \"192.168.1.1\"} TIMEOUT 3s"
	assert.Equal(t, expectedSql, sql)
	assert.NotContains(t, sql, "$") // no positional placeholders
}

func TestRelateBuilderSurrealSetMap(t *testing.T) {
	b := Relate("customer:A", "purchased", "product:X").
		SetMap(map[string]interface{}{
			"quantity":  5,
			"total":     149.95,
			"completed": true,
		}).
		Timeout("1s").
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "RELATE customer:A->purchased->product:X SET completed = $p1, quantity = $p2, total = $p3 TIMEOUT 1s"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{true, 5, 149.95}, args)
}

func TestSelectBuilderSurrealValue(t *testing.T) {
	b := Select("*").
		Value("name").
		From("user").
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT VALUE name FROM user"
	assert.Equal(t, expectedSql, sql)
}

func TestInsertBuilderSurrealContent(t *testing.T) {
	content := map[string]interface{}{"name": "John", "age": 30}
	b := Insert("user").
		Content(content).
		Timeout("5s").
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "INSERT INTO user CONTENT $p1 TIMEOUT 5s"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{content}, args)
}

// ========== Complex SurrealDB Scenarios ==========

func TestSurrealComplexInsertScenario(t *testing.T) {
	// Simulate a complex insert scenario with nested records
	userRecord := Expr(`{
		"id": "user:new",
		"name": "Bob",
		"profile": {
			"age": 25,
			"city": "New York"
		}
	}`)

	b := Create("user").
		Content(userRecord).
		Timeout("10s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.NotEmpty(t, sql)
}

func TestSurrealNestedQueryInCreate(t *testing.T) {
	// Create with a subquery value
	subquery := Select("id", "name").From("temp_users").Where(Eq{"verified": true})
	b := Create("user").
		Set("source", subquery).
		Timeout("5s").
		PlaceholderFormat(Surreal)

	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql, "(SELECT")
	assert.Contains(t, sql, "FROM temp_users")
	assert.Contains(t, sql, "WHERE verified = $p1")
}

func TestSurrealMixedPlaceholders(t *testing.T) {
	// Test mixed use of placeholders in different clauses
	b := Select("u.*").
		From("user u").
		Join("profile p ON p.user_id = u.id").
		Where(Eq{"u.status": "active"}).
		Where(Eq{"p.verified": true}).
		Fetch("posts", "comments").
		Timeout("5s").
		Parallel().
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT u.* FROM user u JOIN profile p ON p.user_id = u.id WHERE u.status = $p1 AND p.verified = $p2 FETCH posts, comments TIMEOUT 5s PARALLEL"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"active", true}, args)
}

// ========== Placeholder Format Isolation Tests ==========

func TestSurrealPlaceholderIsolation(t *testing.T) {
	// Verify Surreal format produces dollar placeholders
	b1 := Select("*").From("t").Where(Eq{"a": 1}).PlaceholderFormat(Surreal)
	sql1, args1, _ := b1.ToSql()
	assert.Contains(t, sql1, "$p1")
	assert.Equal(t, []interface{}{1}, args1)

	// Compare with Question format
	b2 := Select("*").From("t").Where(Eq{"a": 1}).PlaceholderFormat(Question)
	sql2, args2, _ := b2.ToSql()
	assert.Contains(t, sql2, "?")
	assert.Equal(t, []interface{}{1}, args2)

	// Compare with Dollar format
	b3 := Select("*").From("t").Where(Eq{"a": 1}).PlaceholderFormat(Dollar)
	sql3, args3, _ := b3.ToSql()
	assert.Contains(t, sql3, "$1")
	assert.Equal(t, []interface{}{1}, args3)
}

func TestMultipleBuildersIndependent(t *testing.T) {
	// Verify builders are independent after setting placeholder format
	b1 := Create("t1").Set("a", 1).PlaceholderFormat(Surreal)
	b2 := Create("t2").Set("b", 2).PlaceholderFormat(Surreal)

	sql1, _, _ := b1.ToSql()
	sql2, _, _ := b2.ToSql()

	// Each builder starts from $1 independently (args are local to ToSql call)
	assert.Equal(t, "CREATE t1 SET a = $p1", sql1)
	assert.Equal(t, "CREATE t2 SET b = $p1", sql2)
}

func TestSelectBuilderSurrealContains(t *testing.T) {
	b := Select("*").
		From("user").
		Where(Contains{"tags": "admin"}).
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT * FROM user WHERE tags CONTAINS $p1"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"admin"}, args)
}

func TestSelectBuilderSurrealMultipleContains(t *testing.T) {
	b := Select("*").
		From("user").
		Where(And{Contains{"tags": "admin"}, Contains{"permissions": "read"}}).
		PlaceholderFormat(Surreal)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT * FROM user WHERE (tags CONTAINS $p1 AND permissions CONTAINS $p2)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"admin", "read"}, args)
}