package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOffsetHandling(t *testing.T) {
	// Test with regular database (should use OFFSET)
	b1 := Select("*").From("users").Offset(10).Limit(20)
	sql1, _, err := b1.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql1, "OFFSET 10")
	assert.NotContains(t, sql1, "START")

	// Test with SurrealDB (should use START instead of OFFSET)
	b2 := Select("*").From("users").Offset(10).Limit(20).PlaceholderFormat(Surreal)
	sql2, _, err := b2.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql2, "START 10")
	assert.NotContains(t, sql2, "OFFSET")

	// Test with SurrealDB using both Start and Offset (should prioritize Start)
	b3 := Select("*").From("users").Offset(10).Start(20).Limit(30).PlaceholderFormat(Surreal)
	sql3, _, err := b3.ToSql()
	assert.NoError(t, err)
	assert.Contains(t, sql3, "START 20")
	assert.NotContains(t, sql3, "OFFSET")
}
