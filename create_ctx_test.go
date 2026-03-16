//go:build go1.8
// +build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateBuilderContextRunners(t *testing.T) {
	db := &DBStub{}
	b := Create("test").Set("x", 1).RunWith(db)

	expectedSql := "CREATE test SET x = ?"

	b.ExecContext(ctx)
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.QueryContext(ctx)
	assert.Equal(t, expectedSql, db.LastQuerySql)

	b.QueryRowContext(ctx)
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	err := b.ScanContext(ctx)
	assert.NoError(t, err)
}

func TestCreateBuilderContextNoRunner(t *testing.T) {
	b := Create("test").Set("x", 1)

	_, err := b.ExecContext(ctx)
	assert.Equal(t, RunnerNotSet, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, RunnerNotSet, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, RunnerNotSet, err)
}
