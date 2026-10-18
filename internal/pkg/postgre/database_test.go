package postgre

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io"
	"testing"
)

type schemaConnector struct {
	history [][]driver.Value
	exists  bool
}

func (c schemaConnector) Connect(context.Context) (driver.Conn, error) { return &schemaConn{c}, nil }
func (schemaConnector) Driver() driver.Driver                          { return schemaDriver{} }

type schemaDriver struct{}

func (schemaDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type schemaConn struct{ schemaConnector }

func (*schemaConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (*schemaConn) Close() error                        { return nil }
func (*schemaConn) Begin() (driver.Tx, error)           { return nil, errors.New("unexpected transaction") }
func (c *schemaConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if q == "SELECT to_regclass('schema_migrations') IS NOT NULL" {
		return &schemaRows{columns: []string{"exists"}, values: [][]driver.Value{{c.exists}}}, nil
	}
	return &schemaRows{columns: []string{"version", "name"}, values: c.history}, nil
}

type schemaRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *schemaRows) Columns() []string { return r.columns }
func (*schemaRows) Close() error        { return nil }
func (r *schemaRows) Next(dst []driver.Value) error {
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dst, r.values[r.index])
	r.index++
	return nil
}

func TestSchemaV3RequiresCompleteMigrationIdentity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		exists  bool
		history [][]driver.Value
		wantErr bool
	}{
		{"V3", true, [][]driver.Value{{int64(1), "initial_schema"}, {int64(2), "event_biz_cursors"}, {int64(3), "drop_chain_transactions_tx_type"}}, false},
		{"V2 only", true, [][]driver.Value{{int64(1), "initial_schema"}, {int64(2), "event_biz_cursors"}}, true},
		{"missing V1", true, [][]driver.Value{{int64(2), "event_biz_cursors"}}, true},
		{"wrong V3 identity", true, [][]driver.Value{{int64(1), "initial_schema"}, {int64(2), "event_biz_cursors"}, {int64(3), "wrong"}}, true},
		{"wrong V2 identity", true, [][]driver.Value{{int64(1), "initial_schema"}, {int64(2), "wrong"}, {int64(3), "drop_chain_transactions_tx_type"}}, true},
		{"wrong V1 identity", true, [][]driver.Value{{int64(1), "wrong"}, {int64(2), "event_biz_cursors"}, {int64(3), "drop_chain_transactions_tx_type"}}, true},
		{"missing table", false, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := sql.OpenDB(schemaConnector{history: tc.history, exists: tc.exists})
			t.Cleanup(func() { _ = conn.Close() })
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: conn}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Discard})
			if err != nil {
				t.Fatal(err)
			}
			err = CheckSchemaVersion(context.Background(), db, 3)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}
