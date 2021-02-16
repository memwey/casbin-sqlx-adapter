package sqlxadapter

import (
	"fmt"
	"runtime"

	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	"github.com/jmoiron/sqlx"
)

// CasbinRule ...
type CasbinRule struct {
	ID    uint   `db:"id"`
	PType string `db:"p_type"`
	V0    string `db:"v0"`
	V1    string `db:"v1"`
	V2    string `db:"v2"`
	V3    string `db:"v3"`
	V4    string `db:"v4"`
	V5    string `db:"v5"`
}

// Adapter represents the sqlx adapter for policy storage.
type Adapter struct {
	db        *sqlx.DB
	tableName string
}

// AdapterOptions contains all possible configuration options.
type AdapterOptions struct {
	DriverName     string
	DataSourceName string
	TableName      string
	DB             *sqlx.DB
}

func finalizer(a *Adapter) {
	a.db.Close()
}

func loadPolicyLine(line CasbinRule, model model.Model) {
	lineText := line.PType
	if line.V0 != "" {
		lineText += ", " + line.V0
	}
	if line.V1 != "" {
		lineText += ", " + line.V1
	}
	if line.V2 != "" {
		lineText += ", " + line.V2
	}
	if line.V3 != "" {
		lineText += ", " + line.V3
	}
	if line.V4 != "" {
		lineText += ", " + line.V4
	}
	if line.V5 != "" {
		lineText += ", " + line.V5
	}
	persist.LoadPolicyLine(lineText, model)
}

func savePolicyLine(ptype string, rule []string) CasbinRule {
	line := CasbinRule{}
	line.PType = ptype
	if len(rule) > 0 {
		line.V0 = rule[0]
	}
	if len(rule) > 1 {
		line.V1 = rule[1]
	}
	if len(rule) > 2 {
		line.V2 = rule[2]
	}
	if len(rule) > 3 {
		line.V3 = rule[3]
	}
	if len(rule) > 4 {
		line.V4 = rule[4]
	}
	if len(rule) > 5 {
		line.V5 = rule[5]
	}
	return line
}

func (a *Adapter) dropTable() {
	_, err := a.db.Exec(fmt.Sprintf("DELETE FROM `%s`", a.tableName))
	if err != nil {
		panic(err)
	}
}

func (a *Adapter) ensureTable() {
	driverName := a.db.DriverName()
	_, err := a.db.Exec(fmt.Sprintf("SELECT 1 FROM `%s` LIMIT 1", a.tableName))
	if err != nil {
		switch driverName {
		case "postgres":
			fallthrough
		case "pgx":
			idName := fmt.Sprintf("id_%s", a.tableName)

			a.db.MustExec(fmt.Sprintf(`
			CREATE SEQUENCE IF NOT EXISTS %s;

			CREATE TABLE IF NOT EXISTS %s(
				id INTEGER NOT NULL DEFAULT nextval('%s') ,
				p_type VARCHAR(32) NOT NULL DEFAULT '',
				v0 VARCHAR(255) NOT NULL DEFAULT '',
				v1 VARCHAR(255) NOT NULL DEFAULT '',
				v2 VARCHAR(255) NOT NULL DEFAULT '',
				v3 VARCHAR(255) NOT NULL DEFAULT '',
				v4 VARCHAR(255) NOT NULL DEFAULT '',
				v5 VARCHAR(255) NOT NULL DEFAULT '',
				PRIMARY KEY (id)
				);
			ALTER SEQUENCE %s OWNED BY %s.id
				 `, idName, a.tableName, idName, idName, a.tableName))
		case "mysql":
			a.db.MustExec(
				fmt.Sprintf(
					`CREATE TABLE IF NOT EXISTS %s(
				id INT UNSIGNED NOT NULL AUTO_INCREMENT,
				p_type VARCHAR(32) NOT NULL DEFAULT '',
				v0 VARCHAR(255) NOT NULL DEFAULT '',
				v1 VARCHAR(255) NOT NULL DEFAULT '',
				v2 VARCHAR(255) NOT NULL DEFAULT '',
				v3 VARCHAR(255) NOT NULL DEFAULT '',
				v4 VARCHAR(255) NOT NULL DEFAULT '',
				v5 VARCHAR(255) NOT NULL DEFAULT '',
				PRIMARY KEY (id)
				)`, a.tableName),
			)
		case "sqlite":
			fallthrough
		case "sqlite3":
			a.db.MustExec(
				fmt.Sprintf(
					`CREATE TABLE IF NOT EXISTS %s(
					id INTEGER,
					p_type VARCHAR(32) NOT NULL DEFAULT '',
					v0 VARCHAR(255) NOT NULL DEFAULT '',
					v1 VARCHAR(255) NOT NULL DEFAULT '',
					v2 VARCHAR(255) NOT NULL DEFAULT '',
					v3 VARCHAR(255) NOT NULL DEFAULT '',
					v4 VARCHAR(255) NOT NULL DEFAULT '',
					v5 VARCHAR(255) NOT NULL DEFAULT '',
					PRIMARY KEY (id)
					)`, a.tableName),
			)

			if err != nil {
				panic(err)
			}
		default:
			panic(err)
		}
	}
}

func (a *Adapter) insertPolicyLine(line *CasbinRule) (err error) {
	query := fmt.Sprintf("INSERT INTO %s (p_type, v0, v1, v2, v3, v4, v5) VALUES (:p_type, :v0, :v1, :v2, :v3, :v4, :v5)", a.tableName)
	_, err = a.db.NamedExec(query, line)
	if err != nil {
		return
	}
	return
}

func (a *Adapter) deletePolicyLine(line *CasbinRule) (err error) {
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE p_type = :p_type AND v0 = :v0 AND v1 = :v1 AND v2 = :v2 AND v3 = :v3 AND v4 = :v4 AND v5 = :v5",
		a.tableName,
	)
	_, err = a.db.NamedExec(query, line)
	if err != nil {
		return
	}
	return
}

// NewAdapter is the constructor for Adapter
// Deprecated: Use NewAdapterFromOptions instead
func NewAdapter(driverName string, dataSourceName string) *Adapter {
	db, err := sqlx.Connect(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	a := &Adapter{
		db:        db,
		tableName: "casbin_rule",
	}
	a.ensureTable()
	// Call the destructor when the object is released.
	runtime.SetFinalizer(a, finalizer)
	return a
}

// NewAdapterByDB is the constructor for Adapter with existed connection
// Deprecated: Use NewAdapterFromOptions instead
func NewAdapterByDB(db *sqlx.DB) *Adapter {
	a := &Adapter{
		db:        db,
		tableName: "casbin_rule",
	}
	a.ensureTable()
	return a
}

// NewAdapterFromOptions is the constructor for Adapter with existed connection
func NewAdapterFromOptions(opts *AdapterOptions) *Adapter {
	a := &Adapter{tableName: "casbin_rule"}
	if opts.TableName != "" {
		a.tableName = opts.TableName
	}
	if opts.DB != nil {
		a.db = opts.DB
	} else {
		db, err := sqlx.Connect(opts.DriverName, opts.DataSourceName)
		if err != nil {
			panic(err)
		}
		a.db = db
		runtime.SetFinalizer(a, finalizer)
	}
	a.ensureTable()
	return a
}

// LoadPolicy loads policy from database.
func (a *Adapter) LoadPolicy(model model.Model) error {
	var lines []CasbinRule
	err := a.db.Select(&lines, fmt.Sprintf("SELECT * FROM %s", a.tableName))
	if err != nil {
		return err
	}
	for _, line := range lines {
		loadPolicyLine(line, model)
	}
	return nil
}

// SavePolicy saves policy to database.
func (a *Adapter) SavePolicy(model model.Model) (err error) {
	a.dropTable()
	for ptype, ast := range model["p"] {
		for _, rule := range ast.Policy {
			line := savePolicyLine(ptype, rule)
			err = a.insertPolicyLine(&line)
			if err != nil {
				return
			}
		}
	}
	for ptype, ast := range model["g"] {
		for _, rule := range ast.Policy {
			line := savePolicyLine(ptype, rule)
			err = a.insertPolicyLine(&line)
			if err != nil {
				return
			}
		}
	}
	return
}

// AddPolicy adds a policy rule to the storage.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) (err error) {
	line := savePolicyLine(ptype, rule)
	err = a.insertPolicyLine(&line)
	if err != nil {
		return
	}
	return err
}

// RemovePolicy removes a policy rule from the storage.
func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) (err error) {
	line := savePolicyLine(ptype, rule)
	err = a.deletePolicyLine(&line)
	if err != nil {
		return
	}
	return err
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) (err error) {
	line := CasbinRule{}
	line.PType = ptype
	if fieldIndex <= 0 && 0 < fieldIndex+len(fieldValues) {
		line.V0 = fieldValues[0-fieldIndex]
	}
	if fieldIndex <= 1 && 1 < fieldIndex+len(fieldValues) {
		line.V1 = fieldValues[1-fieldIndex]
	}
	if fieldIndex <= 2 && 2 < fieldIndex+len(fieldValues) {
		line.V2 = fieldValues[2-fieldIndex]
	}
	if fieldIndex <= 3 && 3 < fieldIndex+len(fieldValues) {
		line.V3 = fieldValues[3-fieldIndex]
	}
	if fieldIndex <= 4 && 4 < fieldIndex+len(fieldValues) {
		line.V4 = fieldValues[4-fieldIndex]
	}
	if fieldIndex <= 5 && 5 < fieldIndex+len(fieldValues) {
		line.V5 = fieldValues[5-fieldIndex]
	}
	err = a.rawDelete(&line)
	if err != nil {
		return
	}
	return
}

func (a *Adapter) rawDelete(line *CasbinRule) (err error) {
	queryArgs := []interface{}{line.PType}
	query := fmt.Sprintf("DELETE FROM `%s` WHERE p_type = ?", a.tableName)
	if line.V0 != "" {
		query += " AND v0 = ?"
		queryArgs = append(queryArgs, line.V0)
	}
	if line.V1 != "" {
		query += " AND v1 = ?"
		queryArgs = append(queryArgs, line.V1)
	}
	if line.V2 != "" {
		query += " AND v2 = ?"
		queryArgs = append(queryArgs, line.V2)
	}
	if line.V3 != "" {
		query += " AND v3 = ?"
		queryArgs = append(queryArgs, line.V3)
	}
	if line.V4 != "" {
		query += " AND v4 = ?"
		queryArgs = append(queryArgs, line.V4)
	}
	if line.V5 != "" {
		query += " AND v5 = ?"
		queryArgs = append(queryArgs, line.V5)
	}
	_, err = a.db.Exec(query, queryArgs...)
	if err != nil {
		return
	}
	return
}
