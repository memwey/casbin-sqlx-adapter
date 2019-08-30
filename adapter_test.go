package sqlxadapter

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/util"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var (
	driverName     = "mysql"
	dataSourceName = "root:mariadb@tcp(127.0.0.1:3306)/abc"
)

func testGetPolicy(t *testing.T, e *casbin.Enforcer, res [][]string) {
	t.Helper()
	myRes := e.GetPolicy()
	log.Print("Policy: ", myRes)

	if !util.Array2DEquals(res, myRes) {
		t.Error("Policy: ", myRes, ", supposed to be ", res)
	}
}

func initPolicy(t *testing.T) {

	var err error

	// Because the DB is empty at first,
	// so we need to load the policy from the file adapter (.CSV) first.
	e, err := casbin.NewEnforcer("examples/rbac_model.conf", "examples/rbac_policy.csv")
	if err != nil {
		t.Fatal(err)
	}

	a := NewAdapter(driverName, dataSourceName)
	// This is a trick to save the current policy to the DB.
	// We can't call e.SavePolicy() because the adapter in the enforcer is still the file adapter.
	// The current policy means the policy in the Casbin enforcer (aka in memory).
	err = a.SavePolicy(e.GetModel())
	if err != nil {
		panic(err)
	}

	// Clear the current policy.
	e.ClearPolicy()
	testGetPolicy(t, e, [][]string{})

	// Load the policy from DB.
	err = a.LoadPolicy(e.GetModel())
	if err != nil {
		panic(err)
	}
	testGetPolicy(t, e, [][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}, {"data2_admin", "data2", "read"}, {"data2_admin", "data2", "write"}})
}

func testSaveLoad(t *testing.T) {
	// Initialize some policy in DB.
	initPolicy(t)
	// Note: you don't need to look at the above code
	// if you already have a working DB with policy inside.

	// Now the DB has policy, so we can provide a normal use case.
	// Create an adapter and an enforcer.
	// NewEnforcer() will load the policy automatically.
	a := NewAdapter(driverName, dataSourceName)
	e, err := casbin.NewEnforcer("examples/rbac_model.conf", a)
	if err != nil {
		t.Fatal(err)
	}
	testGetPolicy(t, e, [][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}, {"data2_admin", "data2", "read"}, {"data2_admin", "data2", "write"}})
}

func testAutoSave(t *testing.T) {
	// Initialize some policy in DB.
	initPolicy(t)
	// Note: you don't need to look at the above code
	// if you already have a working DB with policy inside.

	// Now the DB has policy, so we can provide a normal use case.
	// Create an adapter and an enforcer.
	// NewEnforcer() will load the policy automatically.
	a := NewAdapter(driverName, dataSourceName)
	e, err := casbin.NewEnforcer("examples/rbac_model.conf", a)
	if err != nil {
		t.Fatal(err)
	}

	// AutoSave is enabled by default.
	// Now we disable it.
	e.EnableAutoSave(false)

	// Because AutoSave is disabled, the policy change only affects the policy in Casbin enforcer,
	// it doesn't affect the policy in the storage.
	e.AddPolicy("alice", "data1", "write")
	// Reload the policy from the storage to see the effect.
	e.LoadPolicy()
	// This is still the original policy.
	testGetPolicy(t, e, [][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}, {"data2_admin", "data2", "read"}, {"data2_admin", "data2", "write"}})

	// Now we enable the AutoSave.
	e.EnableAutoSave(true)

	// Because AutoSave is enabled, the policy change not only affects the policy in Casbin enforcer,
	// but also affects the policy in the storage.
	e.AddPolicy("alice", "data1", "write")
	// Reload the policy from the storage to see the effect.
	e.LoadPolicy()
	// The policy has a new rule: {"alice", "data1", "write"}.
	testGetPolicy(t, e, [][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}, {"data2_admin", "data2", "read"}, {"data2_admin", "data2", "write"}, {"alice", "data1", "write"}})

	// Remove the added rule.
	e.RemovePolicy("alice", "data1", "write")
	e.LoadPolicy()
	testGetPolicy(t, e, [][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}, {"data2_admin", "data2", "read"}, {"data2_admin", "data2", "write"}})

	// Remove "data2_admin" related policy rules via a filter.
	// Two rules: {"data2_admin", "data2", "read"}, {"data2_admin", "data2", "write"} are deleted.
	e.RemoveFilteredPolicy(0, "data2_admin")
	e.LoadPolicy()
	testGetPolicy(t, e, [][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}})

}

func TestAdapters(t *testing.T) {
	setupDatabase(t)
	testSaveLoad(t)
	testAutoSave(t)
}

// Make sure the initial casbin_rule table exists
func setupDatabase(t *testing.T) {
	migration, err := ioutil.ReadFile("examples/casbin_rule.sql")
	if err != nil {
		t.Fatalf("failed to load casbin_rule sql migration: %s", err)
	}

	db, err := sqlx.Connect(driverName, dataSourceName)
	if err != nil {
		t.Fatalf("failed to connect to database: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(string(migration))
	if err != nil {
		t.Fatalf("failed to run casbin_rule sql migration: %s", err)
	}
}
