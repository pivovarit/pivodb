package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"strings"
	"testing"
)

func TestNoTable(t *testing.T) {
	db := New()
	table := "42"

	_, err := db.Execute(statement.Select(table))

	if err == nil {
		t.Errorf("expected 'table [%s] does not exist' error", table)
	}

	expected := fmt.Sprintf("table [%s] does not exist", table)
	if expected != err.Error() {
		t.Errorf("Expected '%s', got: %s", expected, err.Error())
	}
}

func TestEmptyDB(t *testing.T) {
	db := New()
	table := "users"

	_, err := db.Execute(statement.CreateTable(table))
	failOnError(err, t)

	results, err := db.Execute(statement.Select(table))
	failOnError(err, t)

	if len(results) != 0 {
		t.Errorf("expected empty result, got: %d", len(results))
	}
}

func TestErrorOnFullDB(t *testing.T) {
	db := New()
	table := "users"

	username := "pivovarit"
	email := "foo@bar.com"

	_, _ = db.Execute(statement.CreateTable(table))
	for i := 0; i < storage.TableMaxRows; i++ {
		_, _ = db.Execute(statement.Insert(storage.Row{
			Id:       uint32(i),
			Username: username,
			Email:    email,
		}, table))
	}
	_, err := db.Execute(statement.Insert(storage.Row{
		Id:       uint32(storage.TableMaxRows),
		Username: username,
		Email:    email,
	}, table))

	if err == nil {
		t.Error("Expected 'max row count reached'")
	}

	if !strings.HasPrefix(err.Error(), "max row count reached") {
		t.Errorf("Expected 'max row count reached', got: %s", err.Error())
	}
}

func TestInsertDB(t *testing.T) {
	db := New()
	table := "users"
	id := 1
	username := "pivovarit"
	email := "foo@bar.com"

	_, _ = db.Execute(statement.CreateTable(table))
	_, _ = db.Execute(statement.Insert(storage.Row{
		Id:       uint32(id),
		Username: username,
		Email:    email,
	}, table))

	result, err := db.Execute(statement.Select(table))

	failOnError(err, t)

	if len(result) != 1 {
		t.Errorf("expected 1 result, got: %d", len(result))
	}

	var user = result[0]

	if user.Id != uint32(id) || user.Email != email || user.Username != username {
		t.Errorf("got: %s, expected: %d, %s, and %s", user.ToString(), id, username, email)
	}
}

func TestInsertMultiplePages(t *testing.T) {
	db := New()
	table := "users"

	username := "pivovarit"
	email := "foo@bar.com"

	_, _ = db.Execute(statement.CreateTable(table))
	for i := 0; i < storage.TableMaxRows; i++ {
		_, _ = db.Execute(statement.Insert(storage.Row{
			Id:       uint32(i),
			Username: username,
			Email:    email,
		}, table))
	}

	results, err := db.Execute(statement.Select(table))

	failOnError(err, t)

	for idx, r := range results {
		if r.Id != uint32(idx) || r.Email != email || r.Username != username {
			fmt.Printf("[%d][%d][equal: %t]\n", r.Id, uint32(idx), r.Id == uint32(idx))
			fmt.Printf("[%s][%s][equal: %t]\n", r.Email, email, r.Email == email)
			fmt.Printf("[%s][%s][equal: %t]\n", r.Username, username, r.Username == username)
			t.Errorf("got: %s, expected: %d, %s, and %s", r.ToString(), idx, username, email)
			break
		}
	}
}

func failOnError(err error, t *testing.T) {
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}
