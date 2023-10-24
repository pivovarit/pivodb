package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"strings"
	"testing"
)

func TestEmptyDB(t *testing.T) {
	db := New()

	results, err := db.Execute(statement.Select())

	failOnError(err, t)

	if len(results) != 0 {
		t.Errorf("expected empty result, got: %d", len(results))
	}
}

func TestErrorOnFullDB(t *testing.T) {
	db := New()

	username := "pivovarit"
	email := "foo@bar.com"

	for i := 0; i < storage.TableMaxRows; i++ {
		_, _ = db.Execute(statement.Insert(storage.Row{
			Id:       uint32(i),
			Username: username,
			Email:    email,
		}))
	}
	_, err := db.Execute(statement.Insert(storage.Row{
		Id:       uint32(storage.TableMaxRows),
		Username: username,
		Email:    email,
	}))

	if err == nil {
		t.Error("Expected 'max row count reached'")
	}

	if !strings.HasPrefix(err.Error(), "max row count reached") {
		t.Errorf("Expected 'max row count reached', got: %s", err.Error())
	}
}

func TestInsertDB(t *testing.T) {
	db := New()

	id := 1
	username := "pivovarit"
	email := "foo@bar.com"

	_, _ = db.Execute(statement.Insert(storage.Row{
		Id:       uint32(id),
		Username: username,
		Email:    email,
	}))

	result, err := db.Execute(statement.Select())

	failOnError(err, t)

	if len(result) != 1 {
		t.Errorf("expected 1 result, got: %d", len(result))
	}

	var user = result[0]

	if user.Id != uint32(id) || user.Email != string(email[:]) || user.Username != string(username[:]) {
		t.Errorf("got: %s, expected: %d, %s, and %s", user.ToString(), id, username, email)
	}
}

func TestInsertMultiplePages(t *testing.T) {
	db := New()

	username := "pivovarit"
	email := "foo@bar.com"

	for i := 0; i < storage.TableMaxRows; i++ {
		_, _ = db.Execute(statement.Insert(storage.Row{
			Id:       uint32(i),
			Username: username,
			Email:    email,
		}))
	}

	results, err := db.Execute(statement.Select())

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
