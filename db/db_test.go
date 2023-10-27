package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestNoTable(t *testing.T) {
	db := newDB(t)
	table := randomTableName()

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
	db := newDB(t)
	table := randomTableName()

	_, err := db.Execute(statement.CreateTable(table))
	if errored(err, t) {
		return
	}

	results, err := db.Execute(statement.Select(table))
	if errored(err, t) {
		return
	}

	if len(results) != 0 {
		t.Errorf("expected empty result, got: %d", len(results))
	}
}

func TestErrorOnFullDB(t *testing.T) {
	db := newDB(t)
	table := randomTableName()

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

func TestSurviveRestart(t *testing.T) {
	db := New()
	table := randomTableName()
	id := 1
	username := "pivovarit"
	email := "foo@bar.com"

	_, _ = db.Execute(statement.CreateTable(table))
	_, _ = db.Execute(statement.Insert(storage.Row{
		Id:       uint32(id),
		Username: username,
		Email:    email,
	}, table))

	db.Close()
	db = newDB(t)

	result, err := db.Execute(statement.Select(table))
	if errored(err, t) {
		return
	}

	if len(result) != 1 {
		t.Errorf("expected 1 result, got: %d", len(result))
	}

	var user = result[0]

	if user.GetString("id") != strconv.Itoa(id) || user.GetString("email") != email || user.GetString("username") != username {
		t.Errorf("got: %s, expected: %d, %s, and %s", user.ToString(), id, username, email)
	}
}

func TestInsertDB(t *testing.T) {
	db := newDB(t)
	table := randomTableName()
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

	if errored(err, t) {
		return
	}

	if len(result) != 1 {
		t.Errorf("expected 1 result, got: %d", len(result))
		return
	}

	var user = result[0]

	if user.GetString("id") != strconv.Itoa(id) || user.GetString("email") != email || user.GetString("username") != username {
		t.Errorf("got: %s, expected: %d, %s, and %s", user.ToString(), id, username, email)
	}
}

func TestMultipleTableInsert(t *testing.T) {
	db := newDB(t)
	table1 := randomTableName()
	table2 := randomTableName()

	username1 := "pivovarit1"
	email1 := "foo1@bar.com"

	username2 := "pivovarit2"
	email2 := "foo2@bar.com"

	_, _ = db.Execute(statement.CreateTable(table1))
	_, _ = db.Execute(statement.CreateTable(table2))

	_, err1 := db.Execute(statement.Insert(storage.Row{
		Id:       uint32(1),
		Username: username1,
		Email:    email1,
	}, table1))

	if errored(err1, t) {
		return
	}

	_, err2 := db.Execute(statement.Insert(storage.Row{
		Id:       uint32(1),
		Username: username2,
		Email:    email2,
	}, table2))

	if errored(err2, t) {
		return
	}

	r1, _ := db.Execute(statement.Select(table1))
	r2, _ := db.Execute(statement.Select(table2))

	if len(r1) != 1 {
		t.Errorf("expected %s to have 1 row, actual: %d", table1, len(r1))
		return
	}

	if r1[0].GetString("id") != "1" || r1[0].GetString("email") != email1 || r1[0].GetString("username") != username1 {
		t.Errorf("got: %s, expected: %d, %s, and %s", r1[0].ToString(), 1, username1, email1)
		return
	}

	if len(r2) != 1 {
		t.Errorf("expected %s to have 1 row, actual: %d", table2, len(r2))
		return
	}

	if r2[0].GetString("id") != "1" || r2[0].GetString("email") != email2 || r2[0].GetString("username") != username2 {
		t.Errorf("got: %s, expected: %d, %s, and %s", r2[0].ToString(), 1, username2, email2)
	}
}

func TestInsertMultiplePages(t *testing.T) {
	db := newDB(t)
	table := randomTableName()

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

	if errored(err, t) {
		return
	}

	for idx, r := range results {
		if r.GetString("id") != strconv.Itoa(idx) || r.GetString("email") != email || r.GetString("username") != username {
			fmt.Printf("[%s][%s][equal: %t]\n", r.GetString("id"), strconv.Itoa(idx), r.GetString("id") == strconv.Itoa(idx))
			fmt.Printf("[%s][%s][equal: %t]\n", r.GetString("email"), email, r.GetString("email") == email)
			fmt.Printf("[%s][%s][equal: %t]\n", r.GetString("username"), username, r.GetString("username") == username)
			t.Errorf("got: %s, expected: %d, %s, and %s", r.ToString(), idx, username, email)
			break
		}
	}
}

func errored(err error, t *testing.T) bool {
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return true
	}
	return false
}

func newDB(t *testing.T) *DB {
	t.Cleanup(func() {
		entries, err := os.ReadDir("./")
		if err != nil {
			log.Fatal(err)
			return
		}
		for _, entry := range entries {
			if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), storage.DbFileNamePrefix) {
				_ = os.Remove(entry.Name())
			}
		}
	})

	return New()
}

func randomTableName() string {
	return "users_" + strconv.Itoa(rand.Intn(100))
}
