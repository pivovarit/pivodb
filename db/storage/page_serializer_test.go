package storage

import (
	"reflect"
	"testing"
)

func TestSerializeAndDeserializePage(t *testing.T) {
	originalPage := NewPage()

	r1 := Row{
		Id:       42,
		Username: "user",
		Email:    "email",
	}

	serializedRow := Serialize(r1)
	originalPage.Rows[0] = &serializedRow
	serialized := SerializePage(originalPage)
	deserializedPage := DeserializePage(serialized)

	if !reflect.DeepEqual(originalPage, deserializedPage) {
		t.Fatalf("Page not the same after serialize and deserialize. got=%#v, want=%#v", deserializedPage, originalPage)
	}
}
