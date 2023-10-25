package storage

import (
	"reflect"
	"testing"
)

func TestSerializationDeserialization(t *testing.T) {
	row := Row{
		Id:       1,
		Username: "username",
		Email:    "email@email.com",
	}

	serializedRow := Serialize(row)
	deserializedRow := Deserialize(&serializedRow)

	if !reflect.DeepEqual(row, deserializedRow) {
		t.Errorf("Mismatch in serialization/deserialization. Got: %+v, want: %+v.", deserializedRow, row)
	}
}
