package storage

import (
	"github.com/pivovarit/pivodb/db/serializer"
	"github.com/pivovarit/pivodb/db/storage/layout"
)

func Serialize(row Row) [layout.RowSize]byte {
	var serialized [layout.RowSize]byte
	copy(serialized[layout.IdOffset:layout.UsernameOffset], serializer.WriteUint32(row.Id))
	copy(serialized[layout.UsernameOffset:layout.EmailOffset], serializer.WriteString(row.Username))
	copy(serialized[layout.EmailOffset:], serializer.WriteString(row.Email))
	return serialized
}

func Deserialize(row [layout.RowSize]byte) Row {
	var id [layout.IdSize]byte
	var username [layout.UsernameSize]byte
	var email [layout.EmailSize]byte

	copy(id[:], row[layout.IdOffset:layout.UsernameOffset])
	copy(username[:], row[layout.UsernameOffset:layout.EmailOffset])
	copy(email[:], row[layout.EmailOffset:])

	return Row{
		Id:       serializer.ReadUint32(id),
		Username: serializer.ReadString(username[:]),
		Email:    serializer.ReadString(email[:]),
	}
}
