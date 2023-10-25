package storage

import "github.com/pivovarit/pivodb/db/serializer"

func Serialize(row Row) [RowSize]byte {
	var serialized [RowSize]byte
	copy(serialized[IdOffset:UsernameOffset], serializer.WriteUint32(row.Id))
	copy(serialized[UsernameOffset:EmailOffset], serializer.WriteString(row.Username))
	copy(serialized[EmailOffset:], serializer.WriteString(row.Email))
	return serialized
}

func Deserialize(row *[RowSize]byte) Row {
	var id [IdSize]byte
	var username [UsernameSize]byte
	var email [EmailSize]byte

	copy(id[:], row[IdOffset:UsernameOffset])
	copy(username[:], row[UsernameOffset:EmailOffset])
	copy(email[:], row[EmailOffset:])

	return Row{
		Id:       serializer.ReadUint32(id),
		Username: serializer.ReadString(username[:]),
		Email:    serializer.ReadString(email[:]),
	}
}
