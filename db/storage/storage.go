package storage

const (
	TableName = "users"
	IdSize = 4
	UsernameSize = 32
	EmailSize = 255
	IdOffset = 0
	UsernameOffset = IdOffset + IdSize
	EmailOffset = UsernameOffset + UsernameSize
)

type Row struct {
	Id       uint32
	Username [UsernameSize]byte
	Email    [EmailSize]byte
}

type Table struct {
	Rows []Row
}
