package column

const UsernameSize = 32
const EmailSize = 255

type Row struct {
	Id       uint32
	Username [UsernameSize]byte
	Email    [EmailSize]byte
}
