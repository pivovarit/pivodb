package layout

const (
	IdSize         = 4
	UsernameSize   = 32
	EmailSize      = 255
	RowSize        = IdSize + UsernameSize + EmailSize
	IdOffset       = 0
	UsernameOffset = IdOffset + IdSize
	EmailOffset    = UsernameOffset + UsernameSize
)
