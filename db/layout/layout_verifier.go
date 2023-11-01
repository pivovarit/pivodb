package layout

import (
	"github.com/pivovarit/pivodb/db/storage"
	"reflect"
)

func VerifyLayout() {
	if storage.IdSize != reflect.TypeOf(uint32(1)).Size() {
		panic("IdSize not equal to reflect.TypeOf(uint32(1)).Size()")
	}
}
