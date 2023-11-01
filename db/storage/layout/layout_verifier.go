package layout

import (
	"reflect"
)

func VerifyLayout() {
	if IdSize != reflect.TypeOf(uint32(1)).Size() {
		panic("IdSize not equal to reflect.TypeOf(uint32(1)).Size()")
	}
}
