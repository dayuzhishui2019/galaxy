package uuid

import (
	"github.com/satori/go.uuid"
	"strings"
)

func UUID() string {
	return uuid.NewV4().String()
}
func UUIDShort() string {
	return strings.ReplaceAll(uuid.NewV4().String(), "-", "")
}
