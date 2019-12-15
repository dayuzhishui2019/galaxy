package util

import (
	"os"
	"strings"
)

func GetAppPath() string {
	return os.Args[0][:(strings.LastIndex(os.Args[0], string(os.PathSeparator)) + 1)]
}