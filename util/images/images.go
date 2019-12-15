package images

import "encoding/base64"

func EncodeBase64(bytes []byte) string {
	return base64.StdEncoding.EncodeToString(bytes)
}

func DecodeBase64(base64Str string) (bytes []byte, err error) {
	return base64.StdEncoding.DecodeString(base64Str)
}
