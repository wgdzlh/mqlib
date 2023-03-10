package mqlib

import (
	"encoding/base64"

	"github.com/google/uuid"
)

func getUniqKey() string {
	uid := uuid.New()
	return base64.RawURLEncoding.EncodeToString(uid[:])
}
