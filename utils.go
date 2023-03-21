package mqlib

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/google/uuid"
)

func GetUniqKey() string {
	uid := uuid.New()
	return base64.RawURLEncoding.EncodeToString(uid[:])
}

func GetTTLFromContext(ctx context.Context) (ttl time.Duration) {
	due, ok := ctx.Deadline()
	if ok {
		now := time.Now()
		if now.Before(due) {
			ttl = due.Sub(now)
		}
	}
	return
}
