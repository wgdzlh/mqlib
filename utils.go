package mqlib

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var opaque int64

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

func getUnitName() string {
	return strconv.FormatInt(atomic.AddInt64(&opaque, 1), 10)
}
