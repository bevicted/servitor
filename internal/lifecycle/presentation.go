// Package lifecycle contains presentation helpers shared by the Slack operator path.
package lifecycle

import (
	"fmt"
	"time"
)

// FormatLeaseExpiry renders a UTC lease deadline and its rounded remaining hours.
func FormatLeaseExpiry(expiry, now time.Time) string {
	if expiry.IsZero() {
		return "-"
	}
	return fmt.Sprintf("%s (~%dh)", expiry.UTC().Format("2006-01-02 15:04:05 UTC"), expiry.Sub(now).Round(time.Hour)/time.Hour)
}
