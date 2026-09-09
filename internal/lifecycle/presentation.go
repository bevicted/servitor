// Package lifecycle contains presentation helpers shared by the Slack operator path.
package lifecycle

import (
	"fmt"
	"time"
)

// FormatLeaseExpiry renders a UTC lease deadline and remaining time.
func FormatLeaseExpiry(expiry, now time.Time) string {
	if expiry.IsZero() {
		return "unavailable"
	}
	remaining := expiry.Sub(now)
	if remaining <= 0 {
		return fmt.Sprintf("%s (expired)", expiry.UTC().Format("2006-01-02 15:04:05 UTC"))
	}
	if remaining < time.Hour {
		minutes := remaining / time.Minute
		if minutes == 0 {
			return fmt.Sprintf("%s (<1m)", expiry.UTC().Format("2006-01-02 15:04:05 UTC"))
		}
		return fmt.Sprintf("%s (%dm)", expiry.UTC().Format("2006-01-02 15:04:05 UTC"), minutes)
	}
	return fmt.Sprintf("%s (~%dh)", expiry.UTC().Format("2006-01-02 15:04:05 UTC"), remaining.Round(time.Hour)/time.Hour)
}
