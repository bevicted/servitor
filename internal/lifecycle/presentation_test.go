package lifecycle

import (
	"testing"
	"time"
)

func TestFormatLeaseExpiryRoundsRelativeHours(t *testing.T) {
	expiry := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	for _, test := range []struct {
		name string
		now  time.Time
		want string
	}{
		{name: "below half hour", now: expiry.Add(-29 * time.Minute), want: "2026-09-07 15:29:13 UTC (~0h)"},
		{name: "above half hour", now: expiry.Add(-31 * time.Minute), want: "2026-09-07 15:29:13 UTC (~1h)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := FormatLeaseExpiry(expiry, test.now); got != test.want {
				t.Fatalf("FormatLeaseExpiry() = %q, want %q", got, test.want)
			}
		})
	}
}
