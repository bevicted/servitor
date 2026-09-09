package lifecycle

import (
	"testing"
	"time"
)

func TestFormatLeaseExpiryRendersPersistedDeadlineAndBoundaries(t *testing.T) {
	expiry := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	for _, test := range []struct {
		name string
		now  time.Time
		want string
	}{
		{name: "unavailable", now: expiry, want: "unavailable"},
		{name: "below one minute", now: expiry.Add(-30 * time.Second), want: "2026-09-07 15:29:13 UTC (<1m)"},
		{name: "below one hour", now: expiry.Add(-59*time.Minute - 59*time.Second), want: "2026-09-07 15:29:13 UTC (59m)"},
		{name: "one hour", now: expiry.Add(-time.Hour), want: "2026-09-07 15:29:13 UTC (~1h)"},
		{name: "at expiry", now: expiry, want: "2026-09-07 15:29:13 UTC (expired)"},
		{name: "after expiry", now: expiry.Add(time.Second), want: "2026-09-07 15:29:13 UTC (expired)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			deadline := expiry
			if test.name == "unavailable" {
				deadline = time.Time{}
			}
			if got := FormatLeaseExpiry(deadline, test.now); got != test.want {
				t.Fatalf("FormatLeaseExpiry() = %q, want %q", got, test.want)
			}
		})
	}
}
