package lifecycle

import (
	"strings"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/state"
	"github.com/bevicted/servitor/internal/terraformview"
)

func TestReviewTextIsWhitelistedSanitizedAndChunked(t *testing.T) {
	request := command.CreateRequest{
		Target: "synthetic-target", Platform: "kubernetes", Version: "1.36", Provider: "vpc-gen2",
		ResourceGroup: "Default", WorkerShape: "bx2.2x8", WorkerCount: "1", Location: "us-south/us-south-1",
		VPCID: "shared\t<@U1>\n<!channel>\r\n```", ReuseVPC: true,
	}
	resources := make([]terraformview.Resource, 0, 240)
	for index := 0; index < 240; index++ {
		resources = append(resources, terraformview.Resource{
			Address: "module.private[\"secret\"].ibm_container_vpc_cluster.cluster",
			Type:    "ibm_container_vpc_cluster", DisplayName: "servitor-<!here>\n```", Actions: []string{"create"},
		})
	}
	chunks := reviewText(request, terraformview.Plan{Resources: resources})
	if len(chunks) < 3 {
		t.Fatalf("review chunks = %d, want split output", len(chunks))
	}
	all := strings.Join(chunks, "\n")
	visible := strings.ReplaceAll(all, "```", "")
	for _, forbidden := range []string{"module.private", "secret", "<@", "<!", "\t"} {
		if strings.Contains(visible, forbidden) {
			t.Fatalf("review exposed %q: %q", forbidden, all)
		}
	}
	if !strings.Contains(all, "Plan: 240 create, 0 change, 0 destroy") || !strings.Contains(all, "VPC Gen 2") || !strings.Contains(all, "us-south/us-south-1") {
		t.Fatalf("review omitted normalized summary: %q", all)
	}
	for index, chunk := range chunks {
		if len(chunk) > maxSlackMessage || strings.Count(chunk, "```") != 2 {
			t.Fatalf("chunk %d is not a bounded balanced table: %q", index, chunk)
		}
		if index != len(chunks)-1 && strings.Contains(chunk, "Reply with exact `yes`") {
			t.Fatalf("non-final chunk contains confirmation instruction: %q", chunk)
		}
		if strings.Contains(chunk, "Planned resources:") && (!strings.Contains(chunk, "Resource") || !strings.Contains(chunk, "Action")) {
			t.Fatalf("resource chunk %d does not repeat its header: %q", index, chunk)
		}
	}
	if !strings.HasSuffix(chunks[len(chunks)-1], "Reply with exact `yes` in this thread within five minutes, or `no` to decline.") {
		t.Fatalf("final review instruction = %q", chunks[len(chunks)-1])
	}
}

func TestReviewTextUsesProviderNeutralLocationsAndRoles(t *testing.T) {
	plan := terraformview.Plan{Resources: []terraformview.Resource{{Type: "ibm_container_cluster", DisplayName: "cluster", Actions: []string{"create"}}, {Type: "ibm_satellite_host", Actions: []string{"create"}}}}
	for _, test := range []struct {
		name    string
		request command.CreateRequest
		wanted  []string
	}{
		{"VPC Kubernetes", command.CreateRequest{Platform: "kubernetes", Version: "1.36", Provider: "vpc-gen2", Location: "us-south/us-south-1", WorkerShape: "bx2.2x8"}, []string{"Kubernetes 1.36", "us-south/us-south-1", "VPC Gen 2"}},
		{"Classic OpenShift", command.CreateRequest{Platform: "openshift", Version: "4.22", Provider: "classic", Location: "dal10", WorkerShape: "b3c.4x16"}, []string{"OpenShift 4.22", "dal10", "Classic networking"}},
		{"Satellite", command.CreateRequest{Platform: "openshift", Version: "4.22", Provider: "satellite", Location: "us-south-1, us-south-2", SatelliteLocationID: "location", WorkerShape: "bx2.4x16"}, []string{"Satellite", "us-south-1, us-south-2", "reuse Satellite location location", "Satellite Host"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			text := strings.Join(reviewText(test.request, plan), "\n")
			for _, wanted := range test.wanted {
				if !strings.Contains(text, wanted) {
					t.Fatalf("review omitted %q: %q", wanted, text)
				}
			}
		})
	}
}

func TestReviewTextChunksOversizedRequestRows(t *testing.T) {
	long := strings.Repeat("😀", safeCellLimit)
	request := command.CreateRequest{
		Target: long, Platform: "kubernetes", Version: long, Provider: "vpc-gen2",
		ResourceGroup: long, WorkerShape: long, WorkerCount: long, Location: long,
		VPCID: long, ReuseVPC: true,
	}
	chunks := reviewText(request, terraformview.Plan{Resources: []terraformview.Resource{{Type: "ibm_container_vpc_cluster", DisplayName: "cluster", Actions: []string{"create"}}}})
	requestChunks := 0
	for index, chunk := range chunks {
		if len(chunk) > maxSlackMessage || strings.Count(chunk, "```") != 2 {
			t.Fatalf("chunk %d is not a bounded balanced table: %q", index, chunk)
		}
		if strings.HasPrefix(chunk, "Cluster request\n") {
			requestChunks++
		}
	}
	if requestChunks < 2 {
		t.Fatalf("request chunks = %d, want split request table", requestChunks)
	}
}

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

func TestExtensionSuccessTextFormatsAddedDuration(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		result ExtensionResult
		want   string
	}{
		{
			name: "8.5 second clamped addition",
			result: ExtensionResult{
				OldExpiry: now.Add(24*time.Hour - 8*time.Second - 500*time.Millisecond),
				NewExpiry: now.Add(24 * time.Hour),
				Added:     8*time.Second + 500*time.Millisecond,
				Clamped:   true,
			},
			want: "Lease extended.\n\n```\nPrevious expiry: 2026-09-05 11:59:51 UTC (~24h)\nNew expiry:      2026-09-05 12:00:00 UTC (~24h)\nAdded:           <1h\n```\nRemaining lease time is capped at 24 hours.",
		},
		{
			name: "non-hour addition above one hour",
			result: ExtensionResult{
				OldExpiry: now.Add(4 * time.Hour),
				NewExpiry: now.Add(8*time.Hour + 31*time.Minute),
				Added:     4*time.Hour + 31*time.Minute,
			},
			want: "Lease extended.\n\n```\nPrevious expiry: 2026-09-04 16:00:00 UTC (~4h)\nNew expiry:      2026-09-04 20:31:00 UTC (~9h)\nAdded:           ~5h\n```\nRemaining lease time is capped at 24 hours.",
		},
		{
			name: "exact hours",
			result: ExtensionResult{
				OldExpiry: now.Add(4 * time.Hour),
				NewExpiry: now.Add(8 * time.Hour),
				Added:     4 * time.Hour,
			},
			want: "Lease extended.\n\n```\nPrevious expiry: 2026-09-04 16:00:00 UTC (~4h)\nNew expiry:      2026-09-04 20:00:00 UTC (~8h)\nAdded:           4h\n```\nRemaining lease time is capped at 24 hours.",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := extensionSuccessText(test.result, now); got != test.want {
				t.Fatalf("extensionSuccessText() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestExistingAllocationNoticeIncludesFormattedExpiry(t *testing.T) {
	expiry := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	text := existingAllocationNotice(state.LifecycleRecord{Status: statusReady, LeaseExpiresAt: expiry}, expiry.Add(-4*time.Hour))
	if !strings.Contains(text, "Expires:  2026-09-07 15:29:13 UTC (~4h)") {
		t.Fatalf("existing allocation notice = %q", text)
	}
}

func TestReadyTextsSeparatesCreatedAndReusedWithoutRequestConfiguration(t *testing.T) {
	expiry := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	chunks := readyTexts(terraformview.State{Resources: []terraformview.Resource{
		{Type: "ibm_container_vpc_cluster", DisplayName: "servitor-<!channel>\n```", ID: "cluster-id"},
		{Type: "ibm_is_vpc", DisplayName: "shared\n<@U1>", ID: "vpc-<!here>\r\n```", Reused: true},
	}}, expiry, expiry.Add(-4*time.Hour))
	all := strings.Join(chunks, "\n")
	for _, wanted := range []string{"Your cluster is ready.", "Created", "Reused", "Resource", "Name", "ID", "This lease will expire at 2026-09-07 15:29:13 UTC (~4h).", "Reply with `done` in this thread or send `@servitor done` in the configured channel to free up your resources sooner."} {
		if !strings.Contains(all, wanted) {
			t.Fatalf("ready output omitted %q: %q", wanted, all)
		}
	}
	for _, forbidden := range []string{"Target:", "Platform:", "Provider:", "<@", "<!", "\n<@"} {
		if strings.Contains(all, forbidden) {
			t.Fatalf("ready output exposed %q: %q", forbidden, all)
		}
	}
	for index, chunk := range chunks {
		if len(chunk) > maxSlackMessage || strings.Count(chunk, "```") != 2 {
			t.Fatalf("chunk %d is not a bounded balanced table: %q", index, chunk)
		}
	}
}

func TestReadyTextsChunksOversizedTablesAtRowsWithRepeatedHeaders(t *testing.T) {
	expiry := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	resources := make([]terraformview.Resource, 0, 40)
	for index := 0; index < 40; index++ {
		resources = append(resources, terraformview.Resource{
			Type:        "ibm_container_vpc_cluster",
			DisplayName: strings.Repeat("n", safeCellLimit),
			ID:          strings.Repeat("i", safeCellLimit),
		})
	}
	chunks := readyTexts(terraformview.State{Resources: resources}, expiry, expiry.Add(-4*time.Hour))
	if len(chunks) < 2 {
		t.Fatalf("ready chunks = %d, want split output", len(chunks))
	}
	for index, chunk := range chunks {
		if len(chunk) > maxSlackMessage || strings.Count(chunk, "```") != 2 {
			t.Fatalf("chunk %d is not a bounded balanced table: %q", index, chunk)
		}
		if !strings.Contains(chunk, "Resource") || !strings.Contains(chunk, "Name") || !strings.Contains(chunk, "ID") {
			t.Fatalf("chunk %d does not repeat its header: %q", index, chunk)
		}
	}
}
