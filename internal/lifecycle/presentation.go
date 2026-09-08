package lifecycle

import (
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/state"
	"github.com/bevicted/servitor/internal/terraformview"
)

const (
	maxSlackMessage = 3000
	safeCellLimit   = 160
)

func reviewText(request command.CreateRequest, plan terraformview.Plan) []string {
	clusterName := plannedClusterName(plan)
	requestRows := [][]string{
		{"Name:", clusterName},
		{"Target:", request.Target},
		{"Platform:", platformLabel(request.Platform) + " " + request.Version},
		{"Provider:", providerLabel(request.Provider)},
		{"Location:", request.Location},
		{"Resource group:", request.ResourceGroup},
		{"Worker:", workerDescription(request)},
		{"Network:", networkDescription(request)},
	}
	chunks := tableChunks("Cluster request", nil, requestRows, "")
	create, change, destroy := actionTotals(plan.Resources)
	prefix := fmt.Sprintf("Plan: %d create, %d change, %d destroy\nPlanned resources:", create, change, destroy)
	rows := make([][]string, 0, len(plan.Resources))
	for _, resource := range plan.Resources {
		rows = append(rows, []string{resource.Role() + ":", actionLabel(resource.Actions)})
	}
	instruction := "Reply with exact `yes` in this thread within five minutes, or `no` to decline."
	chunks = append(chunks, tableChunks(prefix, []string{"Resource", "Action"}, rows, "\n"+instruction)...)
	return chunks
}

func readyTexts(view terraformview.State, expiry, now time.Time) []string {
	created, reused := make([][]string, 0, len(view.Resources)), make([][]string, 0, len(view.Resources))
	for _, resource := range view.Resources {
		row := []string{resource.Role(), resource.DisplayName, resource.ID}
		if resource.Reused {
			reused = append(reused, row)
		} else {
			created = append(created, row)
		}
	}
	header := []string{"Resource", "Name", "ID"}
	chunks := tableChunks("Your cluster is ready.\n\nCreated", header, created, "")
	conclusion := "\nThis lease will expire at " + FormatLeaseExpiry(expiry, now) + ".\nReply with `done` in this thread or send `@servitor done` in the configured channel to free up your resources sooner."
	chunks = append(chunks, tableChunks("Reused", header, reused, conclusion)...)
	return chunks
}

func tableChunks(title string, header []string, rows [][]string, conclusion string) []string {
	if len(rows) == 0 {
		return []string{title + "\n" + tableBlock(header, nil) + conclusion}
	}
	chunks := make([]string, 0, 1)
	current := make([][]string, 0, len(rows))
	for _, row := range rows {
		candidate := append(append([][]string(nil), current...), row)
		text := title + "\n" + tableBlock(header, candidate)
		if len(current) != 0 && len(text)+len(conclusion) > maxSlackMessage {
			chunks = append(chunks, title+"\n"+tableBlock(header, current))
			current = current[:0]
		}
		current = append(current, row)
	}
	chunks = append(chunks, title+"\n"+tableBlock(header, current)+conclusion)
	return chunks
}

func tableBlock(header []string, rows [][]string) string {
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)
	if len(header) != 0 {
		writeTableRow(writer, header)
	}
	for _, row := range rows {
		writeTableRow(writer, row)
	}
	_ = writer.Flush()
	return "```\n" + strings.TrimSuffix(buffer.String(), "\n") + "\n```"
}

func writeTableRow(writer *tabwriter.Writer, row []string) {
	for index, value := range row {
		if index != 0 {
			_, _ = writer.Write([]byte{'\t'})
		}
		_, _ = writer.Write([]byte(safeCell(value)))
	}
	_, _ = writer.Write([]byte{'\n'})
}

func safeCell(value string) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) || character == '`' || character == '@' || character == '<' || character == '>' {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "-"
	}
	if len([]rune(value)) > safeCellLimit {
		return string([]rune(value)[:safeCellLimit-3]) + "..."
	}
	return value
}

func existingAllocationNotice(record state.LifecycleRecord, now time.Time) string {
	rows := make([][]string, 0, 3)
	if record.ClusterName != "" {
		rows = append(rows, []string{"Cluster:", record.ClusterName})
	}
	rows = append(rows, []string{"State:", record.Status})
	if !record.LeaseExpiresAt.IsZero() {
		rows = append(rows, []string{"Expires:", FormatLeaseExpiry(record.LeaseExpiresAt, now)})
	}
	return "You already have resources allocated.\n" + tableBlock(nil, rows) + "\nUse `@servitor list` for status or `@servitor done` to clean up."
}

// FormatLeaseExpiry renders a UTC lease deadline and its rounded remaining hours.
func FormatLeaseExpiry(expiry, now time.Time) string {
	if expiry.IsZero() {
		return "-"
	}
	return fmt.Sprintf("%s (~%dh)", expiry.UTC().Format("2006-01-02 15:04:05 UTC"), expiry.Sub(now).Round(time.Hour)/time.Hour)
}

func extensionSuccessText(result ExtensionResult, now time.Time) string {
	return fmt.Sprintf("Lease extended.\n\n```\nPrevious expiry: %s\nNew expiry:      %s\nAdded:           %s\n```\nRemaining lease time is capped at 24 hours.", FormatLeaseExpiry(result.OldExpiry, now), FormatLeaseExpiry(result.NewExpiry, now), formatAddedDuration(result.Added))
}

func formatAddedDuration(added time.Duration) string {
	switch {
	case added <= 0:
		return "-"
	case added < time.Hour:
		return "<1h"
	case added%time.Hour == 0:
		return fmt.Sprintf("%dh", added/time.Hour)
	default:
		return fmt.Sprintf("~%dh", added.Round(time.Hour)/time.Hour)
	}
}

func plannedClusterName(plan terraformview.Plan) string {
	for _, resource := range plan.Resources {
		if resource.Role() == "Cluster" && resource.DisplayName != "" {
			return resource.DisplayName
		}
	}
	return "-"
}

func platformLabel(platform string) string {
	switch platform {
	case "kubernetes":
		return "Kubernetes"
	case "openshift":
		return "OpenShift"
	default:
		return "Platform"
	}
}

func providerLabel(provider string) string {
	switch provider {
	case "vpc-gen2":
		return "VPC Gen 2"
	case "classic":
		return "Classic"
	case "satellite":
		return "Satellite"
	default:
		return "Provider"
	}
}

func workerDescription(request command.CreateRequest) string {
	if request.WorkerCount == "" {
		return request.WorkerShape
	}
	return request.WorkerCount + " x " + request.WorkerShape
}

func networkDescription(request command.CreateRequest) string {
	switch request.Provider {
	case "classic":
		if request.PublicVLANID != "" || request.PrivateVLANID != "" {
			return "reuse Classic VLANs"
		}
		return "Classic networking"
	case "satellite":
		if request.SatelliteLocationID != "" {
			return "reuse Satellite location " + request.SatelliteLocationID
		}
		return "Satellite networking"
	}
	parts := make([]string, 0, 3)
	if request.ReuseVPC {
		parts = append(parts, "reuse VPC "+request.VPCID)
	} else {
		parts = append(parts, "create VPC")
	}
	if request.ReuseSubnet {
		parts = append(parts, "reuse subnet")
	} else {
		parts = append(parts, "create subnet")
	}
	if request.ReuseGateway {
		parts = append(parts, "reuse gateway")
	} else {
		parts = append(parts, "create gateway")
	}
	return strings.Join(parts, "; ")
}

func actionTotals(resources []terraformview.Resource) (create, change, destroy int) {
	for _, resource := range resources {
		for _, action := range resource.Actions {
			switch action {
			case "create":
				create++
			case "delete":
				destroy++
			case "update":
				change++
			}
		}
	}
	return create, change, destroy
}

func actionLabel(actions []string) string {
	if len(actions) == 2 && actions[0] == "delete" && actions[1] == "create" {
		return "replace"
	}
	return strings.Join(actions, "/")
}
