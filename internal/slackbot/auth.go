package slackbot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/slack-go/slack"
)

// AuthDelivery adapts Slack's external-upload API for owner-only bundle delivery.
type AuthDelivery struct{ client *slack.Client }

func NewAuthDelivery(client *slack.Client) *AuthDelivery { return &AuthDelivery{client: client} }

// DeliverAuthBundle opens the owner's DM and completes one external upload for
// the exact stored public or VPN bundle. It deliberately has no logging path
// because its input is credential-bearing.
func (d *AuthDelivery) DeliverAuthBundle(ctx context.Context, ownerID, clusterName, mode, expiry string, kubeconfig, vpn []byte) error {
	if d == nil || d.client == nil {
		return errors.New("Slack auth delivery is not configured")
	}
	if ownerID == "" || len(kubeconfig) == 0 || (mode == "public" && (expiry != "" || len(vpn) != 0)) || (mode == "vpn" && (expiry == "" || len(vpn) == 0)) || (mode != "public" && mode != "vpn") {
		return errors.New("invalid Slack auth delivery request")
	}
	dm, _, _, err := d.client.OpenConversationContext(ctx, &slack.OpenConversationParameters{Users: []string{ownerID}, ReturnIM: true})
	if err != nil {
		return fmt.Errorf("open owner DM: %w", err)
	}
	if dm == nil || dm.ID == "" {
		return errors.New("open owner DM returned no channel")
	}
	files := make([]slack.FileSummary, 0, 2)
	for _, file := range []struct {
		name string
		data []byte
	}{{name: "kubeconfig.yaml", data: kubeconfig}, {name: "client.ovpn", data: vpn}} {
		if len(file.data) == 0 {
			continue
		}
		upload, err := d.client.GetUploadURLExternalContext(ctx, slack.GetUploadURLExternalParameters{FileName: file.name, FileSize: len(file.data)})
		if err != nil {
			return fmt.Errorf("request external upload: %w", err)
		}
		if upload == nil || upload.UploadURL == "" || upload.FileID == "" {
			return errors.New("external upload returned incomplete metadata")
		}
		if err := d.client.UploadToURL(ctx, slack.UploadToURLParameters{UploadURL: upload.UploadURL, Filename: file.name, Reader: bytes.NewReader(file.data)}); err != nil {
			return fmt.Errorf("upload auth bundle: %w", err)
		}
		files = append(files, slack.FileSummary{ID: upload.FileID, Title: file.name})
	}
	comment := "Your requested authentication bundle for cluster " + safeAuthClusterName(clusterName) + " is attached."
	if mode == "vpn" {
		comment += " VPN certificate expiry: " + expiry + ". Extending the allocation does not renew this certificate."
	}
	_, err = d.client.CompleteUploadExternalContext(ctx, slack.CompleteUploadExternalParameters{Files: files, Channel: dm.ID, InitialComment: comment})
	if err != nil {
		return fmt.Errorf("complete auth bundle upload: %w", err)
	}
	return nil
}

func safeAuthClusterName(value string) string {
	value = strings.Map(func(character rune) rune {
		if character < 0x20 || character == 0x7f {
			return -1
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "requested cluster"
	}
	if len(value) > 128 {
		return value[:128]
	}
	return value
}
