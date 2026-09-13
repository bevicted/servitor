package slackbot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/slack-go/slack"
)

// AuthDelivery adapts Slack's external-upload API for owner-only kubeconfig delivery.
type AuthDelivery struct{ client *slack.Client }

func NewAuthDelivery(client *slack.Client) *AuthDelivery { return &AuthDelivery{client: client} }

// DeliverKubeconfig opens the owner's DM and shares exactly one stored file there.
// It deliberately has no logging path because its input is credential-bearing.
func (d *AuthDelivery) DeliverKubeconfig(ctx context.Context, ownerID, clusterName string, kubeconfig []byte) error {
	if d == nil || d.client == nil {
		return errors.New("Slack auth delivery is not configured")
	}
	if ownerID == "" || len(kubeconfig) == 0 {
		return errors.New("invalid Slack auth delivery request")
	}
	dm, _, _, err := d.client.OpenConversationContext(ctx, &slack.OpenConversationParameters{Users: []string{ownerID}, ReturnIM: true})
	if err != nil {
		return fmt.Errorf("open owner DM: %w", err)
	}
	if dm == nil || dm.ID == "" {
		return errors.New("open owner DM returned no channel")
	}
	upload, err := d.client.GetUploadURLExternalContext(ctx, slack.GetUploadURLExternalParameters{FileName: "kubeconfig.yaml", FileSize: len(kubeconfig)})
	if err != nil {
		return fmt.Errorf("request external upload: %w", err)
	}
	if upload == nil || upload.UploadURL == "" || upload.FileID == "" {
		return errors.New("external upload returned incomplete metadata")
	}
	if err := d.client.UploadToURL(ctx, slack.UploadToURLParameters{UploadURL: upload.UploadURL, Filename: "kubeconfig.yaml", Reader: bytes.NewReader(kubeconfig)}); err != nil {
		return fmt.Errorf("upload kubeconfig: %w", err)
	}
	comment := "Your requested kubeconfig for cluster " + safeAuthClusterName(clusterName) + " is attached."
	_, err = d.client.CompleteUploadExternalContext(ctx, slack.CompleteUploadExternalParameters{
		Files:          []slack.FileSummary{{ID: upload.FileID, Title: "kubeconfig.yaml"}},
		Channel:        dm.ID,
		InitialComment: comment,
	})
	if err != nil {
		return fmt.Errorf("complete kubeconfig upload: %w", err)
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
