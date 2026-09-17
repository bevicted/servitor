package slackbot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/slack-go/slack"
)

func TestAuthDeliveryCompletesExactPublicOrVPNBundleInOwnerDM(t *testing.T) {
	for _, test := range []struct {
		name, mode, expiry string
		vpn                []byte
		wantFiles          int
	}{
		{name: "public", mode: "public", wantFiles: 1},
		{name: "vpn", mode: "vpn", expiry: "2026-09-08T01:00:00Z", vpn: []byte("synthetic-vpn"), wantFiles: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			var uploads [][]byte
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/conversations.open":
					if err := request.ParseForm(); err != nil {
						t.Fatal(err)
					}
					if request.Form.Get("users") != "U1" || request.Form.Get("return_im") != "true" {
						t.Fatalf("open DM form = %v", request.Form)
					}
					_, _ = writer.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
				case "/files.getUploadURLExternal":
					if err := request.ParseForm(); err != nil {
						t.Fatal(err)
					}
					name := request.Form.Get("filename")
					if name != "kubeconfig.yaml" && name != "client.ovpn" {
						t.Fatalf("upload filename = %q", name)
					}
					_, _ = writer.Write([]byte(`{"ok":true,"upload_url":"` + server.URL + `/upload","file_id":"F` + string(rune('1'+len(uploads))) + `"}`))
				case "/upload":
					body, err := io.ReadAll(request.Body)
					if err != nil {
						t.Fatal(err)
					}
					uploads = append(uploads, body)
					writer.WriteHeader(http.StatusOK)
				case "/files.completeUploadExternal":
					if err := request.ParseForm(); err != nil {
						t.Fatal(err)
					}
					if request.Form.Get("channel_id") != "D1" || !strings.Contains(request.Form.Get("initial_comment"), "cluster example-cluster") || strings.Contains(request.Form.Get("initial_comment"), "http") {
						t.Fatalf("complete form = %v", request.Form)
					}
					if test.mode == "vpn" && (!strings.Contains(request.Form.Get("initial_comment"), test.expiry) || !strings.Contains(request.Form.Get("initial_comment"), "does not renew")) {
						t.Fatalf("VPN completion text = %q", request.Form.Get("initial_comment"))
					}
					var files []slack.FileSummary
					if err := json.Unmarshal([]byte(request.Form.Get("files")), &files); err != nil {
						t.Fatal(err)
					}
					if len(files) != test.wantFiles || files[0].Title != "kubeconfig.yaml" || (test.wantFiles == 2 && files[1].Title != "client.ovpn") {
						t.Fatalf("completed files = %#v", files)
					}
					_, _ = writer.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
				default:
					t.Errorf("unexpected request %s", request.URL.Path)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			api := slack.New("synthetic-token", slack.OptionAPIURL(server.URL+"/"))
			if err := NewAuthDelivery(api).DeliverAuthBundle(context.Background(), "U1", "example-cluster", test.mode, test.expiry, []byte("synthetic-kubeconfig"), test.vpn); err != nil {
				t.Fatal(err)
			}
			if len(uploads) != test.wantFiles || !bytes.Contains(uploads[0], []byte("synthetic-kubeconfig")) || (test.wantFiles == 2 && !bytes.Contains(uploads[1], test.vpn)) {
				t.Fatalf("external uploads did not receive the stored bundle")
			}
		})
	}
}

func TestAuthDeliveryDoesNotCompletePartialVPNUpload(t *testing.T) {
	var completeCalls int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/conversations.open":
			_, _ = writer.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
		case "/files.getUploadURLExternal":
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if request.Form.Get("filename") == "client.ovpn" {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = writer.Write([]byte(`{"ok":true,"upload_url":"` + server.URL + `/upload","file_id":"F1"}`))
		case "/upload":
			writer.WriteHeader(http.StatusOK)
		case "/files.completeUploadExternal":
			completeCalls++
			writer.WriteHeader(http.StatusInternalServerError)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	api := slack.New("synthetic-token", slack.OptionAPIURL(server.URL+"/"))
	if err := NewAuthDelivery(api).DeliverAuthBundle(context.Background(), "U1", "cluster", "vpn", "2026-09-08T01:00:00Z", []byte("synthetic-kubeconfig"), []byte("synthetic-vpn")); err == nil {
		t.Fatal("partial VPN upload unexpectedly succeeded")
	}
	if completeCalls != 0 {
		t.Fatalf("partial VPN upload completed externally: %d", completeCalls)
	}
}

func TestAuthDeliveryRejectsIncompleteBundleBeforeSlackCalls(t *testing.T) {
	api := slack.New("synthetic-token")
	if err := NewAuthDelivery(api).DeliverAuthBundle(context.Background(), "U1", "cluster", "vpn", "", []byte("synthetic-kubeconfig"), nil); err == nil {
		t.Fatal("incomplete VPN bundle reached Slack adapter")
	}
}
