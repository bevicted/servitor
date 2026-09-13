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

func TestAuthDeliveryOpensOwnerDMAndCompletesOneExternalUpload(t *testing.T) {
	var uploadReceived bool
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
			if request.Form.Get("filename") != "kubeconfig.yaml" || request.Form.Get("length") != "20" {
				t.Fatalf("upload URL form = %v", request.Form)
			}
			_, _ = writer.Write([]byte(`{"ok":true,"upload_url":"` + server.URL + `/upload","file_id":"F1"}`))
		case "/upload":
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			uploadReceived = bytes.Contains(body, []byte("synthetic-kubeconfig"))
			writer.WriteHeader(http.StatusOK)
		case "/files.completeUploadExternal":
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if request.Form.Get("channel_id") != "D1" || !strings.Contains(request.Form.Get("initial_comment"), "cluster example-cluster") || strings.Contains(request.Form.Get("initial_comment"), "http") {
				t.Fatalf("complete form = %v", request.Form)
			}
			var files []slack.FileSummary
			if err := json.Unmarshal([]byte(request.Form.Get("files")), &files); err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 || files[0].ID != "F1" || files[0].Title != "kubeconfig.yaml" {
				t.Fatalf("completed files = %#v", files)
			}
			_, _ = writer.Write([]byte(`{"ok":true,"files":[{"id":"F1","title":"kubeconfig.yaml"}]}`))
		default:
			t.Errorf("unexpected request %s", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	api := slack.New("synthetic-token", slack.OptionAPIURL(server.URL+"/"))
	if err := NewAuthDelivery(api).DeliverKubeconfig(context.Background(), "U1", "example-cluster", []byte("synthetic-kubeconfig")); err != nil {
		t.Fatal(err)
	}
	if !uploadReceived {
		t.Fatal("external upload did not receive stored bytes")
	}
}

func TestAuthDeliveryRejectsEmptyInputBeforeSlackCalls(t *testing.T) {
	api := slack.New("synthetic-token")
	if err := NewAuthDelivery(api).DeliverKubeconfig(context.Background(), "U1", "cluster", nil); err == nil {
		t.Fatal("empty kubeconfig reached Slack adapter")
	}
}
