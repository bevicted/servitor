package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	"github.com/slack-go/slack"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type replies struct{ values []slackbot.Response }

func (r *replies) Reply(_ context.Context, response slackbot.Response) error {
	r.values = append(r.values, response)
	return nil
}

func TestOwnerThreadAuthDrivesControllerAndExternalDMUpload(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "slack-316ca0efda6296d8f2c11d1e", Namespace: "ns", UID: "allocation-uid"},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack:     servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
		},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhaseReady,
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, PublicAuthEligible: true},
			PublicAuth:        &servitorv1alpha1.PublicAuthStatus{Availability: "available"},
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{ClusterName: "example-cluster"},
			LeaseExpiresAt:    &metav1.Time{Time: now.Add(time.Hour)},
		},
	}
	secretName := pipeline.AuthResourceName(string(cluster.UID))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "ns", Labels: map[string]string{"servitor.bevicted.github.io/auth-uid": string(cluster.UID)}, Annotations: map[string]string{"servitor.bevicted.github.io/auth-operation": "apply-bb82212777bdc1b9"}}, Data: map[string][]byte{"kubeconfig.yaml": []byte("synthetic-kubeconfig")}}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithRuntimeObjects(cluster, secret).Build()

	var openCalls, uploadURLCalls, uploadCalls, completeCalls int
	var uploadReceived bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/conversations.open":
			openCalls++
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if request.Form.Get("users") != "U1" {
				t.Fatalf("DM owner = %q", request.Form.Get("users"))
			}
			_, _ = writer.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
		case "/files.getUploadURLExternal":
			uploadURLCalls++
			_, _ = writer.Write([]byte(`{"ok":true,"upload_url":"` + server.URL + `/upload","file_id":"F1"}`))
		case "/upload":
			uploadCalls++
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			uploadReceived = bytes.Contains(body, []byte("synthetic-kubeconfig"))
			writer.WriteHeader(http.StatusOK)
		case "/files.completeUploadExternal":
			completeCalls++
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if request.Form.Get("channel_id") != "D1" {
				t.Fatalf("file shared outside owner DM: %q", request.Form.Get("channel_id"))
			}
			_, _ = writer.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	responses := &replies{}
	bot := slackbot.Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "ns", Client: kube, Events: state.NewEventStore(kube, "ns"), Responder: responses}
	if err := bot.Handle(context.Background(), slackbot.Envelope{ID: "auth", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "auth", Timestamp: "1710000000.000100", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.values) != 1 || responses.values[0].Text != "Authentication delivery has been queued for your DM." {
		t.Fatalf("thread acknowledgement = %#v", responses.values)
	}

	api := slack.New("synthetic-token", slack.OptionAPIURL(server.URL+"/"))
	reconciler := &controller.Reconciler{Client: kube, SecretReader: kube, AuthDelivery: slackbot.NewAuthDelivery(api), Config: controller.Config{Namespace: "ns"}, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "slack-316ca0efda6296d8f2c11d1e"}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != "Delivered" || openCalls != 1 || uploadURLCalls != 1 || uploadCalls != 1 || completeCalls != 1 || !uploadReceived {
		t.Fatalf("auth delivery status=%#v calls=%d/%d/%d/%d upload=%v", stored.Status.AuthDelivery, openCalls, uploadURLCalls, uploadCalls, completeCalls, uploadReceived)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if openCalls != 1 || completeCalls != 1 {
		t.Fatalf("reconciliation replayed consumed upload: open=%d complete=%d", openCalls, completeCalls)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: secretName}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["kubeconfig.yaml"]) != "synthetic-kubeconfig" {
		t.Fatal("delivery modified stored kubeconfig")
	}
}
