package slackbot

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
)

type permalinkRecorder struct {
	value    string
	err      error
	channels []string
	times    []string
}

func (p *permalinkRecorder) Permalink(_ context.Context, channel, timestamp string) (string, error) {
	p.channels = append(p.channels, channel)
	p.times = append(p.times, timestamp)
	return p.value, p.err
}

func existingAllocation(owner string) *servitorv1alpha1.ServitorCluster {
	expires := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	return &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: ownerClusterName("U1"), Namespace: "servitor"},
		Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: owner, ChannelID: "C1", ThreadTimestamp: "1710000000.000100"}},
		Status:     servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expires},
	}
}

func TestExistingAllocationLinksOnlyOwnerLifecycleThread(t *testing.T) {
	cluster := existingAllocation("U1")
	bot, responses := botForTest(t, cluster)
	links := &permalinkRecorder{value: "https://slack.example.invalid/archives/C1/p1710000000000100"}
	bot.Permalinks = links

	if err := bot.Handle(context.Background(), Envelope{ID: "existing", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create", Timestamp: "1710000010.000100"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 {
		t.Fatalf("responses = %+v", responses.responses)
	}
	text := responses.responses[0].Text
	if !strings.Contains(text, "state ready") || !strings.Contains(text, "Lease:") || !strings.Contains(text, "<https://slack.example.invalid/archives/C1/p1710000000000100|Open allocation thread>") {
		t.Fatalf("existing allocation response = %q", text)
	}
	if !reflect.DeepEqual(links.channels, []string{"C1"}) || !reflect.DeepEqual(links.times, []string{"1710000000.000100"}) {
		t.Fatalf("permalink lookup = channels=%v timestamps=%v", links.channels, links.times)
	}
}

func TestExistingAllocationNavigationFallsBackWithoutCrossOwnerLink(t *testing.T) {
	for _, test := range []struct {
		name  string
		owner string
		links *permalinkRecorder
	}{
		{name: "lookup failure", owner: "U1", links: &permalinkRecorder{err: errors.New("Slack unavailable")}},
		{name: "invalid permalink", owner: "U1", links: &permalinkRecorder{value: "https://slack.example.invalid/archives/C1|injected"}},
		{name: "different owner", owner: "U2", links: &permalinkRecorder{value: "https://slack.example.invalid/archives/C1/p1710000000000100"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bot, responses := botForTest(t, existingAllocation(test.owner))
			bot.Permalinks = test.links
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create", Timestamp: "1710000010.000100"}}); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 {
				t.Fatalf("responses = %+v", responses.responses)
			}
			text := responses.responses[0].Text
			if !strings.Contains(text, "state ready") || !strings.Contains(text, "Lease:") || !strings.Contains(text, "Open the original allocation thread in the configured channel.") || strings.Contains(text, "|Open allocation thread>") {
				t.Fatalf("fallback response = %q", text)
			}
			if test.owner != "U1" && len(test.links.channels) != 0 {
				t.Fatalf("looked up another owner's allocation: %+v", test.links)
			}
		})
	}
}

func TestDMChannelOnlyCommandsUseConfiguredChannelMarkup(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "dm-create", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "create", Timestamp: "1710000010.000100"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !strings.Contains(responses.responses[0].Text, "<#C1>") {
		t.Fatalf("DM responses = %+v", responses.responses)
	}
}

func TestExistingAllocationNavigationUsesSlackTransportAndFallsBack(t *testing.T) {
	for _, test := range []struct {
		name       string
		permalink  int
		body       string
		wantLinked bool
	}{
		{name: "success", permalink: http.StatusOK, body: `{"ok":true,"channel":"C1","permalink":"https://slack.example.invalid/archives/C1/p1710000000000100"}`, wantLinked: true},
		{name: "missing message", permalink: http.StatusOK, body: `{"ok":false,"error":"message_not_found"}`},
		{name: "failure", permalink: http.StatusInternalServerError, body: `{"ok":false,"error":"internal_error"}`},
		{name: "rate limited", permalink: http.StatusTooManyRequests, body: `{"ok":false,"error":"ratelimited"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var permalinkQuery url.Values
			var permalinkAuthorization string
			var posted url.Values
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/chat.getPermalink":
					permalinkQuery = request.URL.Query()
					permalinkAuthorization = request.Header.Get("Authorization")
					if test.permalink == http.StatusTooManyRequests {
						writer.Header().Set("Retry-After", "1")
					}
					writer.WriteHeader(test.permalink)
					_, _ = writer.Write([]byte(test.body))
				case "/chat.postMessage":
					if err := request.ParseForm(); err != nil {
						t.Errorf("parse post form: %v", err)
					}
					posted = request.Form
					_, _ = writer.Write([]byte(`{"ok":true,"channel":"C1","ts":"1710000010.000100"}`))
				default:
					t.Errorf("unexpected Slack request %s", request.URL.Path)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			transport := testSocketMode(server.URL + "/")
			cluster := existingAllocation("U1")
			wantSpec, wantStatus := cluster.Spec.DeepCopy(), cluster.Status.DeepCopy()
			bot, _ := botForTest(t, cluster)
			bot.Responder = transport
			bot.Permalinks = transport
			if err := bot.Handle(context.Background(), Envelope{ID: "existing-" + test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create", Timestamp: "1710000010.000100"}}); err != nil {
				t.Fatal(err)
			}
			if permalinkQuery.Get("channel") != "C1" || permalinkQuery.Get("message_ts") != "1710000000.000100" || permalinkAuthorization != "Bearer synthetic-token" {
				t.Fatalf("permalink request = query=%v authorization=%q", permalinkQuery, permalinkAuthorization)
			}
			if posted.Get("channel") != "C1" || posted.Get("thread_ts") != "1710000010.000100" || posted.Get("mrkdwn") == "false" {
				t.Fatalf("posted Slack mrkdwn = %v", posted)
			}
			if test.wantLinked {
				if !strings.Contains(posted.Get("text"), "<https://slack.example.invalid/archives/C1/p1710000000000100|Open allocation thread>") {
					t.Fatalf("linked message = %q", posted.Get("text"))
				}
			} else if !strings.Contains(posted.Get("text"), "Open the original allocation thread in the configured channel.") || strings.Contains(posted.Get("text"), "|Open allocation thread>") {
				t.Fatalf("fallback message = %q", posted.Get("text"))
			}
			actual := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, actual); err != nil {
				t.Fatal(err)
			}
			if !apiequality.Semantic.DeepEqual(actual.Spec, *wantSpec) || !apiequality.Semantic.DeepEqual(actual.Status, *wantStatus) {
				t.Fatalf("existing allocation was mutated: spec=%+v status=%+v", actual.Spec, actual.Status)
			}
		})
	}
}

func testSocketMode(apiURL string) *SocketMode {
	api := slack.New("synthetic-token", slack.OptionAppLevelToken("synthetic-app-token"), slack.OptionAPIURL(apiURL))
	return &SocketMode{client: socketmode.New(api)}
}
