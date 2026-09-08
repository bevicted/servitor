package slackbot

import (
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterListRendersSafeStatusWithoutSlackIDs(t *testing.T) {
	expires := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	clusters := []servitorv1alpha1.ServitorCluster{{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "servitor-one", Region: "us-south/us-south-1"}, LeaseExpiresAt: &expires}}, {Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uother"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying}}}
	text := strings.Join(clusterListMessages(clusters, "Ucaller", time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)), "\n")
	for _, want := range []string{"servitor-one", "ready", "applying", "2026-09-08 04:00:00 UTC (~4h)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("list missing %q: %s", want, text)
		}
	}
	for _, private := range []string{"Ucaller", "Uother"} {
		if strings.Contains(text, private) {
			t.Fatalf("list leaked %q: %s", private, text)
		}
	}
}
