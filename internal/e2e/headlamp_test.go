package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type headlampReportLogs struct{ data []byte }

func (l headlampReportLogs) ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), nil
}

type headlampResponder struct{ responses []slackbot.Response }

func (r *headlampResponder) Reply(_ context.Context, response slackbot.Response) error {
	r.responses = append(r.responses, response)
	return nil
}

type retryingConfigurationResponder struct {
	fail          bool
	failed        chan struct{}
	delivered     chan struct{}
	failedOnce    sync.Once
	deliveredOnce sync.Once
	mu            sync.Mutex
	responses     []slackbot.Response
}

func (r *retryingConfigurationResponder) Reply(_ context.Context, response slackbot.Response) error {
	if strings.Contains(response.Text, "Cluster configuration") {
		if r.fail {
			r.failedOnce.Do(func() { close(r.failed) })
			return errors.New("Slack unavailable")
		}
		r.deliveredOnce.Do(func() { close(r.delivered) })
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.responses = append(r.responses, response)
	return nil
}

func (r *retryingConfigurationResponder) configurationCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, response := range r.responses {
		if strings.Contains(response.Text, "Cluster configuration") {
			count++
		}
	}
	return count
}

func TestRecoveredConfigurationDeliveryFailureDoesNotBlockAutomaticApply(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, servitorv1alpha1.AddToScheme, tektonv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	network := servitorv1alpha1.FrozenNetwork{BindingID: "existing", AccountID: "account", VPCRegion: "us-south", VPCID: "vpc", SubnetID: "subnet", PublicGatewayID: "gateway", Zone: "us-south-1"}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22"}, ResourceGroup: "Default", Platform: "openshift", ClusterName: "recovered-cluster", Region: "us-south", Network: network}
	recovery := servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: strings.Repeat("a", 64), Endpoints: map[string]string{"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid"}, Values: servitorv1alpha1.RecoveryValues{ClusterName: "recovered-cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16", AccountID: "account", VPCRegion: "us-south", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}}}
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "allocation", Namespace: "ns", UID: "allocation-uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack:     servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
		},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhasePending,
			ResolvedOptions:   &options,
			Recovery:          &recovery,
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
			Backend:           &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "servitor/allocation-uid.tfstate", Region: "us-south", Endpoint: "https://s3.us-south.example.invalid"},
			ExecutionImage:    "registry.example/ict@sha256:deadbeef",
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithRuntimeObjects(cluster).Build()
	responses := &retryingConfigurationResponder{fail: true, failed: make(chan struct{}), delivered: make(chan struct{})}
	firstReceipts := state.NewEventStore(kube, "ns")
	firstNotifier := &slackbot.StatusNotifier{Client: kube, Namespace: "ns", Responder: responses, Receipts: firstReceipts, Interval: time.Hour}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstNotifier.Start(firstCtx) }()
	select {
	case <-responses.failed:
	case <-time.After(time.Second):
		stopFirst()
		t.Fatal("notifier did not attempt the recovered configuration")
	}
	reconciler := &controller.Reconciler{Client: kube, DirectReader: kube, Scheme: scheme, Config: controller.Config{Namespace: "ns"}, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "allocation"}}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(ctx, request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseApplying || stored.Status.Operation == nil || stored.Status.Operation.Kind != "apply" || !stored.Status.ApplyDispatched {
		t.Fatalf("failed Slack delivery blocked apply dispatch: %+v", stored.Status)
	}
	apply := *stored.Status.Operation
	run := &tektonv1.PipelineRun{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: apply.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "apply-task", PipelineTaskName: "operation"}}
	if err := kube.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-task", Namespace: "ns"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "apply-pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	if err := kube.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(stored.UID), OperationID: apply.ID, ResolvedOptions: *stored.Status.ResolvedOptions, Recovery: *stored.Status.Recovery, Ready: servitorv1alpha1.ReadySummary{Resources: []servitorv1alpha1.SummaryResource{{Role: "Cluster", Name: "recovered-cluster", ID: "cluster-id"}}}})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Logs = headlampReportLogs{data: report}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseReady {
		t.Fatalf("apply did not reach Ready without approval state: %+v", stored.Status)
	}
	stopFirst()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	configurationReceipt := "phase:allocation-uid:configuration:0"
	claimed, err := firstReceipts.Claim(ctx, configurationReceipt)
	if err != nil || !claimed {
		t.Fatalf("failed delivery did not release configuration receipt: claimed=%t err=%v", claimed, err)
	}
	if err := firstReceipts.Release(ctx, configurationReceipt); err != nil {
		t.Fatal(err)
	}

	responses.fail = false
	restarted := &slackbot.StatusNotifier{Client: kube, Namespace: "ns", Responder: responses, Receipts: state.NewEventStore(kube, "ns"), Interval: time.Hour}
	retryCtx, stopRetry := context.WithCancel(ctx)
	retryDone := make(chan error, 1)
	go func() { retryDone <- restarted.Start(retryCtx) }()
	select {
	case <-responses.delivered:
	case <-time.After(time.Second):
		stopRetry()
		t.Fatal("restarted notifier did not retry the recovered configuration")
	}
	stopRetry()
	if err := <-retryDone; err != nil {
		t.Fatal(err)
	}
	if got := responses.configurationCount(); got != 1 {
		t.Fatalf("configuration tables after Ready retry = %d, want 1", got)
	}
}

func TestHeadlampAllocationUsesFrozenLifecycleForAutomaticApplyAndDestroy(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, servitorv1alpha1.AddToScheme, tektonv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).Build()
	responses := &headlampResponder{}
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	bot := slackbot.Bot{
		ChannelID: "C1", SelfUserID: "BOT", Namespace: "ns", Client: kube, Events: state.NewEventStore(kube, "ns"), Responder: responses,
		Defaults: command.CreateDefaults{Target: "target", Provider: "vpc-gen2", Version: "4.22"}, Lease: time.Hour, RetryIntervals: []time.Duration{time.Minute}, Clock: func() time.Time { return now },
	}
	message := slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create kubernetes headlamp", Timestamp: "1710000000.000100"}
	if err := bot.Handle(ctx, slackbot.Envelope{ID: "headlamp-create", Message: message}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Provisioning..." {
		t.Fatalf("create responses = %+v", responses.responses)
	}
	key := types.NamespacedName{Namespace: "ns", Name: "slack-8af2c7b3adfde728cff2e8e9"}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(ctx, key, cluster); err != nil || !cluster.Spec.UserOptions.Headlamp {
		t.Fatalf("Headlamp intent = %+v, %v", cluster.Spec.UserOptions, err)
	}
	// API servers assign a nonzero generation on create; fake clients do not.
	cluster.Generation = 1
	if err := kube.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	reconciler := &controller.Reconciler{Client: kube, DirectReader: kube, Scheme: scheme, Config: controller.Config{
		Namespace: "ns", Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22"}, ResourceGroup: "Default"},
		NetworkBindings: map[string]servitorv1alpha1.FrozenNetwork{"target": {BindingID: "existing", AccountID: "account", VPCRegion: "us-south", VPCID: "vpc", SubnetID: "subnet", PublicGatewayID: "gateway", Zone: "us-south-1"}},
		Backend:         servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Region: "us-south", Endpoint: "https://s3.example.invalid"}, BackendPrefix: "servitor",
		ExecutionImage: "registry.example/ict@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: key}
	for range 4 {
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, key, cluster); err != nil || cluster.Status.Operation == nil || cluster.Status.Operation.Kind != "plan" || cluster.Status.ResolvedOptions == nil || !cluster.Status.ResolvedOptions.Headlamp {
		t.Fatalf("frozen plan state = %+v, %v", cluster.Status, err)
	}
	adoptHeadlampReport(t, ctx, kube, reconciler, request, cluster, "plan", []servitorv1alpha1.SummaryResource{{Role: "Add-on", Name: "headlamp", Actions: []string{"create"}}})
	if err := kube.Get(ctx, key, cluster); err != nil || cluster.Status.Phase != servitorv1alpha1.PhasePending || cluster.Status.Recovery == nil || cluster.Status.ResolvedOptions == nil || !cluster.Status.ResolvedOptions.Headlamp {
		t.Fatalf("Headlamp recovery state = %+v, %v", cluster.Status, err)
	}
	for range 3 {
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, key, cluster); err != nil || cluster.Status.Operation == nil || cluster.Status.Operation.Kind != "apply" {
		t.Fatalf("Headlamp apply state = %+v, %v", cluster.Status, err)
	}
	adoptHeadlampReport(t, ctx, kube, reconciler, request, cluster, "apply", []servitorv1alpha1.SummaryResource{{Role: "Add-on", Name: "headlamp", Actions: []string{"create"}}})
	if err := kube.Get(ctx, key, cluster); err != nil || cluster.Status.Phase != servitorv1alpha1.PhaseReady || cluster.Status.Ready == nil || cluster.Status.Ready.Resources[0].Role != "Add-on" {
		t.Fatalf("Headlamp Ready state = %+v, %v", cluster.Status, err)
	}
	if err := bot.Handle(ctx, slackbot.Envelope{ID: "headlamp-done", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "done", Timestamp: "1710000002.000100", ThreadTimestamp: message.Timestamp}}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, key, cluster); err != nil || cluster.Status.Operation == nil || cluster.Status.Operation.Kind != "destroy" {
		t.Fatalf("Headlamp destroy state = %+v, %v", cluster.Status, err)
	}
	run := &tektonv1.PipelineRun{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: cluster.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	params := map[string]string{}
	for _, parameter := range run.Spec.Params {
		params[parameter.Name] = parameter.Value.StringVal
	}
	if !strings.Contains(params["recovery"], `"headlamp":true`) || !strings.Contains(params["resolved-options"], `"headlamp":true`) {
		t.Fatalf("destroy did not retain Headlamp inputs: %#v", params)
	}
}

func adoptHeadlampReport(t *testing.T, ctx context.Context, kube client.Client, reconciler *controller.Reconciler, request ctrl.Request, cluster *servitorv1alpha1.ServitorCluster, kind string, resources []servitorv1alpha1.SummaryResource) {
	t.Helper()
	run := &tektonv1.PipelineRun{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	taskName := kind + "-headlamp-task"
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: taskName, PipelineTaskName: "operation"}}
	if err := kube.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: cluster.Namespace}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: taskName, Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	if err := kube.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	resolved := *cluster.Status.ResolvedOptions
	resolved.Version = "1.31"
	resolved.ClusterName = "headlamp-cluster"
	resolved.Region = "us-south"
	recovery := servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: strings.Repeat("a", 64), Endpoints: map[string]string{"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid"}, Values: servitorv1alpha1.RecoveryValues{ClusterName: "headlamp-cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 1, Headlamp: true, Zone: "us-south-1", Flavor: "bx2.2x8", AccountID: "account", VPCRegion: "us-south", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}}}
	report := pipeline.Report{Version: 1, ClusterUID: string(cluster.UID), OperationID: cluster.Status.Operation.ID, ResolvedOptions: resolved, Recovery: recovery}
	if kind == "apply" {
		report.Ready = servitorv1alpha1.ReadySummary{Resources: resources}
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Logs = headlampReportLogs{data: data}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
}
