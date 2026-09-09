package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type reportLogs struct {
	data []byte
	err  error
}

func (l reportLogs) ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), l.err
}

func validRecoveryMetadata() servitorv1alpha1.RecoveryMetadata {
	return servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"},
	}
}

func validRecoveryPointer() *servitorv1alpha1.RecoveryMetadata {
	recovery := validRecoveryMetadata()
	return &recovery
}

func TestReconcilePersistsOneOperationAndAdoptsMatchingReport(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "cluster-uid", Generation: 1}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, Approval: "approved"}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster).Build()
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	reconciler := &Reconciler{Client: client, Scheme: scheme, Config: Config{Namespace: "ns", Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Platform: "openshift", Version: "4.22", ResourceGroup: "Default"}}, Backend: servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "ict-state-bucket", Region: "us-south", Endpoint: "https://s3.us-south.example.invalid", SkipCredentialsValidation: true, SkipMetadataAPICheck: true, SkipRegionValidation: true, SkipRequestingAccountID: true, ForcePathStyle: true}, BackendPrefix: "servitor", ExecutionImage: "registry.example/ict@sha256:deadbeef", ReviewTimeout: 5 * time.Minute}, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}
	for i := 0; i < 4; i++ {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation == nil || stored.Status.Backend.Key != "servitor/cluster-uid.tfstate" || stored.Status.Backend.Bucket != "ict-state-bucket" || stored.Status.Phase != servitorv1alpha1.PhasePlanning {
		t.Fatalf("unfrozen operation status: %+v", stored.Status)
	}
	run := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	if !pipeline.MatchingRun(run, string(stored.UID), stored.Status.Operation.ID) {
		t.Fatal("PipelineRun identity labels are missing")
	}
	if len(run.OwnerReferences) != 0 {
		t.Fatal("active PipelineRun must not have a CR owner reference")
	}

	// A restarted controller with different defaults observes the original run and snapshot.
	reconciler.Config.Defaults.Version = "1.31"
	reconciler.Config.ExecutionImage = "registry.example/changed"
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ResolvedOptions.Version != "4.22" || stored.Status.ExecutionImage != "registry.example/ict@sha256:deadbeef" || stored.Status.LifecycleSnapshot == nil || stored.Status.LifecycleSnapshot.InitialLeaseSeconds != 3600 || len(stored.Status.LifecycleSnapshot.RetrySeconds) != 1 || stored.Status.LifecycleSnapshot.RetrySeconds[0] != 60 {
		t.Fatal("restart changed frozen planning inputs")
	}

	now = now.Add(10 * time.Minute)
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{APIVersion: "tekton.dev/v1", Kind: "TaskRun"}, Name: "task", PipelineTaskName: "operation"}}
	if err := client.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "ns"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	if err := client.Create(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	resolved := *stored.Status.ResolvedOptions
	resolved.ClusterName = "cluster"
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(stored.UID), OperationID: stored.Status.Operation.ID, ResolvedOptions: resolved, Recovery: validRecoveryMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Logs = reportLogs{data: report}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval || !stored.Status.Operation.Adopted || stored.Status.ReviewDeadline == nil || !stored.Status.ReviewDeadline.Time.Equal(now.Add(5*time.Minute)) || stored.Status.ReviewGeneration != stored.Generation || stored.Status.ReviewApproval != "approved" {
		t.Fatalf("report was not adopted with its approval state: %+v", stored.Status)
	}
}

func TestSnapshotOmitsVPCDefaultForClassic(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{Spec: servitorv1alpha1.ServitorClusterSpec{
		UserOptions: servitorv1alpha1.UserOptions{Provider: "classic"},
	}}
	reconciler := Reconciler{Config: Config{Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{
		Version: "4.22", Provider: "vpc-gen2", VPCID: "default-vpc",
	}}, OpenShiftFlavor: "bx2.4x16"}}

	if err := reconciler.snapshot(cluster); err != nil {
		t.Fatal(err)
	}

	if cluster.Status.ResolvedOptions.Provider != "classic" || cluster.Status.ResolvedOptions.VPCID != "" {
		t.Fatalf("Classic snapshot retained VPC default: %+v", cluster.Status.ResolvedOptions)
	}
}

func TestReconcileFreezesDerivedStartupDefaults(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := cleanupCluster(now)
	cluster.Status = servitorv1alpha1.ServitorClusterStatus{}
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := cleanupReconciler(client, now)
	reconciler.Config.Defaults = servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{
		Version: "4.22", Target: "target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "default-vpc",
	}}
	reconciler.Config.OpenShiftFlavor = "bx2.4x16"
	reconciler.Config.KubernetesFlavor = "bx2.2x8"
	request := cleanupRequest()
	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ResolvedOptions == nil || stored.Status.ResolvedOptions.Platform != "openshift" || stored.Status.ResolvedOptions.Flavor != "bx2.4x16" || stored.Status.ResolvedOptions.Zone != "us-south-1" || stored.Status.ResolvedOptions.VPCID != "default-vpc" {
		t.Fatalf("startup defaults were not fully resolved: %+v", stored.Status.ResolvedOptions)
	}
	reconciler.Config.Defaults.Version = "1.31"
	reconciler.Config.OpenShiftFlavor = "changed"
	reconciler.Config.KubernetesFlavor = "changed"
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ResolvedOptions.Platform != "openshift" || stored.Status.ResolvedOptions.Flavor != "bx2.4x16" {
		t.Fatalf("restart changed frozen startup defaults: %+v", stored.Status.ResolvedOptions)
	}
}

func TestReconcileRejectsChangedLifecyclePolicy(t *testing.T) {
	scheme := cleanupScheme(t)
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := cleanupCluster(now)
	cluster.Spec.Lifecycle.InitialLeaseSeconds = 7200
	cluster.Spec.Lifecycle.RetrySeconds = []int64{120}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	if _, err := cleanupReconciler(client, now).Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.Diagnostic != "LifecyclePolicyChanged" || stored.Status.LifecycleSnapshot.InitialLeaseSeconds != 3600 || stored.Status.LifecycleSnapshot.RetrySeconds[0] != 60 {
		t.Fatalf("changed policy was not rejected against the frozen snapshot: %+v", stored.Status)
	}
}

func TestReconcileApprovedApplyUsesFrozenInputsAndAdoptsReadyReport(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	operation := "plan-a"
	deadline := metav1.NewTime(now.Add(time.Minute))
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "cluster-uid", Generation: 1, Finalizers: []string{servitorv1alpha1.CleanupFinalizer}},
		Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhaseAwaitingApproval,
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default"}, ClusterName: "frozen", Region: "us-south"},
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
			Backend:           &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "frozen.tfstate", Region: "us-south", Endpoint: "https://s3.example.invalid"},
			ExecutionImage:    "registry.example/ict@sha256:frozen",
			Recovery:          validRecoveryPointer(),
			Operation:         &servitorv1alpha1.OperationReference{ID: operation, Kind: "plan", PipelineRunName: "plan-run", Adopted: true},
			ReviewDeadline:    &deadline,
			ReviewGeneration:  1,
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client, Scheme: scheme, Config: Config{Namespace: "ns", Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Version: "changed"}}, ExecutionImage: "registry.example/changed"}, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}
	if result, err := reconciler.Reconcile(context.Background(), request); err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("awaiting approval did not requeue: result=%+v err=%v", result, err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation == nil || stored.Status.Operation.ID != operation {
		t.Fatalf("awaiting approval changed the planning operation: %+v", stored.Status)
	}
	stored.Spec.Lifecycle.Approval = "approved"
	stored.Generation++
	if err := client.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseApplying || stored.Status.Operation.Kind != "apply" || stored.Status.Operation.ID == operation {
		t.Fatalf("approval did not persist a fresh apply identity: %+v", stored.Status)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	applyRun := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: stored.Status.Operation.PipelineRunName}, applyRun); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 1 {
		t.Fatalf("duplicate approval created PipelineRuns: %d, %v", len(runs.Items), err)
	}
	params := map[string]string{}
	for _, param := range applyRun.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	if params["operation-kind"] != "apply" || params["execution-image"] != "registry.example/ict@sha256:frozen" || params["resolved-options"] == "" || params["recovery"] == "" || params["backend"] == "" {
		t.Fatalf("apply PipelineRun did not receive frozen inputs: %#v", params)
	}
	applyRun.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	applyRun.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "apply-task", PipelineTaskName: "operation"}}
	if err := client.Status().Update(context.Background(), applyRun); err != nil {
		t.Fatal(err)
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-task", Namespace: "ns"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	if err := client.Create(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(stored.UID), OperationID: stored.Status.Operation.ID, ResolvedOptions: *stored.Status.ResolvedOptions, Recovery: *stored.Status.Recovery, Ready: servitorv1alpha1.ReadySummary{Resources: []servitorv1alpha1.SummaryResource{{Role: "Cluster", ID: "cluster-id", Name: "frozen"}}}})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Logs = reportLogs{data: report}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseReady || !stored.Status.Operation.Adopted || stored.Status.Ready == nil || len(stored.Status.Ready.Resources) != 1 {
		t.Fatalf("apply report was not adopted as ready: %+v", stored.Status)
	}
	if stored.Status.LeaseExpiresAt == nil || !stored.Status.LeaseExpiresAt.Time.Equal(now.Add(time.Hour)) {
		t.Fatalf("ready transition did not snapshot the initial lease: %+v", stored.Status.LeaseExpiresAt)
	}
	reconciler.Now = func() time.Time { return now.Add(10 * time.Minute) }
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Status.LeaseExpiresAt.Time.Equal(now.Add(time.Hour)) {
		t.Fatalf("duplicate ready reconcile moved lease expiry: %s", stored.Status.LeaseExpiresAt)
	}
}

func TestReconcileApprovalSetBeforeReviewRequiresNewApproval(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Minute))
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "uid", Generation: 2, Finalizers: []string{servitorv1alpha1.CleanupFinalizer}},
		Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, Approval: "approved"}},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhaseAwaitingApproval,
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{},
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
			Operation:         &servitorv1alpha1.OperationReference{ID: "plan-a", Kind: "plan", PipelineRunName: "plan-run", Adopted: true},
			ReviewDeadline:    &deadline,
			ReviewGeneration:  1,
			ReviewApproval:    "approved",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client, Config: Config{Namespace: "ns"}, Now: func() time.Time { return now }}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}})
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("pre-existing approval did not remain pending: result=%+v err=%v", result, err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval || cluster.Status.Operation == nil || cluster.Status.Operation.Kind != "plan" {
		t.Fatalf("approval written during planning launched apply: %+v", cluster.Status)
	}
	cluster.Spec.Lifecycle.Approval = ""
	cluster.Generation++
	if err := client.Update(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.ReviewGeneration != cluster.Generation || cluster.Status.ReviewApproval != "" {
		t.Fatalf("cleared approval did not reset review state: %+v", cluster.Status)
	}
	cluster.Spec.Lifecycle.Approval = "approved"
	cluster.Generation++
	if err := client.Update(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != servitorv1alpha1.PhaseApplying || cluster.Status.Operation == nil || cluster.Status.Operation.Kind != "apply" {
		t.Fatalf("new approval written while awaiting did not launch apply: %+v", cluster.Status)
	}
}

func TestReconcileApprovalRejectsLateAndRejectedDecisions(t *testing.T) {
	for _, test := range []struct {
		name     string
		approval string
		deadline time.Time
	}{
		{name: "late approval", approval: "approved", deadline: time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)},
		{name: "rejected approval", approval: "rejected", deadline: time.Date(2026, 9, 8, 0, 1, 0, 0, time.UTC)},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			deadline := metav1.NewTime(test.deadline)
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, Approval: test.approval}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}, ReviewDeadline: &deadline}}
			client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
			reconciler := &Reconciler{Client: client, Config: Config{Namespace: "ns"}, Now: func() time.Time { return time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC) }}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}); err != nil {
				t.Fatal(err)
			}
			if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, cluster); err != nil {
				t.Fatal(err)
			}
			if cluster.Status.Phase != servitorv1alpha1.PhaseCleanupPending || !cluster.Status.CleanupRequested || cluster.Status.Operation != nil {
				t.Fatalf("invalid decision launched or retained apply: %+v", cluster.Status)
			}
		})
	}
}

func TestReconcileFailedApplyRequestsCleanupWithoutRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}, Operation: &servitorv1alpha1.OperationReference{ID: "apply-a", Kind: "apply", PipelineRunName: "run"}}}
	run := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: "uid", pipeline.OperationLabel: "apply-a"}}, Status: tektonv1.PipelineRunStatus{Status: duckv1.Status{Conditions: duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionFalse}}}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster, run).Build()
	reconciler := &Reconciler{Client: client, Config: Config{Namespace: "ns"}}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != servitorv1alpha1.PhaseCleanupPending || !cluster.Status.CleanupRequested || cluster.Status.Diagnostic != "ApplyFailed" {
		t.Fatalf("failed apply did not remain available for cleanup: %+v", cluster.Status)
	}
}

func TestReconcileMarksMissingReportLogUnresolved(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	operation := "plan-a"
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "cluster-uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}},
		Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhasePlanning,
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}},
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
			Operation:         &servitorv1alpha1.OperationReference{ID: operation, PipelineRunName: "run"},
		},
	}
	run := &tektonv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: operation}},
		Status: tektonv1.PipelineRunStatus{
			Status:                  duckv1.Status{Conditions: duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}},
			PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{ChildReferences: []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "task", PipelineTaskName: "operation"}}},
		},
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "ns"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "missing", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster, run, task).Build()
	reconciler := &Reconciler{Client: client, Scheme: scheme, Config: Config{Namespace: "ns"}, Logs: reportLogs{err: apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "missing")}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("missing log was requeued: %+v", result)
	}
	if err := client.Get(context.Background(), request.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != servitorv1alpha1.PhaseUnresolved || cluster.Status.Diagnostic != "ReportMissing" {
		t.Fatalf("missing log was not retained as unresolved: %+v", cluster.Status)
	}
}

func TestReconcileDoesNotRecreateDispatchedPipelineRun(t *testing.T) {
	for _, operation := range []struct {
		name  string
		kind  string
		phase string
	}{
		{name: "plan", kind: "plan", phase: servitorv1alpha1.PhasePlanning},
		{name: "apply", kind: "apply", phase: servitorv1alpha1.PhaseApplying},
	} {
		t.Run(operation.name, func(t *testing.T) {
			now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
			cluster := cleanupCluster(now)
			cluster.Status.Phase = operation.phase
			cluster.Status.Operation = &servitorv1alpha1.OperationReference{
				ID:              operation.kind,
				Kind:            operation.kind,
				PipelineRunName: "missing-run",
				Dispatched:      true,
			}
			client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
			if _, err := cleanupReconciler(client, now).Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.Diagnostic != "OperationRunMissing" {
				t.Fatalf("missing dispatched %s run was not retained as unresolved: %+v", operation.kind, stored.Status)
			}
			var runs tektonv1.PipelineRunList
			if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 0 {
				t.Fatalf("missing dispatched %s run was recreated: %d, %v", operation.kind, len(runs.Items), err)
			}
		})
	}
}

func TestCleanupDoesNotStartDestroyAfterMissingDispatchedApply(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := cleanupCluster(now)
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.ApplyDispatched = true
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit, RequestedAt: metav1.NewTime(now), RequiresDestroy: true}
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: "apply", Kind: "apply", PipelineRunName: "missing-apply", Dispatched: true}
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
	if _, err := cleanupReconciler(client, now).Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.Diagnostic != "OperationRunMissing" {
		t.Fatalf("missing dispatched apply was not retained as unresolved: %+v", stored.Status)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 0 {
		t.Fatalf("destroy was started after missing dispatched apply: %d, %v", len(runs.Items), err)
	}
}

func TestCleanupSourcesPersistTypedReason(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		reason servitorv1alpha1.CleanupReason
		setup  func(*servitorv1alpha1.ServitorCluster, *tektonv1.PipelineRun)
	}{
		{"rejected review", servitorv1alpha1.CleanupReasonRejected, func(cluster *servitorv1alpha1.ServitorCluster, _ *tektonv1.PipelineRun) {
			deadline := metav1.NewTime(now.Add(time.Minute))
			cluster.Status.Phase, cluster.Status.ReviewDeadline, cluster.Spec.Lifecycle.Approval = servitorv1alpha1.PhaseAwaitingApproval, &deadline, "rejected"
		}},
		{"expired review", servitorv1alpha1.CleanupReasonReviewExpired, func(cluster *servitorv1alpha1.ServitorCluster, _ *tektonv1.PipelineRun) {
			expired := metav1.NewTime(now)
			cluster.Status.Phase, cluster.Status.ReviewDeadline = servitorv1alpha1.PhaseAwaitingApproval, &expired
		}},
		{"planning failure", servitorv1alpha1.CleanupReasonPlanningFailed, func(cluster *servitorv1alpha1.ServitorCluster, run *tektonv1.PipelineRun) {
			cluster.Status.Phase = servitorv1alpha1.PhasePlanning
			cluster.Status.Operation = operationReference("plan", "plan-run")
			failedRun(run, cluster, "plan", "plan-run")
		}},
		{"apply failure", servitorv1alpha1.CleanupReasonApplyFailed, func(cluster *servitorv1alpha1.ServitorCluster, run *tektonv1.PipelineRun) {
			cluster.Status.Phase = servitorv1alpha1.PhaseApplying
			cluster.Status.Operation = operationReference("apply", "apply-run")
			cluster.Status.Operation.Kind = "apply"
			failedRun(run, cluster, "apply", "apply-run")
		}},
		{"explicit intent", servitorv1alpha1.CleanupReasonExplicit, func(cluster *servitorv1alpha1.ServitorCluster, _ *tektonv1.PipelineRun) {
			cluster.Spec.Lifecycle.CleanupRequested = true
			cluster.Status.Phase = servitorv1alpha1.PhaseReady
			cluster.Status.Ready = &servitorv1alpha1.ReadySummary{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := cleanupScheme(t)
			cluster := cleanupCluster(now)
			run := &tektonv1.PipelineRun{}
			test.setup(cluster, run)
			objects := []client.Object{cluster}
			if run.Name != "" {
				objects = append(objects, run)
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(objects...).Build()
			reconciler := cleanupReconciler(client, now)
			if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Cleanup == nil || stored.Status.Cleanup.Reason != test.reason || stored.Status.Phase != servitorv1alpha1.PhaseCleanupPending {
				t.Fatalf("cleanup source was not persisted: %+v", stored.Status)
			}
		})
	}
}

func TestExplicitCleanupCreatesDestroyPipelineRun(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	scheme := cleanupScheme(t)
	cluster := cleanupCluster(now)
	cluster.Spec.Lifecycle.CleanupRequested = true
	cluster.Status.Phase = servitorv1alpha1.PhaseReady
	cluster.Status.Ready = &servitorv1alpha1.ReadySummary{}
	cluster.Status.ApplyDispatched = true
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
	reconciler := cleanupReconciler(client, now)

	for i := 0; i < 3; i++ {
		if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
			t.Fatal(err)
		}
	}

	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cleanup == nil || stored.Status.Cleanup.Reason != servitorv1alpha1.CleanupReasonExplicit || stored.Status.Operation == nil || stored.Status.Operation.Kind != "destroy" {
		t.Fatalf("explicit cleanup did not advance to destroy: %+v", stored.Status)
	}
	run := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatalf("explicit cleanup did not create destroy PipelineRun: %v", err)
	}
	if !pipeline.MatchingRun(run, string(stored.UID), stored.Status.Operation.ID) {
		t.Fatal("destroy PipelineRun identity labels are missing")
	}
}

func TestCleanupWaitsForActiveApplyThenUsesDeterministicDestroy(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	scheme := cleanupScheme(t)
	cluster := cleanupCluster(now)
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.ApplyDispatched = true
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit, RequestedAt: metav1.NewTime(now), RequiresDestroy: true}
	cluster.Status.Operation = operationReference("apply", "apply-run")
	apply := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-run", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: "apply"}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster, apply).Build()
	reconciler := cleanupReconciler(client, now)
	result, err := reconciler.Reconcile(context.Background(), cleanupRequest())
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("active apply was not awaited: %+v, %v", result, err)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 1 {
		t.Fatalf("cleanup overlapped apply: %d, %v", len(runs.Items), err)
	}
	failedRun(apply, cluster, "apply", "apply-run")
	if err := client.Status().Update(context.Background(), apply); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation == nil || stored.Status.Operation.Kind != "destroy" || stored.Status.Operation.ID != destroyID(string(stored.UID), 0) {
		t.Fatalf("destroy operation = %+v", stored.Status.Operation)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 2 {
		t.Fatalf("deterministic destroy was not created: %d, %v", len(runs.Items), err)
	}
	for _, run := range runs.Items {
		if run.Name == stored.Status.Operation.PipelineRunName && len(run.OwnerReferences) != 0 {
			t.Fatal("destroy PipelineRun must not have a CR owner reference")
		}
	}
}

func TestPlanOnlyCleanupFinalizesWithoutDestroy(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	scheme := cleanupScheme(t)
	cluster := cleanupCluster(now)
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonRejected, RequestedAt: metav1.NewTime(now)}
	cluster.Status.Operation = operationReference("plan", "plan-run")
	plan := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "plan-run", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: "plan"}}}
	succeededRun(plan)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster, plan).Build()
	reconciler := cleanupReconciler(client, now)
	for i := 0; i < 2; i++ {
		if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
			t.Fatal(err)
		}
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cleanup.CompletedAt == nil || stored.Status.Phase != servitorv1alpha1.PhaseCleanupComplete || !contains(stored.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		t.Fatalf("plan-only cleanup did not retain its observable completion state: %+v", stored)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 0 {
		t.Fatalf("plan run was not explicitly removed: %d, %v", len(runs.Items), err)
	}
}

func TestDestroyRetryPersistsDeadlineAndRetainsUnresolvedFinalizer(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	scheme := cleanupScheme(t)
	cluster := cleanupCluster(now)
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.ApplyDispatched = true
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonApplyFailed, RequestedAt: metav1.NewTime(now), RequiresDestroy: true}
	cluster.Status.Operation = operationReference(destroyID(string(cluster.UID), 0), "destroy-run-0")
	cluster.Status.Operation.Kind = "destroy"
	destroy := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "destroy-run-0", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: cluster.Status.Operation.ID}}}
	failedRun(destroy, cluster, cluster.Status.Operation.ID, "destroy-run-0")
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster, destroy).Build()
	reconciler := cleanupReconciler(client, now)
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cleanup.RetryCount != 1 || stored.Status.Cleanup.NextRetryAt == nil || !stored.Status.Cleanup.NextRetryAt.Time.Equal(now.Add(time.Minute)) {
		t.Fatalf("retry state = %+v", stored.Status.Cleanup)
	}
	// A restarted reconciler obeys the persisted absolute deadline.
	if result, err := cleanupReconciler(client, now).Reconcile(context.Background(), cleanupRequest()); err != nil || result.RequeueAfter != time.Minute {
		t.Fatalf("restart did not retain retry deadline: %+v, %v", result, err)
	}
	reconciler.Now = func() time.Time { return now.Add(time.Minute) }
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation == nil || stored.Status.Operation.ID != destroyID(string(stored.UID), 1) {
		t.Fatalf("retry destroy identity = %+v", stored.Status.Operation)
	}
	second := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: stored.Status.Operation.PipelineRunName, Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(stored.UID), pipeline.OperationLabel: stored.Status.Operation.ID}}}
	failedRun(second, stored, stored.Status.Operation.ID, second.Name)
	if err := client.Create(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.Cleanup.NextRetryAt != nil || !contains(stored.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		t.Fatalf("exhausted cleanup did not retain recovery ownership: %+v", stored.Status)
	}
}

func cleanupScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func cleanupCluster(_ time.Time) *servitorv1alpha1.ServitorCluster {
	return &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "cleanup-uid", Generation: 1, Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}}, Status: servitorv1alpha1.ServitorClusterStatus{ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "frozen"}, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}, Backend: &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "key", Region: "us-south", Endpoint: "https://s3.example.invalid"}, ExecutionImage: "registry.example/ict@sha256:frozen", Recovery: validRecoveryPointer()}}
}

func cleanupReconciler(client client.Client, now time.Time) *Reconciler {
	return &Reconciler{Client: client, Config: Config{Namespace: "ns"}, Now: func() time.Time { return now }}
}
func cleanupRequest() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "cluster"}}
}
func operationReference(id, name string) *servitorv1alpha1.OperationReference {
	return &servitorv1alpha1.OperationReference{ID: id, Kind: "plan", PipelineRunName: name}
}
func succeededRun(run *tektonv1.PipelineRun) {
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
}
func failedRun(run *tektonv1.PipelineRun, cluster *servitorv1alpha1.ServitorCluster, operation, name string) {
	run.Name, run.Namespace = name, cluster.Namespace
	run.Labels = map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: operation}
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionFalse}}
}

func TestSuccessfulDestroyConsumesReportThenCompletesAndRemovesFinalizer(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	scheme := cleanupScheme(t)
	cluster := cleanupCluster(now)
	operation := destroyID(string(cluster.UID), 0)
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.ApplyDispatched = true
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit, RequestedAt: metav1.NewTime(now), RequiresDestroy: true}
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: operation, Kind: "destroy", PipelineRunName: "destroy-run", Dispatched: true}
	run := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "destroy-run", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: operation}}}
	succeededRun(run)
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "destroy-task", PipelineTaskName: "operation"}}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "destroy-task", Namespace: "ns"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(cluster.UID), OperationID: operation, ResolvedOptions: *cluster.Status.ResolvedOptions, Recovery: *cluster.Status.Recovery})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster, run, task).Build()
	reconciler := cleanupReconciler(client, now)
	reconciler.Logs = reportLogs{data: report}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cleanup.CompletedAt == nil || stored.Status.Phase != servitorv1alpha1.PhaseCleanupComplete || !contains(stored.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		t.Fatalf("destroy did not retain observable cleanup completion: %+v", stored.Status)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "destroy-run"}, &tektonv1.PipelineRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed destroy PipelineRun was not explicitly deleted: %v", err)
	}
}

func TestCleanupCompleteAllocationRemainsObservableBeforeDeletion(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := cleanupCluster(now)
	completed := metav1.NewTime(now)
	cluster.Finalizers = nil
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit, RequestedAt: completed, CompletedAt: &completed}
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	request := cleanupRequest()
	reconciler := cleanupReconciler(client, now)
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil || result.RequeueAfter != cleanupNotificationGrace {
		t.Fatalf("completion was not retained for notification: result=%+v err=%v", result, err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, stored); err != nil || stored.Status.Phase != servitorv1alpha1.PhaseCleanupComplete {
		t.Fatalf("cleanup completion was not observable: cluster=%+v err=%v", stored, err)
	}
	reconciler.Now = func() time.Time { return now.Add(cleanupNotificationGrace) }
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, &servitorv1alpha1.ServitorCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed allocation was retained or had its finalizer re-added: %v", err)
	}

	replacement := cleanupCluster(now)
	replacement.UID = ""
	replacement.Finalizers = nil
	replacement.Status = servitorv1alpha1.ServitorClusterStatus{}
	if err := client.Create(context.Background(), replacement); err != nil {
		t.Fatalf("completed allocation name cannot be reused: %v", err)
	}
}

func TestReadyLeaseExtensionPersistsTargetAndRollingCap(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		expiry     time.Time
		increment  time.Duration
		wantExpiry time.Time
		wantAdded  time.Duration
	}{
		{name: "whole-hour extension", expiry: now.Add(4 * time.Hour), increment: 8 * time.Hour, wantExpiry: now.Add(12 * time.Hour), wantAdded: 8 * time.Hour},
		{name: "sub-hour rolling-cap clamp", expiry: now.Add(24*time.Hour - 30*time.Second), increment: time.Hour, wantExpiry: now.Add(24 * time.Hour), wantAdded: 30 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := readyLeaseCluster(now, test.expiry)
			requested := metav1.NewTime(test.expiry.Add(test.increment))
			cluster.Spec.Lifecycle.RequestedExpiry = &requested
			client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
			reconciler := cleanupReconciler(client, now)
			if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			result := stored.Status.LeaseExtension
			if result == nil || result.Outcome != servitorv1alpha1.ExtensionOutcomeApplied || !result.RequestedExpiry.Time.Equal(requested.Time) || result.PreviousExpiry == nil || !result.PreviousExpiry.Time.Equal(test.expiry) || result.NewExpiry == nil || !result.NewExpiry.Time.Equal(test.wantExpiry) || result.AddedSeconds != int64(test.wantAdded.Seconds()) || stored.Status.LeaseExpiresAt == nil || !stored.Status.LeaseExpiresAt.Time.Equal(test.wantExpiry) {
				t.Fatalf("extension result = %+v, lease = %+v", result, stored.Status.LeaseExpiresAt)
			}
			// A restarted reconciler sees the recorded absolute target and does not
			// add the same duration again.
			restarted := cleanupReconciler(client, now.Add(time.Minute))
			if _, err := restarted.Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			if !stored.Status.LeaseExpiresAt.Time.Equal(test.wantExpiry) || stored.Status.LeaseExtension.AddedSeconds != int64(test.wantAdded.Seconds()) {
				t.Fatalf("replayed extension changed durable result: %+v", stored.Status)
			}
		})
	}
}

func TestReadyLeaseRejectsInvalidStateAndExpiredExtensionIntents(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		setup   func(*servitorv1alpha1.ServitorCluster)
		outcome servitorv1alpha1.ExtensionOutcome
	}{
		{name: "non-ready", setup: func(cluster *servitorv1alpha1.ServitorCluster) { cluster.Status.Phase = servitorv1alpha1.PhaseApplying }, outcome: servitorv1alpha1.ExtensionOutcomeNotReady},
		{name: "invalid increment", setup: func(cluster *servitorv1alpha1.ServitorCluster) {
			requested := metav1.NewTime(cluster.Status.LeaseExpiresAt.Time.Add(30 * time.Minute))
			cluster.Spec.Lifecycle.RequestedExpiry = &requested
		}, outcome: servitorv1alpha1.ExtensionOutcomeInvalid},
		{name: "expired", setup: func(cluster *servitorv1alpha1.ServitorCluster) {
			expired := metav1.NewTime(now)
			cluster.Status.LeaseExpiresAt = &expired
			requested := metav1.NewTime(now.Add(time.Hour))
			cluster.Spec.Lifecycle.RequestedExpiry = &requested
		}, outcome: servitorv1alpha1.ExtensionOutcomeExpired},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := readyLeaseCluster(now, now.Add(4*time.Hour))
			requested := metav1.NewTime(now.Add(5 * time.Hour))
			cluster.Spec.Lifecycle.RequestedExpiry = &requested
			test.setup(cluster)
			original := cluster.Status.LeaseExpiresAt.DeepCopy()
			client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
			if _, err := cleanupReconciler(client, now).Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.LeaseExtension == nil || stored.Status.LeaseExtension.Outcome != test.outcome || stored.Status.LeaseExpiresAt == nil || !stored.Status.LeaseExpiresAt.Time.Equal(original.Time) {
				t.Fatalf("invalid extension changed lease or missed typed result: %+v", stored.Status)
			}
			if test.outcome == servitorv1alpha1.ExtensionOutcomeExpired && (stored.Status.Cleanup == nil || stored.Status.Cleanup.Reason != servitorv1alpha1.CleanupReasonLeaseExpired) {
				t.Fatalf("expired lease did not start cleanup: %+v", stored.Status.Cleanup)
			}
		})
	}
}

type conflictStatusClient struct {
	client.Client
	failUpdate bool
}

func (c *conflictStatusClient) Status() client.SubResourceWriter {
	return conflictStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type conflictStatusWriter struct {
	client.SubResourceWriter
	client *conflictStatusClient
}

func (w conflictStatusWriter) Update(ctx context.Context, object client.Object, options ...client.SubResourceUpdateOption) error {
	if w.client.failUpdate {
		w.client.failUpdate = false
		return apierrors.NewConflict(schema.GroupResource{Group: servitorv1alpha1.GroupVersion.Group, Resource: "servitorclusters/status"}, object.GetName(), nil)
	}
	return w.SubResourceWriter.Update(ctx, object, options...)
}

func TestReadyLeaseExtensionRetriesConflictWithoutDoubleAddition(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	cluster := readyLeaseCluster(now, now.Add(4*time.Hour))
	requested := metav1.NewTime(now.Add(5 * time.Hour))
	cluster.Spec.Lifecycle.RequestedExpiry = &requested
	base := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	client := &conflictStatusClient{Client: base, failUpdate: true}
	reconciler := cleanupReconciler(client, now)
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); !apierrors.IsConflict(err) {
		t.Fatalf("first extension update error = %v, want conflict", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := base.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.LeaseExpiresAt == nil || !stored.Status.LeaseExpiresAt.Time.Equal(requested.Time) || stored.Status.LeaseExtension.AddedSeconds != int64(time.Hour.Seconds()) {
		t.Fatalf("conflict retry added lease more than once: %+v", stored.Status)
	}
}

func readyLeaseCluster(now, expiry time.Time) *servitorv1alpha1.ServitorCluster {
	cluster := cleanupCluster(now)
	leaseExpiry := metav1.NewTime(expiry)
	cluster.Status.Phase = servitorv1alpha1.PhaseReady
	cluster.Status.Ready = &servitorv1alpha1.ReadySummary{}
	cluster.Status.LeaseExpiresAt = &leaseExpiry
	return cluster
}

func TestDeletionWaitsForLabelledNonOwnedApply(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	scheme := cleanupScheme(t)
	cluster := cleanupCluster(now)
	deleting := metav1.NewTime(now)
	cluster.DeletionTimestamp = &deleting
	cluster.Status.Phase = servitorv1alpha1.PhaseApplying
	cluster.Status.ApplyDispatched = true
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: "apply", Kind: "apply", PipelineRunName: "apply-run", Dispatched: true}
	apply := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-run", Namespace: "ns", Labels: map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: "apply"}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster, apply).Build()
	reconciler := cleanupReconciler(client, now)
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cleanup == nil || stored.Status.Cleanup.Reason != servitorv1alpha1.CleanupReasonDeletion || !contains(stored.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		t.Fatalf("deletion did not enter finalizer-protected cleanup: %+v", stored.Status)
	}
	if len(apply.OwnerReferences) != 0 {
		t.Fatal("foreground CR deletion must not own an active PipelineRun")
	}
	if result, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil || result.RequeueAfter == 0 {
		t.Fatalf("deletion did not wait for active apply: %+v, %v", result, err)
	}
}
