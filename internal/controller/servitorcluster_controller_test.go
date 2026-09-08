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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type reportLogs struct {
	data []byte
	err  error
}

func (l reportLogs) ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), l.err
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
	reconciler := &Reconciler{Client: client, Scheme: scheme, Config: Config{Namespace: "ns", Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Platform: "openshift", Version: "4.22", ResourceGroup: "Default"}}, Backend: servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "ict-state-bucket", Region: "us-south", Endpoint: "https://s3.us-south.example.invalid", SkipCredentialsValidation: true, SkipMetadataAPICheck: true, SkipRegionValidation: true, SkipRequestingAccountID: true, ForcePathStyle: true}, BackendPrefix: "servitor", ExecutionImage: "registry.example/ict@sha256:deadbeef"}, Now: func() time.Time { return time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC) }}
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
	if stored.Status.ResolvedOptions.Version != "4.22" || stored.Status.ExecutionImage != "registry.example/ict@sha256:deadbeef" {
		t.Fatal("restart changed frozen planning inputs")
	}

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
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(stored.UID), OperationID: stored.Status.Operation.ID, ResolvedOptions: resolved, Recovery: servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"}})
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
	if stored.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval || !stored.Status.Operation.Adopted || stored.Status.ReviewDeadline == nil || stored.Status.ReviewGeneration != stored.Generation || stored.Status.ReviewApproval != "approved" {
		t.Fatalf("report was not adopted with its approval state: %+v", stored.Status)
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
			Phase:            servitorv1alpha1.PhaseAwaitingApproval,
			ResolvedOptions:  &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default"}, ClusterName: "frozen", Region: "us-south"},
			Backend:          &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "frozen.tfstate", Region: "us-south", Endpoint: "https://s3.example.invalid"},
			ExecutionImage:   "registry.example/ict@sha256:frozen",
			Recovery:         &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"},
			Operation:        &servitorv1alpha1.OperationReference{ID: operation, Kind: "plan", PipelineRunName: "plan-run", Adopted: true},
			ReviewDeadline:   &deadline,
			ReviewGeneration: 1,
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
			Phase:            servitorv1alpha1.PhaseAwaitingApproval,
			ResolvedOptions:  &servitorv1alpha1.ResolvedOptions{},
			Operation:        &servitorv1alpha1.OperationReference{ID: "plan-a", Kind: "plan", PipelineRunName: "plan-run", Adopted: true},
			ReviewDeadline:   &deadline,
			ReviewGeneration: 1,
			ReviewApproval:   "approved",
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
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, Approval: test.approval}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, ReviewDeadline: &deadline}}
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
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, Operation: &servitorv1alpha1.OperationReference{ID: "apply-a", Kind: "apply", PipelineRunName: "run"}}}
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
			Phase:           servitorv1alpha1.PhasePlanning,
			ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}},
			Operation:       &servitorv1alpha1.OperationReference{ID: operation, PipelineRunName: "run"},
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
