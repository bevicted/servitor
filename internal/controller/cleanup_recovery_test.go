package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	cleanupRecoveryFrozenImage  = "registry.example/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cleanupRecoveryImage        = "registry.example/servitor-task@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	cleanupRecoveryChangedImage = "registry.example/servitor-task@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func exhaustedCleanupRecoveryCluster(now time.Time) *servitorv1alpha1.ServitorCluster {
	cluster := cleanupCluster(now)
	cluster.ResourceVersion = "42"
	cluster.Status.Phase = servitorv1alpha1.PhaseUnresolved
	cluster.Status.ExecutionImage = cleanupRecoveryFrozenImage
	cluster.Status.CleanupRequested = true
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{
		Reason:          servitorv1alpha1.CleanupReasonLeaseExpired,
		RequestedAt:     metav1.NewTime(now),
		RequiresDestroy: true,
		RetryCount:      len(cluster.Status.LifecycleSnapshot.RetrySeconds),
	}
	return cluster
}

func failedCleanupRecovery(now time.Time, image, operation string, attempt int) *servitorv1alpha1.CleanupRecoveryStatus {
	completed := metav1.NewTime(now)
	return &servitorv1alpha1.CleanupRecoveryStatus{
		Attempt:        attempt,
		ExecutionImage: image,
		OperationID:    operation,
		StartedAt:      completed,
		State:          servitorv1alpha1.CleanupRecoveryFailed,
		CompletedAt:    &completed,
	}
}

func cleanupRecoveryRequestForCluster(t *testing.T, cluster *servitorv1alpha1.ServitorCluster, image string) cleanupRecoveryRequest {
	t.Helper()
	fingerprint, ok := cleanupRecoveryStatusFingerprint(cluster)
	if !ok {
		t.Fatal("cleanup recovery fingerprint inputs are incomplete")
	}
	return cleanupRecoveryRequest{UID: string(cluster.UID), Generation: cluster.Generation, StatusFingerprint: fingerprint, ExecutionImage: image, Reason: cleanupRecoveryReason}
}

func setCleanupRecoveryRequest(t *testing.T, cluster *servitorv1alpha1.ServitorCluster, request cleanupRecoveryRequest) {
	t.Helper()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[servitorv1alpha1.CleanupRecoveryRequestAnnotation] = string(encoded)
}

func approveCleanupRecovery(t *testing.T, cluster *servitorv1alpha1.ServitorCluster, image string) {
	t.Helper()
	setCleanupRecoveryRequest(t, cluster, cleanupRecoveryRequestForCluster(t, cluster, image))
}

func cleanupRecoveryParams(run *tektonv1.PipelineRun) map[string]string {
	params := make(map[string]string, len(run.Spec.Params))
	for _, parameter := range run.Spec.Params {
		params[parameter.Name] = parameter.Value.StringVal
	}
	return params
}

func TestCleanupRecoveryRequiresExactApprovedRequest(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	for name, mutate := range map[string]func(*servitorv1alpha1.ServitorCluster){
		"wrong UID": func(cluster *servitorv1alpha1.ServitorCluster) {
			request := cleanupRecoveryRequestForCluster(t, cluster, cleanupRecoveryImage)
			request.UID = "other"
			setCleanupRecoveryRequest(t, cluster, request)
		},
		"stale status fingerprint": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
			cluster.Status.Cleanup.RetryCount++
		},
		"stale generation": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
			cluster.Generation++
		},
		"wrong image": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cleanupRecoveryFrozenImage)
		},
		"wrong failure reason": func(cluster *servitorv1alpha1.ServitorCluster) {
			request := cleanupRecoveryRequestForCluster(t, cluster, cleanupRecoveryImage)
			request.Reason = "other"
			setCleanupRecoveryRequest(t, cluster, request)
		},
		"same frozen image": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cluster.Status.ExecutionImage)
		},
		"unexhausted retries": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
			cluster.Status.Cleanup.RetryCount--
		},
		"active operation": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
			cluster.Status.Operation = operationReference("destroy", "destroy-run")
			cluster.Status.Operation.Kind = "destroy"
		},
		"prior recovery": func(cluster *servitorv1alpha1.ServitorCluster) {
			approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
			cluster.Status.CleanupRecovery = &servitorv1alpha1.CleanupRecoveryStatus{ExecutionImage: cleanupRecoveryImage, OperationID: "old", StartedAt: metav1.NewTime(now), State: servitorv1alpha1.CleanupRecoveryFailed}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := exhaustedCleanupRecoveryCluster(now)
			mutate(cluster)
			client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
			reconciler := cleanupReconciler(client, now)
			reconciler.Config.ExecutionImage = cleanupRecoveryImage
			if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.CleanupRecovery != nil && stored.Status.CleanupRecovery.State == servitorv1alpha1.CleanupRecoveryPending {
				t.Fatalf("ineligible request started cleanup recovery: %+v", stored.Status)
			}
			var runs tektonv1.PipelineRunList
			if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 0 {
				t.Fatalf("ineligible request created %d runs: %v", len(runs.Items), err)
			}
		})
	}
}

func TestSecondCleanupRecoveryRequiresFailedFirstAttemptWithChangedImage(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	for name, setup := range map[string]func(*servitorv1alpha1.ServitorCluster){
		"missing replacement request": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.CleanupRecovery = failedCleanupRecovery(now, cleanupRecoveryImage, "first", 1)
		},
		"same image": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.CleanupRecovery = failedCleanupRecovery(now, cleanupRecoveryChangedImage, "first", 1)
		},
		"successful first attempt": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.CleanupRecovery = failedCleanupRecovery(now, cleanupRecoveryImage, "first", 1)
			cluster.Status.CleanupRecovery.State = servitorv1alpha1.CleanupRecoverySucceeded
		},
		"nonterminal first attempt": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.CleanupRecovery = failedCleanupRecovery(now, cleanupRecoveryImage, "first", 1)
			cluster.Status.CleanupRecovery.State = servitorv1alpha1.CleanupRecoveryPending
			cluster.Status.CleanupRecovery.CompletedAt = nil
		},
		"second attempt already used": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.CleanupRecovery = failedCleanupRecovery(now, cleanupRecoveryImage, "second", 2)
		},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := exhaustedCleanupRecoveryCluster(now)
			setup(cluster)
			if name != "missing replacement request" {
				approveCleanupRecovery(t, cluster, cleanupRecoveryChangedImage)
			}
			client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
			reconciler := cleanupReconciler(client, now)
			reconciler.Config.ExecutionImage = cleanupRecoveryChangedImage
			if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
				t.Fatal(err)
			}
			var runs tektonv1.PipelineRunList
			if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 0 {
				t.Fatalf("ineligible second recovery dispatched %d runs: %v", len(runs.Items), err)
			}
		})
	}
}

func TestSecondCleanupRecoveryPersistsHistoryAcrossRestartAndIsTerminal(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	cluster := exhaustedCleanupRecoveryCluster(now)
	cluster.Status.CleanupRecovery = failedCleanupRecovery(now, cleanupRecoveryImage, "first-recovery", 0)
	cluster.Status.AuthCertificateReferences = []servitorv1alpha1.AuthCertificateReference{{ID: "certificate-one", AllocationUID: string(cluster.UID), AttemptID: "attempt-one"}}
	approveCleanupRecovery(t, cluster, cleanupRecoveryChangedImage)
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
	reconciler := cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryChangedImage
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if recovery := stored.Status.CleanupRecovery; recovery == nil || recovery.Attempt != 2 || recovery.ExecutionImage != cleanupRecoveryChangedImage || len(recovery.History) != 1 || recovery.History[0].ExecutionImage != cleanupRecoveryImage || recovery.History[0].OperationID != "first-recovery" || recovery.History[0].Outcome != servitorv1alpha1.CleanupRecoveryFailed || !recovery.History[0].Timestamp.Time.Equal(now) {
		t.Fatalf("second recovery did not retain the first attempt: %+v", recovery)
	}

	// A restart must dispatch the persisted second image and all current refs.
	reconciler = cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryFrozenImage
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	run := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	params := cleanupRecoveryParams(run)
	var references []servitorv1alpha1.AuthCertificateReference
	if err := json.Unmarshal([]byte(params["auth-certificate-refs"]), &references); err != nil || len(references) != 1 || references[0].ID != "certificate-one" {
		t.Fatalf("second recovery dispatch lost certificate refs: %+v, err=%v", references, err)
	}
	if params["execution-image"] != cleanupRecoveryChangedImage {
		t.Fatalf("second recovery dispatch did not use frozen contract: %#v", params)
	}

	failedRun(run, stored, stored.Status.Operation.ID, run.Name)
	if err := client.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.CleanupRecovery.State != servitorv1alpha1.CleanupRecoveryFailed || stored.Status.CleanupRecovery.Attempt != 2 || stored.Status.Operation != nil {
		t.Fatalf("second recovery failure was not terminal: %+v", stored.Status)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 1 {
		t.Fatalf("terminal second recovery dispatched %d runs: %v", len(runs.Items), err)
	}
}

func TestCleanupRecoverySnapshotsCurrentImageAndDispatchesOneFrozenDestroy(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	cluster := exhaustedCleanupRecoveryCluster(now)
	cluster.Status.AuthCertificateReferences = []servitorv1alpha1.AuthCertificateReference{{ID: "certificate-one", AllocationUID: string(cluster.UID), AttemptID: "attempt-one"}, {ID: "certificate-two", AllocationUID: string(cluster.UID), AttemptID: "attempt-two"}}
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
	approved := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, approved); err != nil {
		t.Fatal(err)
	}
	initialResourceVersion := approved.ResourceVersion
	approveCleanupRecovery(t, approved, cleanupRecoveryImage)
	if err := client.Update(context.Background(), approved); err != nil {
		t.Fatal(err)
	}
	if approved.ResourceVersion == initialResourceVersion {
		t.Fatal("annotation update did not advance resourceVersion")
	}
	approvalResourceVersion := approved.ResourceVersion
	reconciler := cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryImage
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.CleanupRecovery == nil || stored.Status.CleanupRecovery.ExecutionImage != cleanupRecoveryImage || stored.Status.CleanupRecovery.OperationID == "" || stored.Status.CleanupRecovery.RequestResourceVersion != approvalResourceVersion || !stored.Status.CleanupRecovery.StartedAt.Time.Equal(now) || stored.Status.CleanupRecovery.State != servitorv1alpha1.CleanupRecoveryPending || stored.Status.ExecutionImage != cleanupRecoveryFrozenImage {
		t.Fatalf("recovery was not persisted with a separate current image: %+v", stored.Status)
	}

	// A restarted controller with a new config must dispatch the persisted image.
	reconciler = cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryChangedImage
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	run := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	params := cleanupRecoveryParams(run)
	if params["execution-image"] != cleanupRecoveryImage || params["backend"] == "" || params["recovery"] == "" || params["resolved-options"] == "" || !pipeline.MatchingRun(run, string(stored.UID), stored.Status.CleanupRecovery.OperationID) {
		t.Fatalf("recovery destroy did not preserve the execution contract: %#v", params)
	}
	var backend servitorv1alpha1.BackendIdentity
	var options servitorv1alpha1.ResolvedOptions
	var recovery servitorv1alpha1.RecoveryMetadata
	if err := json.Unmarshal([]byte(params["backend"]), &backend); err != nil || !reflect.DeepEqual(backend, *stored.Status.Backend) {
		t.Fatalf("recovery destroy changed frozen backend: %+v, err=%v", backend, err)
	}
	if err := json.Unmarshal([]byte(params["resolved-options"]), &options); err != nil || !reflect.DeepEqual(options, *stored.Status.ResolvedOptions) {
		t.Fatalf("recovery destroy changed resolved options: %+v, err=%v", options, err)
	}
	if err := json.Unmarshal([]byte(params["recovery"]), &recovery); err != nil || !reflect.DeepEqual(recovery, *stored.Status.Recovery) {
		t.Fatalf("recovery destroy changed recovery context: %+v, err=%v", recovery, err)
	}
	var references []servitorv1alpha1.AuthCertificateReference
	if err := json.Unmarshal([]byte(params["auth-certificate-refs"]), &references); err != nil || len(references) != 2 || references[0].ID != "certificate-one" || references[1].ID != "certificate-two" {
		t.Fatalf("recovery destroy refs = %+v, err=%v", references, err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 1 {
		t.Fatalf("one-shot recovery dispatched %d runs: %v", len(runs.Items), err)
	}
}

func TestCleanupRecoverySuccessCompletesNormalCleanup(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	cluster := exhaustedCleanupRecoveryCluster(now)
	approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster).Build()
	reconciler := cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryImage
	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
			t.Fatal(err)
		}
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	run := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	succeededRun(run)
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "recovery-task", PipelineTaskName: "operation"}}
	if err := client.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "recovery-task", Namespace: stored.Namespace}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	if err := client.Create(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(stored.UID), OperationID: stored.Status.Operation.ID, ResolvedOptions: *stored.Status.ResolvedOptions, Recovery: *stored.Status.Recovery})
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Logs = reportLogs{data: report}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseCleanupComplete || stored.Status.Cleanup.CompletedAt == nil || stored.Status.Operation != nil || stored.Status.CleanupRecovery == nil || stored.Status.CleanupRecovery.State != servitorv1alpha1.CleanupRecoverySucceeded || stored.Status.CleanupRecovery.CompletedAt == nil || !contains(stored.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		t.Fatalf("successful recovery did not use normal cleanup completion: %+v", stored.Status)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: run.Name}, &tektonv1.PipelineRun{}); err == nil {
		t.Fatal("successful recovery PipelineRun was not removed")
	}
}

func TestCleanupRecoveryRejectsMismatchedResult(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	cluster := exhaustedCleanupRecoveryCluster(now)
	approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
	reconciler := cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryImage
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	mismatched := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: stored.Status.Operation.PipelineRunName, Namespace: stored.Namespace, Labels: map[string]string{pipeline.ClusterUIDLabel: string(stored.UID), pipeline.OperationLabel: "other"}}}
	if err := client.Create(context.Background(), mismatched); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.Operation != nil || stored.Status.CleanupRecovery == nil || stored.Status.CleanupRecovery.State != servitorv1alpha1.CleanupRecoveryFailed || stored.Status.Diagnostic != "CleanupRecoveryIdentityMismatch" {
		t.Fatalf("mismatched recovery result was accepted: %+v", stored.Status)
	}
}

func TestCleanupRecoveryFailureIsTerminalAndDoesNotRetry(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	cluster := exhaustedCleanupRecoveryCluster(now)
	approveCleanupRecovery(t, cluster, cleanupRecoveryImage)
	client := fake.NewClientBuilder().WithScheme(cleanupScheme(t)).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithObjects(cluster).Build()
	reconciler := cleanupReconciler(client, now)
	reconciler.Config.ExecutionImage = cleanupRecoveryImage
	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
			t.Fatal(err)
		}
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	run := &tektonv1.PipelineRun{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: stored.Namespace, Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	failedRun(run, stored, stored.Status.Operation.ID, run.Name)
	if err := client.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), cleanupRequest().NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseUnresolved || stored.Status.Operation != nil || stored.Status.CleanupRecovery == nil || stored.Status.CleanupRecovery.State != servitorv1alpha1.CleanupRecoveryFailed || stored.Status.CleanupRecovery.CompletedAt == nil || stored.Status.Cleanup.RetryCount != 1 {
		t.Fatalf("failed recovery was not terminal: %+v", stored.Status)
	}
	if _, err := reconciler.Reconcile(context.Background(), cleanupRequest()); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := client.List(context.Background(), &runs); err != nil || len(runs.Items) != 1 {
		t.Fatalf("terminal recovery retried with %d runs: %v", len(runs.Items), err)
	}
}
