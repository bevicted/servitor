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
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	authRetryExecutionImage = "registry.example/servitor-task@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	frozenExecutionImage    = "registry.example/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	changedExecutionImage   = "registry.example/servitor-task@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func eligibleAuthRetryCluster() *servitorv1alpha1.ServitorCluster {
	return &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: "allocation"}, Spec: servitorv1alpha1.ServitorClusterSpec{Lifecycle: servitorv1alpha1.LifecyclePolicy{AuthRetryRequestTimestamp: "1710000000.000001"}}, Status: servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseReady,
		Ready:             &servitorv1alpha1.ReadySummary{},
		ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}, Network: servitorv1alpha1.FrozenNetwork{AuthPolicy: &servitorv1alpha1.FrozenAuthPolicy{}}},
		LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true},
		Auth:              &servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "certificate-cleanup-pending"},
		ExecutionImage:    frozenExecutionImage,
		Recovery:          validRecoveryPointer(),
	}}
}

func TestAuthRetryRefusesCertificateReferenceCapacity(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{Status: servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseReady,
		Ready:             &servitorv1alpha1.ReadySummary{},
		ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}, Network: servitorv1alpha1.FrozenNetwork{AuthPolicy: &servitorv1alpha1.FrozenAuthPolicy{}}},
		Auth:              &servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "certificate-cleanup-pending"},
		LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true},
	}}
	for index := 0; index < servitorv1alpha1.MaxAuthCertificateReferences; index++ {
		cluster.Status.AuthCertificateReferences = append(cluster.Status.AuthCertificateReferences, servitorv1alpha1.AuthCertificateReference{ID: string(rune('a' + index)), AllocationUID: "allocation", AttemptID: string(rune('a' + index))})
	}
	if authRetryEligible(cluster) {
		t.Fatal("accepted auth retry after certificate reference capacity was exhausted")
	}
}

func TestAuthRetrySnapshotsCurrentExecutionImage(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := eligibleAuthRetryCluster()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client, Config: Config{ExecutionImage: authRetryExecutionImage}}
	if _, handled, err := reconciler.reconcileAuthRetry(context.Background(), cluster); err != nil || !handled {
		t.Fatalf("reconcile auth retry = handled:%t err:%v", handled, err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.AuthRetry == nil || stored.Status.AuthRetry.ExecutionImage != authRetryExecutionImage || stored.Status.ExecutionImage != frozenExecutionImage {
		t.Fatalf("auth retry image snapshot = %+v", stored.Status)
	}

	// A later controller configuration cannot change the persisted retry image.
	reconciler.Config.ExecutionImage = changedExecutionImage
	run, err := pipeline.NewAuthRetryRun(stored, pipeline.TaskConfig{})
	if err != nil {
		t.Fatal(err)
	}
	params := make(map[string]string, len(run.Spec.Params))
	for _, param := range run.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	if params["execution-image"] != authRetryExecutionImage || params["execution-image"] == reconciler.Config.ExecutionImage {
		t.Fatalf("auth retry PipelineRun image = %q", params["execution-image"])
	}
}

func TestAuthRetryRejectsMutableExecutionImage(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := eligibleAuthRetryCluster()
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client, Config: Config{ExecutionImage: "registry.example/servitor-task:latest"}}
	if _, handled, err := reconciler.reconcileAuthRetry(context.Background(), cluster); err != nil || !handled {
		t.Fatalf("reconcile mutable auth retry image = handled:%t err:%v", handled, err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation != nil || stored.Status.AuthRetry != nil || stored.Status.Phase != servitorv1alpha1.PhaseUnresolved {
		t.Fatalf("mutable auth retry image started work: %+v", stored.Status)
	}
}

func TestAuthRetryTerminalRequestIsNotReplayed(t *testing.T) {
	cluster := eligibleAuthRetryCluster()
	cluster.Status.AuthRetry = &servitorv1alpha1.AuthRetryStatus{RequestTimestamp: cluster.Spec.Lifecycle.AuthRetryRequestTimestamp, AttemptID: "completed", ExecutionImage: frozenExecutionImage, Outcome: authRetryAvailable}
	if _, handled, err := (&Reconciler{Config: Config{ExecutionImage: authRetryExecutionImage}}).reconcileAuthRetry(context.Background(), cluster); err != nil || handled {
		t.Fatalf("terminal auth retry replay = handled:%t err:%v", handled, err)
	}
}

func TestDaqcaraAdoptedApplyClearsOperationBeforeRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, tektonv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 9, 20, 21, 0, 19, 0, time.UTC)
	policy := &servitorv1alpha1.FrozenAuthPolicy{VPNServerID: "vpn", SecretsManagerID: "secrets", SecretsManagerRegion: "eu-gb", SecretGroupID: "group", CertificateTemplate: "template", Issuer: "issuer", TTL: "2h"}
	network := frozenNetwork()
	network.AuthPolicy = policy
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true, Version: "4.22"}, ResourceGroup: "Default", ClusterName: "daqcara", Region: "us-south", Platform: "openshift", Network: network}
	recovery := validRecoveryMetadata()
	recovery.Values.PrivateOnly = true
	recovery.Values.AuthPolicy = policy
	operation := "apply-daqcara"
	certificate := servitorv1alpha1.AuthCertificateReference{ID: "certificate-daqcara", AllocationUID: "daqcara-uid", AttemptID: operation}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "daqcara", UID: "daqcara-uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1710000000.000001"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}}, Status: servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseApplying,
		ResolvedOptions:   &options,
		Recovery:          &recovery,
		LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, AuthEligible: true},
		Operation:         &servitorv1alpha1.OperationReference{ID: operation, Kind: "apply", PipelineRunName: "apply-daqcara-run"},
	}}
	run := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-daqcara-run", Namespace: "ns"}, Status: tektonv1.PipelineRunStatus{PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{ChildReferences: []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "apply-daqcara-task", PipelineTaskName: "operation"}}}}}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-daqcara-task", Namespace: "ns"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(cluster.UID), OperationID: operation, ResolvedOptions: options, Recovery: recovery, Auth: &servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "certificate-cleanup-pending", AttemptID: operation, Certificate: &certificate}})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster, run, task).Build()
	reconciler := &Reconciler{Client: client, Logs: reportLogs{data: report}, Config: Config{ExecutionImage: authRetryExecutionImage}, Now: func() time.Time { return now }}
	if _, err := reconciler.adoptReport(context.Background(), cluster, run); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: "ns", Name: "daqcara"}
	if err := client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation != nil || stored.Status.Auth == nil || stored.Status.Auth.Reason != "certificate-cleanup-pending" || len(stored.Status.AuthCertificateReferences) != 1 || stored.Status.AuthCertificateReferences[0] != certificate {
		t.Fatalf("adopted daqcara state lost retry provenance: %+v", stored.Status)
	}
	stored.Spec.Lifecycle.AuthRetryRequestTimestamp = "1710000000.000001"
	if err := client.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation == nil || stored.Status.Operation.Kind != "auth-retry" || stored.Status.AuthRetry == nil || stored.Status.AuthRetry.Outcome != authRetryPending {
		t.Fatalf("auth retry intent was not accepted after adopted apply: %+v", stored.Status)
	}
}

func TestReadyAdoptedApplyMigratesBeforeAuthRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 9, 20, 21, 0, 19, 0, time.UTC)
	policy := &servitorv1alpha1.FrozenAuthPolicy{VPNServerID: "vpn", SecretsManagerID: "secrets", SecretsManagerRegion: "eu-gb", SecretGroupID: "group", CertificateTemplate: "template", Issuer: "issuer", TTL: "2h"}
	network := frozenNetwork()
	network.AuthPolicy = policy
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true, Version: "4.22"}, ResourceGroup: "Default", ClusterName: "daqcara", Region: "us-south", Platform: "openshift", Network: network}
	recovery := validRecoveryMetadata()
	recovery.Values.PrivateOnly = true
	recovery.Values.AuthPolicy = policy
	operation := applyID("daqcara-uid")
	certificate := servitorv1alpha1.AuthCertificateReference{ID: "certificate-daqcara", AllocationUID: "daqcara-uid", AttemptID: operation}
	expiry := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "daqcara", UID: "daqcara-uid", Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1710000000.000001"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, AuthRetryRequestTimestamp: "1710000000.000001"}}, Status: servitorv1alpha1.ServitorClusterStatus{
		Phase:                     servitorv1alpha1.PhaseReady,
		Conditions:                []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "ReportAdopted"}},
		Ready:                     &servitorv1alpha1.ReadySummary{},
		ResolvedOptions:           &options,
		Recovery:                  &recovery,
		LifecycleSnapshot:         &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, AuthEligible: true},
		LeaseExpiresAt:            &expiry,
		Operation:                 &servitorv1alpha1.OperationReference{ID: operation, Kind: "apply", PipelineRunName: pipeline.DeterministicRunName("daqcara-uid", operation), Adopted: true},
		Auth:                      &servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "certificate-cleanup-pending", AttemptID: operation, Certificate: &certificate},
		AuthCertificateReferences: []servitorv1alpha1.AuthCertificateReference{certificate},
	}}
	for name, mutate := range map[string]func(*servitorv1alpha1.ServitorCluster){
		"running operation": func(cluster *servitorv1alpha1.ServitorCluster) { cluster.Status.Operation.Adopted = false },
		"failed operation": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
		},
		"mismatched operation": func(cluster *servitorv1alpha1.ServitorCluster) {
			cluster.Status.Operation.PipelineRunName = "other-run"
		},
		"unadopted report": func(cluster *servitorv1alpha1.ServitorCluster) { cluster.Status.Conditions[0].Reason = "ApplyFailed" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cluster.DeepCopy()
			mutate(candidate)
			if isReadyAdoptedApplyOperation(candidate) {
				t.Fatal("accepted a nonterminal apply operation for migration")
			}
		})
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client, Config: Config{ExecutionImage: authRetryExecutionImage}, Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: "ns", Name: "daqcara"}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation != nil || stored.Status.AuthRetry != nil {
		t.Fatalf("migration did not persist only the terminal operation removal: %+v", stored.Status)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Operation == nil || stored.Status.Operation.Kind != "auth-retry" || stored.Status.AuthRetry == nil || stored.Status.AuthRetry.Outcome != authRetryPending {
		t.Fatalf("auth retry intent was not accepted after migration: %+v", stored.Status)
	}
}

func TestAuthRetryLateResultIsFencedAfterCleanupRevokesAuthority(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	operation := "auth-retry-current"
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: "allocation"}, Spec: servitorv1alpha1.ServitorClusterSpec{Lifecycle: servitorv1alpha1.LifecyclePolicy{AuthRetryRequestTimestamp: "2"}}, Status: servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseCleanupPending,
		LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true},
		Operation:         &servitorv1alpha1.OperationReference{ID: operation, Kind: "auth-retry"},
		AuthRetry:         &servitorv1alpha1.AuthRetryStatus{RequestTimestamp: "2", AttemptID: operation, Outcome: authRetryPending},
		Cleanup:           &servitorv1alpha1.CleanupStatus{},
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client}
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, cluster.Status.Operation); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.finishAuthRetry(context.Background(), cluster, servitorv1alpha1.AuthStatus{Availability: "available", AttemptID: operation}); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{}
	attempt := pipeline.AuthAttemptResourceName(string(cluster.UID), operation)
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: attempt}, binding); err == nil {
		t.Fatal("late retry result retained publication authority")
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored); err != nil || stored.Status.Auth != nil || stored.Status.Phase != servitorv1alpha1.PhaseUnresolved {
		t.Fatalf("late retry result was adopted: status=%+v err=%v", stored.Status, err)
	}
}

func TestFinishAuthRetryUnavailableRevokesThenDeletesDeliverySecret(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	operation := "auth-retry-current"
	resolved := servitorv1alpha1.AuthCertificateReference{ID: "certificate-apply", AllocationUID: "allocation", AttemptID: "apply-current"}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: "allocation"}, Spec: servitorv1alpha1.ServitorClusterSpec{Lifecycle: servitorv1alpha1.LifecyclePolicy{AuthRetryRequestTimestamp: "2"}}, Status: servitorv1alpha1.ServitorClusterStatus{
		Phase:                     servitorv1alpha1.PhaseReady,
		LifecycleSnapshot:         &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true},
		Operation:                 &servitorv1alpha1.OperationReference{ID: operation, Kind: "auth-retry"},
		AuthRetry:                 &servitorv1alpha1.AuthRetryStatus{RequestTimestamp: "2", AttemptID: operation, Outcome: authRetryPending},
		AuthCertificateReferences: []servitorv1alpha1.AuthCertificateReference{resolved},
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client}
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, cluster.Status.Operation); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.finishAuthRetry(context.Background(), cluster, servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "publisher-unavailable", ClearedCertificateReferences: []servitorv1alpha1.AuthCertificateReference{resolved}}); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: authResourceName(cluster)}, secret); err == nil {
		t.Fatal("unavailable auth retained delivery Secret")
	}
	binding := &rbacv1.RoleBinding{}
	attempt := pipeline.AuthAttemptResourceName(string(cluster.UID), operation)
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: attempt}, binding); err == nil {
		t.Fatal("unavailable auth retained publication authority")
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored); err != nil || stored.Status.Operation != nil || len(stored.Status.AuthCertificateReferences) != 0 || stored.Status.Auth == nil || len(stored.Status.Auth.ClearedCertificateReferences) != 0 {
		t.Fatalf("preclean result was not adopted atomically: status=%+v err=%v", stored.Status, err)
	}
}

func TestFinishAuthRetryRecoversPendingCertificateJournalBeforeSecretRevocation(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := eligibleAuthRetryCluster()
	operation := "auth-retry-current"
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: operation, Kind: "auth-retry", AuthAttemptID: operation}
	cluster.Status.AuthRetry = &servitorv1alpha1.AuthRetryStatus{RequestTimestamp: cluster.Spec.Lifecycle.AuthRetryRequestTimestamp, AttemptID: operation, Outcome: authRetryPending}
	certificate := servitorv1alpha1.AuthCertificateReference{ID: "certificate-123", AllocationUID: string(cluster.UID), AttemptID: operation}
	encoded, err := json.Marshal(certificate)
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: authResourceName(cluster), Namespace: cluster.Namespace, Labels: map[string]string{authUIDLabel: string(cluster.UID)}, Annotations: map[string]string{authOperationKey: operation, authPendingCertificateReferenceKey: string(encoded)}}, Data: map[string][]byte{authSecretDataName: []byte("private")}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster, secret).Build()
	reconciler := &Reconciler{Client: client}
	if _, err := reconciler.finishAuthRetry(context.Background(), cluster, servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "auth-state-failure"}); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Auth == nil || stored.Status.Auth.Reason != "certificate-cleanup-pending" || stored.Status.Auth.CleanupOutcome != "pending" || stored.Status.Auth.Certificate == nil || *stored.Status.Auth.Certificate != certificate || len(stored.Status.AuthCertificateReferences) != 1 || stored.Status.AuthCertificateReferences[0] != certificate {
		t.Fatalf("pending publication recovery was not retained: %+v", stored.Status)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: authResourceName(cluster)}, &corev1.Secret{}); err == nil {
		t.Fatal("recovered publication Secret was not revoked")
	}
}

func TestFinishAuthRetryRetainsHistoricalPendingCertificateJournal(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := eligibleAuthRetryCluster()
	attempt := "auth-retry-current"
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: "auth-retry-operation", Kind: "auth-retry", AuthAttemptID: attempt}
	cluster.Status.AuthRetry = &servitorv1alpha1.AuthRetryStatus{RequestTimestamp: cluster.Spec.Lifecycle.AuthRetryRequestTimestamp, AttemptID: attempt, Outcome: authRetryPending}
	historical := servitorv1alpha1.AuthCertificateReference{ID: "certificate-previous", AllocationUID: string(cluster.UID), AttemptID: "apply-previous"}
	status := servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "certificate-cleanup-pending", CleanupOutcome: "pending", CleanupReason: "transport", AttemptID: attempt, Certificate: &historical}
	if !validAdoptedAuth(status, *cluster.Status.ResolvedOptions, string(cluster.UID), time.Now()) {
		t.Fatal("controller rejected a valid historical pending certificate")
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster).Build()
	reconciler := &Reconciler{Client: client}
	if _, err := reconciler.finishAuthRetry(context.Background(), cluster, status); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Auth == nil || !reflect.DeepEqual(stored.Status.Auth.Certificate, &historical) || len(stored.Status.AuthCertificateReferences) != 1 || stored.Status.AuthCertificateReferences[0] != historical {
		t.Fatalf("historical certificate journal was not retained: %+v", stored.Status)
	}
}

func TestTrackAuthCertificateReferenceDeduplicatesAndBounds(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{}
	reference := &servitorv1alpha1.AuthCertificateReference{ID: "certificate", AllocationUID: "allocation", AttemptID: "attempt"}
	trackAuthCertificateReference(cluster, reference)
	trackAuthCertificateReference(cluster, reference)
	if len(cluster.Status.AuthCertificateReferences) != 1 {
		t.Fatalf("certificate references = %#v", cluster.Status.AuthCertificateReferences)
	}
	for index := 0; index < servitorv1alpha1.MaxAuthCertificateReferences; index++ {
		trackAuthCertificateReference(cluster, &servitorv1alpha1.AuthCertificateReference{ID: string(rune('b' + index)), AllocationUID: "allocation", AttemptID: string(rune('b' + index))})
	}
	if len(cluster.Status.AuthCertificateReferences) != servitorv1alpha1.MaxAuthCertificateReferences {
		t.Fatalf("certificate reference bound = %d", len(cluster.Status.AuthCertificateReferences))
	}
}
