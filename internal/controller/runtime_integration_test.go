//go:build integration

package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

const runtimeContractNamespace = "servitor-runtime-contract"

type runtimeContractResponder struct {
	mu                   sync.Mutex
	thread               string
	manualThread         string
	manualReplies        int
	failingThread        string
	failingThreadReplies int
	responses            []slackbot.Response
}

func (r *runtimeContractResponder) Reply(_ context.Context, response slackbot.Response) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if response.ThreadTimestamp == r.failingThread {
		r.failingThreadReplies++
		if r.failingThreadReplies > 1 {
			return errors.New("synthetic Slack delivery failure")
		}
		return nil
	}
	if r.thread == "" || r.thread == response.ThreadTimestamp {
		r.responses = append(r.responses, response)
	}
	if r.manualThread == response.ThreadTimestamp {
		r.manualReplies++
	}
	return nil
}

func (r *runtimeContractResponder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.responses)
}

func (r *runtimeContractResponder) manualCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.manualReplies
}

func (r *runtimeContractResponder) failureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return max(r.failingThreadReplies-1, 0)
}

type runtimeContractDelivery struct {
	mu    sync.Mutex
	calls int
	owner string
	data  []byte
}

func (d *runtimeContractDelivery) DeliverAuthBundle(_ context.Context, owner, _, _, _ string, data, _ []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	d.owner = owner
	d.data = append([]byte(nil), data...)
	return nil
}

func (d *runtimeContractDelivery) result() (int, string, []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls, d.owner, append([]byte(nil), d.data...)
}

func TestRuntimeContract(t *testing.T) {
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	testEnvironment := &envtest.Environment{
		Scheme:                   scheme,
		CRDDirectoryPaths:        []string{filepath.Join(repositoryRoot(t), "config", "crd", "bases")},
		ErrorIfCRDPathMissing:    true,
		CRDs:                     []*apiextensionsv1.CustomResourceDefinition{minimalTektonCRD("pipelineruns", "pipelinerun", "PipelineRun", "PipelineRunList"), minimalTektonCRD("taskruns", "taskrun", "TaskRun", "TaskRunList")},
		ControlPlaneStartTimeout: 30 * time.Second,
		ControlPlaneStopTimeout:  30 * time.Second,
		AttachControlPlaneOutput: false,
	}
	adminConfig, err := testEnvironment.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnvironment.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	admin, err := client.New(adminConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: runtimeContractNamespace}}); err != nil {
		t.Fatal(err)
	}
	installControllerRBAC(t, ctx, admin)
	verifyHeadlampRuntimePersistence(t, ctx, adminConfig, runtimeContractNamespace)

	publicationKey := seedPublicationContract(t, ctx, admin)
	deliveryKey := seedDeliveryContract(t, ctx, admin)
	cleanupKey, cleanupObjects := seedCleanupContract(t, ctx, admin)

	user, err := testEnvironment.AddUser(envtest.User{
		Name: "system:serviceaccount:" + runtimeContractNamespace + ":servitor-controller",
		Groups: []string{
			"system:authenticated",
			"system:serviceaccounts",
			"system:serviceaccounts:" + runtimeContractNamespace,
		},
	}, &rest.Config{QPS: 100, Burst: 200})
	if err != nil {
		t.Fatalf("create controller test user: %v", err)
	}
	manager, err := ctrl.NewManager(user.Config(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          true,
		LeaderElectionID:        "servitor-runtime-contract",
		LeaderElectionNamespace: runtimeContractNamespace,
		Cache:                   cache.Options{DefaultNamespaces: map[string]cache.Config{runtimeContractNamespace: {}}},
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  "0",
	})
	if err != nil {
		t.Fatalf("create restricted manager: %v", err)
	}
	delivery := &runtimeContractDelivery{}
	reconciler := NewReconciler(manager, Config{Namespace: runtimeContractNamespace}, nil, delivery)
	if err := reconciler.SetupWithManager(manager); err != nil {
		t.Fatalf("configure reconciler: %v", err)
	}

	managerContext, stopManager := context.WithCancel(ctx)
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(managerContext) }()
	t.Cleanup(func() {
		stopManager()
		select {
		case err := <-managerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("stop manager: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("manager did not stop")
		}
	})

	syncContext, cancelSync := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSync()
	if !manager.GetCache().WaitForCacheSync(syncContext) {
		t.Fatalf("manager cache did not synchronize: %v", managerResult(managerDone))
	}

	eventually(t, managerDone, "publisher resources and Applying status", func() error {
		cluster := &servitorv1alpha1.ServitorCluster{}
		if err := admin.Get(ctx, publicationKey, cluster); err != nil {
			return err
		}
		if cluster.Status.Phase != servitorv1alpha1.PhaseApplying {
			return fmt.Errorf("phase is %q", cluster.Status.Phase)
		}
		operation := cluster.Status.Operation
		if operation == nil || operation.ID != applyID(string(cluster.UID)) || operation.AuthAttemptID == "" || operation.AuthAttemptID == operation.ID {
			return fmt.Errorf("operation identity = %#v", operation)
		}
		var runs tektonv1.PipelineRunList
		if err := admin.List(ctx, &runs, client.InNamespace(runtimeContractNamespace), client.MatchingLabels{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: operation.ID}); err != nil {
			return err
		}
		if len(runs.Items) != 1 || runs.Items[0].Name != operation.PipelineRunName {
			return fmt.Errorf("operation PipelineRuns = %#v", runs.Items)
		}
		secretName := pipeline.AuthResourceName(string(cluster.UID))
		attemptName := pipeline.AuthAttemptResourceName(string(cluster.UID), operation.AuthAttemptID)
		secret := &corev1.Secret{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: runtimeContractNamespace, Name: secretName}, secret); err != nil {
			return fmt.Errorf("get Secret: %w", err)
		}
		if secret.Labels[authUIDLabel] != string(cluster.UID) || secret.Annotations[authOperationKey] != operation.AuthAttemptID {
			return fmt.Errorf("Secret provenance = %#v", secret.ObjectMeta)
		}
		serviceAccount := &corev1.ServiceAccount{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: runtimeContractNamespace, Name: attemptName}, serviceAccount); err != nil {
			return fmt.Errorf("get ServiceAccount: %w", err)
		}
		if serviceAccount.Labels[authUIDLabel] != string(cluster.UID) || serviceAccount.AutomountServiceAccountToken == nil || *serviceAccount.AutomountServiceAccountToken {
			return fmt.Errorf("ServiceAccount = %#v", serviceAccount)
		}
		role := &rbacv1.Role{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: runtimeContractNamespace, Name: attemptName}, role); err != nil {
			return fmt.Errorf("get Role: %w", err)
		}
		if role.Labels[authUIDLabel] != string(cluster.UID) || role.Annotations[authOperationKey] != operation.AuthAttemptID || !publisherRoleMatches(role, secretName, cluster.Name, false) {
			return fmt.Errorf("Role = %#v", role)
		}
		binding := &rbacv1.RoleBinding{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: runtimeContractNamespace, Name: attemptName}, binding); err != nil {
			return fmt.Errorf("get RoleBinding: %w", err)
		}
		if binding.Labels[authUIDLabel] != string(cluster.UID) || len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != attemptName || binding.RoleRef.APIGroup != rbacv1.GroupName || binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != attemptName {
			return fmt.Errorf("RoleBinding = %#v", binding)
		}
		return nil
	})

	eventually(t, managerDone, "consumed auth delivery", func() error {
		cluster := &servitorv1alpha1.ServitorCluster{}
		if err := admin.Get(ctx, deliveryKey, cluster); err != nil {
			return err
		}
		calls, owner, data := delivery.result()
		if calls != 1 || owner != "UDELIVERY" || string(data) != "synthetic-kubeconfig" {
			return fmt.Errorf("delivery calls=%d owner=%q data=%q", calls, owner, data)
		}
		if cluster.Status.AuthDelivery == nil || cluster.Status.AuthDelivery.Outcome != authDeliveryDelivered {
			return fmt.Errorf("delivery status is %#v", cluster.Status.AuthDelivery)
		}
		return nil
	})

	eventually(t, managerDone, "terminal run and publisher cleanup", func() error {
		cluster := &servitorv1alpha1.ServitorCluster{}
		if err := admin.Get(ctx, cleanupKey, cluster); err != nil && !apierrors.IsNotFound(err) {
			return err
		} else if err == nil && cluster.Status.Phase != servitorv1alpha1.PhaseCleanupComplete {
			return fmt.Errorf("phase is %q", cluster.Status.Phase)
		}
		for _, object := range cleanupObjects {
			if err := admin.Get(ctx, client.ObjectKeyFromObject(object), object.DeepCopyObject().(client.Object)); !apierrors.IsNotFound(err) {
				return fmt.Errorf("%T %q still exists: %v", object, object.GetName(), err)
			}
		}
		return nil
	})
}

func seedPublicationContract(t *testing.T, ctx context.Context, kube client.Client) types.NamespacedName {
	t.Helper()
	cluster := contractCluster("publication")
	if err := kube.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(cluster)
	if err := kube.Get(ctx, key, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Status = servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhasePending,
		ResolvedOptions:   contractResolvedOptions("publication-cluster"),
		LifecycleSnapshot: contractLifecycleSnapshot(true),
		Backend:           &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "publication.tfstate", Region: "us-south", Endpoint: "https://s3.example.invalid"},
		ExecutionImage:    "registry.example.invalid/task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Recovery:          contractRecovery("publication-cluster"),
	}
	if err := kube.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestStrictNetworkUserOptionsAdmission(t *testing.T) {
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	testEnvironment := &envtest.Environment{
		Scheme:                scheme,
		CRDDirectoryPaths:     []string{filepath.Join(repositoryRoot(t), "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := testEnvironment.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnvironment.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	admin, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const namespace = "servitor-strict-admission"
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	verifyRemovedNetworkUserOptionsRejected(t, ctx, config, namespace)
	verifyResourceGroupUserOptionRejected(t, ctx, config, namespace)
}

func TestApprovalContractAdmission(t *testing.T) {
	scheme, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &envtest.Environment{Scheme: scheme, CRDDirectoryPaths: []string{filepath.Join(repositoryRoot(t), "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	configuration, err := environment.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	ctx := context.Background()
	const namespace = "servitor-approval-contract-admission"
	admin, err := client.New(configuration, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := admin.Get(ctx, types.NamespacedName{Name: "servitorclusters.servitor.bevicted.github.io"}, crd); err != nil {
		t.Fatal(err)
	}
	properties := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties
	for _, test := range []struct {
		properties map[string]apiextensionsv1.JSONSchemaProps
		fields     []string
	}{
		{properties["spec"].Properties["lifecycle"].Properties, []string{"autoApprove", "approval"}},
		{properties["status"].Properties, []string{"review", "reviewDeadline", "reviewGeneration", "reviewApproval"}},
	} {
		for _, field := range test.fields {
			if _, exists := test.properties[field]; exists {
				t.Fatalf("CRD still advertises %s", field)
			}
		}
	}
	resources := dynamic.NewForConfigOrDie(configuration).Resource(schema.GroupVersionResource{Group: "servitor.bevicted.github.io", Version: "v1alpha1", Resource: "servitorclusters"}).Namespace(namespace)
	base := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "servitor.bevicted.github.io/v1alpha1", "kind": "ServitorCluster", "metadata": map[string]any{"name": "status-contract"}, "spec": map[string]any{"slack": map[string]any{"ownerID": "U1", "channelID": "C1", "threadTimestamp": "1.2"}, "lifecycle": map[string]any{"initialLeaseSeconds": int64(3600), "retrySeconds": []any{int64(60)}}}}}
	for field, value := range map[string]any{"autoApprove": true, "approval": "approved"} {
		t.Run("lifecycle-"+field, func(t *testing.T) {
			candidate := base.DeepCopy()
			candidate.SetName("removed-" + strings.ToLower(field))
			if err := unstructured.SetNestedField(candidate.Object, value, "spec", "lifecycle", field); err != nil {
				t.Fatal(err)
			}
			if _, err := resources.Create(ctx, candidate, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict}); !apierrors.IsBadRequest(err) || !strings.Contains(err.Error(), "unknown field") || !strings.Contains(err.Error(), field) {
				t.Fatalf("strict lifecycle.%s error = %v, want unknown-field rejection", field, err)
			}
			stored, err := resources.Create(ctx, candidate, metav1.CreateOptions{FieldValidation: metav1.FieldValidationIgnore})
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := unstructured.NestedFieldNoCopy(stored.Object, "spec", "lifecycle", field); err != nil || found {
				t.Fatalf("obsolete lifecycle.%s survived pruning: found=%t err=%v", field, found, err)
			}
		})
	}
	stored, err := resources.Create(ctx, base, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
	if err != nil {
		t.Fatal(err)
	}
	current := &servitorv1alpha1.ServitorCluster{}
	if err := admin.Get(ctx, types.NamespacedName{Namespace: namespace, Name: stored.GetName()}, current); err != nil {
		t.Fatal(err)
	}
	if err := current.Spec.Validate(); err != nil {
		t.Fatalf("reduced CR spec is invalid: %v", err)
	}
	for field, value := range map[string]any{"review": map[string]any{"resources": []any{}}, "reviewDeadline": "2026-09-29T12:00:00Z", "reviewGeneration": int64(1), "reviewApproval": "approved"} {
		t.Run("status-"+field, func(t *testing.T) {
			candidate := stored.DeepCopy()
			candidate.Object["status"] = map[string]any{field: value}
			if _, err := resources.UpdateStatus(ctx, candidate, metav1.UpdateOptions{FieldValidation: metav1.FieldValidationStrict}); !apierrors.IsBadRequest(err) || !strings.Contains(err.Error(), "unknown field") || !strings.Contains(err.Error(), field) {
				t.Fatalf("strict status.%s error = %v, want unknown-field rejection", field, err)
			}
			stored, err = resources.UpdateStatus(ctx, candidate, metav1.UpdateOptions{FieldValidation: metav1.FieldValidationIgnore})
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := unstructured.NestedFieldNoCopy(stored.Object, "status", field); err != nil || found {
				t.Fatalf("obsolete status.%s survived pruning: found=%t err=%v", field, found, err)
			}
		})
	}
	for _, test := range []struct{ field, value string }{
		{"phase", "AwaitingApproval"}, {"cleanup", "Rejected"}, {"cleanup", "ReviewExpired"},
	} {
		enum := properties["status"].Properties[test.field].Enum
		status := map[string]any{test.field: test.value}
		if test.field == "cleanup" {
			enum = properties["status"].Properties["cleanup"].Properties["reason"].Enum
			status["cleanup"] = map[string]any{"reason": test.value}
		}
		for _, item := range enum {
			if string(item.Raw) == strconv.Quote(test.value) {
				t.Fatalf("CRD advertises removed enum %s", test.value)
			}
		}
		candidate := stored.DeepCopy()
		candidate.Object["status"] = status
		if _, err := resources.UpdateStatus(ctx, candidate, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "Unsupported value") {
			t.Fatalf("removed enum %s error = %v, want enum rejection", test.value, err)
		}
	}
	for _, phase := range []string{servitorv1alpha1.PhasePending, servitorv1alpha1.PhasePlanning, servitorv1alpha1.PhaseApplying, servitorv1alpha1.PhaseReady, servitorv1alpha1.PhaseCleanupPending, servitorv1alpha1.PhaseCleanupComplete, servitorv1alpha1.PhaseUnresolved} {
		candidate := stored.DeepCopy()
		candidate.Object["status"] = map[string]any{"phase": phase}
		stored, err = resources.UpdateStatus(ctx, candidate, metav1.UpdateOptions{FieldValidation: metav1.FieldValidationStrict})
		if err != nil {
			t.Fatalf("retained phase %s rejected: %v", phase, err)
		}
	}
}

func verifyHeadlampRuntimePersistence(t *testing.T, ctx context.Context, config *rest.Config, namespace string) {
	t.Helper()
	clusters, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	resources := clusters.Resource(schema.GroupVersionResource{Group: "servitor.bevicted.github.io", Version: "v1alpha1", Resource: "servitorclusters"}).Namespace(namespace)
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "servitor.bevicted.github.io/v1alpha1", "kind": "ServitorCluster",
		"metadata": map[string]any{"name": "headlamp-round-trip"},
		"spec": map[string]any{
			"slack":       map[string]any{"ownerID": "UHEADLAMP", "channelID": "CCHANNEL", "threadTimestamp": "1.2"},
			"userOptions": map[string]any{"headlamp": true, "version": "1.31"},
			"lifecycle":   map[string]any{"initialLeaseSeconds": int64(3600), "retrySeconds": []any{int64(60)}},
		},
	}}
	stored, err := resources.Create(ctx, cluster, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]any{
		"resolvedOptions": map[string]any{"headlamp": true},
		"recovery":        map[string]any{"values": map[string]any{"headlamp": true}},
	}
	if err := unstructured.SetNestedMap(stored.Object, status, "status"); err != nil {
		t.Fatal(err)
	}
	if _, err := resources.UpdateStatus(ctx, stored, metav1.UpdateOptions{FieldValidation: metav1.FieldValidationStrict}); err != nil {
		t.Fatal(err)
	}
	stored, err = resources.Get(ctx, stored.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, fields := range [][]string{{"spec", "userOptions", "headlamp"}, {"status", "resolvedOptions", "headlamp"}, {"status", "recovery", "values", "headlamp"}} {
		value, found, err := unstructured.NestedBool(stored.Object, fields...)
		if err != nil || !found || !value {
			t.Fatalf("Headlamp %v persisted as value=%t found=%t err=%v", fields, value, found, err)
		}
	}
	if err := unstructured.SetNestedField(stored.Object, false, "spec", "userOptions", "headlamp"); err != nil {
		t.Fatal(err)
	}
	if _, err := resources.Update(ctx, stored, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) {
		t.Fatalf("Headlamp immutable update error = %v, want invalid", err)
	}
	if err := resources.Delete(ctx, "headlamp-round-trip", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

func seedDeliveryContract(t *testing.T, ctx context.Context, kube client.Client) types.NamespacedName {
	t.Helper()
	cluster := contractCluster("delivery")
	cluster.Spec.Slack.OwnerID = "UDELIVERY"
	cluster.Spec.Lifecycle.AuthRequestTimestamp = "1710000000.000100"
	if err := kube.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(cluster)
	if err := kube.Get(ctx, key, cluster); err != nil {
		t.Fatal(err)
	}
	expiry := metav1.NewTime(time.Now().UTC().Add(time.Hour))
	cluster.Status = servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseReady,
		ResolvedOptions:   contractResolvedOptions("delivery-cluster"),
		LifecycleSnapshot: contractLifecycleSnapshot(true),
		PublicAuth:        &servitorv1alpha1.PublicAuthStatus{Availability: "available"},
		LeaseExpiresAt:    &expiry,
		Ready:             &servitorv1alpha1.ReadySummary{},
	}
	if err := kube.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	secret := contractAuthSecret(cluster, applyID(string(cluster.UID)), []byte("synthetic-kubeconfig"))
	if err := kube.Create(ctx, secret); err != nil {
		t.Fatal(err)
	}
	return key
}

func seedCleanupContract(t *testing.T, ctx context.Context, kube client.Client) (types.NamespacedName, []client.Object) {
	t.Helper()
	cluster := contractCluster("cleanup")
	if err := kube.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(cluster)
	if err := kube.Get(ctx, key, cluster); err != nil {
		t.Fatal(err)
	}
	operationID := "cleanup-operation"
	attemptID := "cleanup-attempt"
	cluster.Status = servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseCleanupPending,
		ResolvedOptions:   contractResolvedOptions("cleanup-cluster"),
		LifecycleSnapshot: contractLifecycleSnapshot(true),
		Operation:         &servitorv1alpha1.OperationReference{ID: operationID, Kind: "apply", AuthAttemptID: attemptID, PipelineRunName: "cleanup-pipeline", StartedAt: metav1.Now(), Dispatched: true},
		CleanupRequested:  true,
		Cleanup: &servitorv1alpha1.CleanupStatus{
			Reason:      servitorv1alpha1.CleanupReasonExplicit,
			RequestedAt: metav1.Now(),
		},
	}
	if err := kube.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	secretName := pipeline.AuthResourceName(string(cluster.UID))
	attemptName := pipeline.AuthAttemptResourceName(string(cluster.UID), attemptID)
	labels := map[string]string{authUIDLabel: string(cluster.UID)}
	secret := contractAuthSecret(cluster, attemptID, nil)
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: attemptName, Namespace: runtimeContractNamespace, Labels: labels}, AutomountServiceAccountToken: boolPointer(false)}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: attemptName, Namespace: runtimeContractNamespace, Labels: labels, Annotations: map[string]string{authOperationKey: attemptID}}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{secretName}, Verbs: []string{"get", "update", "patch"}}}}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: attemptName, Namespace: runtimeContractNamespace, Labels: labels}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: attemptName, Namespace: runtimeContractNamespace}}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: attemptName}}
	for _, object := range []client.Object{secret, serviceAccount, role, binding} {
		if err := kube.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}

	runLabels := map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: operationID}
	pipelineRun := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "cleanup-pipeline", Namespace: runtimeContractNamespace, Labels: runLabels}}
	if err := kube.Create(ctx, pipelineRun); err != nil {
		t.Fatal(err)
	}
	pipelineRun.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	if err := kube.Status().Update(ctx, pipelineRun); err != nil {
		t.Fatal(err)
	}
	taskRun := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "cleanup-task", Namespace: runtimeContractNamespace, Labels: runLabels}}
	if err := kube.Create(ctx, taskRun); err != nil {
		t.Fatal(err)
	}
	taskRun.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	if err := kube.Status().Update(ctx, taskRun); err != nil {
		t.Fatal(err)
	}
	return key, []client.Object{secret, serviceAccount, role, binding, pipelineRun, taskRun}
}

func contractCluster(name string) *servitorv1alpha1.ServitorCluster {
	return &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: runtimeContractNamespace, Finalizers: []string{servitorv1alpha1.CleanupFinalizer}},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack: servitorv1alpha1.SlackIdentity{OwnerID: "UOWNER", ChannelID: "CCHANNEL", ThreadTimestamp: "1.2"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{
				InitialLeaseSeconds: 3600,
				RetrySeconds:        []int64{60},
			},
		},
	}
}

func contractResolvedOptions(name string) *servitorv1alpha1.ResolvedOptions {
	return &servitorv1alpha1.ResolvedOptions{
		UserOptions:   servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22", Zone: "us-south-1", Flavor: "bx2.4x16"},
		ResourceGroup: "Default",
		Platform:      "openshift",
		ClusterName:   name,
		Region:        "us-south",
		Network:       runtimeContractNetworkBindings()["target"],
	}
}

func runtimeContractNetworkBindings() map[string]servitorv1alpha1.FrozenNetwork {
	return map[string]servitorv1alpha1.FrozenNetwork{
		"target": {
			BindingID:       "runtime-contract-network",
			AccountID:       "runtime-contract-account",
			VPCID:           "vpc",
			SubnetID:        "subnet",
			PublicGatewayID: "gateway",
			Zone:            "us-south-1",
		},
	}
}

func contractLifecycleSnapshot(publicAuth bool) *servitorv1alpha1.LifecycleSnapshot {
	return &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, PublicAuthEligible: publicAuth}
}

func contractRecovery(clusterName string) *servitorv1alpha1.RecoveryMetadata {
	return &servitorv1alpha1.RecoveryMetadata{
		Version: 1,
		Target:  "target",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values:       servitorv1alpha1.RecoveryValues{ClusterName: clusterName, ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 1, Zone: "us-south-1", Flavor: "bx2.4x16", AccountID: "runtime-contract-account", VPCRegion: "us-south", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}},
		TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
}

func contractAuthSecret(cluster *servitorv1alpha1.ServitorCluster, attemptID string, data []byte) *corev1.Secret {
	name := pipeline.AuthResourceName(string(cluster.UID))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: runtimeContractNamespace,
		Labels: map[string]string{authUIDLabel: string(cluster.UID)}, Annotations: map[string]string{authOperationKey: attemptID},
	}}
	if data != nil {
		secret.Data = map[string][]byte{authSecretDataName: append([]byte(nil), data...)}
	}
	return secret
}

func installControllerRBAC(t *testing.T, ctx context.Context, kube client.Client) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	documents := bytes.Split(data, []byte("\n---\n"))
	if len(documents) != 2 {
		t.Fatalf("controller RBAC manifest has %d documents, want 2", len(documents))
	}
	role := &rbacv1.Role{}
	if err := yaml.Unmarshal(documents[0], role); err != nil {
		t.Fatal(err)
	}
	role.Namespace = runtimeContractNamespace
	if err := kube.Create(ctx, role); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{}
	if err := yaml.Unmarshal(documents[1], binding); err != nil {
		t.Fatal(err)
	}
	binding.Namespace = runtimeContractNamespace
	for index := range binding.Subjects {
		if binding.Subjects[index].Kind == "ServiceAccount" {
			binding.Subjects[index].Namespace = runtimeContractNamespace
		}
	}
	if err := kube.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
}

func verifyRemovedNetworkUserOptionsRejected(t *testing.T, ctx context.Context, config *rest.Config, namespace string) {
	t.Helper()
	clusters, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	resources := clusters.Resource(schema.GroupVersionResource{
		Group: "servitor.bevicted.github.io", Version: "v1alpha1", Resource: "servitorclusters",
	}).Namespace(namespace)
	for _, test := range []struct {
		name  string
		field string
		value any
	}{
		{name: "vpc-id", field: "vpcID", value: "untrusted-vpc"},
		{name: "subnet-ids", field: "subnetIDs", value: []any{"untrusted-subnet"}},
		{name: "public-gateway-ids", field: "publicGatewayIDs", value: []any{"untrusted-gateway"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := map[string]any{"version": "4.22", test.field: test.value}
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "servitor.bevicted.github.io/v1alpha1",
				"kind":       "ServitorCluster",
				"metadata":   map[string]any{"name": "reject-" + test.name},
				"spec": map[string]any{
					"slack":       map[string]any{"ownerID": "U1", "channelID": "C1", "threadTimestamp": "1.2"},
					"userOptions": options,
					"lifecycle":   map[string]any{"initialLeaseSeconds": int64(3600), "retrySeconds": []any{int64(60)}},
				},
			}}
			_, err := resources.Create(ctx, cluster, metav1.CreateOptions{FieldValidation: metav1.FieldValidationStrict})
			if !apierrors.IsBadRequest(err) {
				t.Fatalf("strict direct CR with %s create error = %v, want bad request", test.field, err)
			}
			if _, err := resources.Get(ctx, cluster.GetName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("strict direct CR with %s was stored: %v", test.field, err)
			}

			cluster.SetName("pruned-" + test.name)
			if _, err := resources.Create(ctx, cluster, metav1.CreateOptions{}); err != nil {
				t.Fatalf("permissive direct CR with %s create error = %v", test.field, err)
			}
			stored, err := resources.Get(ctx, cluster.GetName(), metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get permissive direct CR with %s: %v", test.field, err)
			}
			if _, found, err := unstructured.NestedFieldNoCopy(stored.Object, "spec", "userOptions", test.field); err != nil || found {
				t.Fatalf("pruned user option %s persisted: found=%t err=%v", test.field, found, err)
			}
			if err := resources.Delete(ctx, cluster.GetName(), metav1.DeleteOptions{}); err != nil {
				t.Fatalf("delete permissive direct CR with %s: %v", test.field, err)
			}
		})
	}
}

func verifyResourceGroupUserOptionRejected(t *testing.T, ctx context.Context, config *rest.Config, namespace string) {
	t.Helper()
	clusters, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	resources := clusters.Resource(schema.GroupVersionResource{
		Group: "servitor.bevicted.github.io", Version: "v1alpha1", Resource: "servitorclusters",
	}).Namespace(namespace)
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "nonempty", value: "caller-selected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "servitor.bevicted.github.io/v1alpha1",
				"kind":       "ServitorCluster",
				"metadata":   map[string]any{"name": "reject-resource-group-" + test.name},
				"spec": map[string]any{
					"slack":       map[string]any{"ownerID": "U1", "channelID": "C1", "threadTimestamp": "1.2"},
					"userOptions": map[string]any{"version": "4.22", "resourceGroup": test.value},
					"lifecycle":   map[string]any{"initialLeaseSeconds": int64(3600), "retrySeconds": []any{int64(60)}},
				},
			}}
			_, err := resources.Create(ctx, cluster, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			if !apierrors.IsInvalid(err) {
				t.Fatalf("dry-run CR with resourceGroup %q create error = %v, want invalid", test.value, err)
			}
			if _, err := resources.Get(ctx, cluster.GetName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("dry-run CR with resourceGroup %q was stored: %v", test.value, err)
			}
		})
	}
}

func minimalTektonCRD(plural, singular, kind, listKind string) *apiextensionsv1.CustomResourceDefinition {
	preserveUnknownFields := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + ".tekton.dev"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "tekton.dev",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: plural, Singular: singular, Kind: kind, ListKind: listKind},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema:       &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserveUnknownFields}},
				Subresources: &apiextensionsv1.CustomResourceSubresources{Status: &apiextensionsv1.CustomResourceSubresourceStatus{}},
			}},
		},
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func eventually(t *testing.T, managerDone chan error, description string, check func() error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := managerResult(managerDone); err != nil {
			t.Fatalf("manager stopped while waiting for %s: %v", description, err)
		}
		if err := check(); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %v", description, lastErr)
}

func managerResult(managerDone chan error) error {
	select {
	case err := <-managerDone:
		managerDone <- err
		if err == nil {
			return errors.New("manager stopped without an error")
		}
		return err
	default:
		return nil
	}
}
