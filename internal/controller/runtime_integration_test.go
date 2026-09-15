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
	"sync"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
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

type runtimeContractDelivery struct {
	mu    sync.Mutex
	calls int
	owner string
	data  []byte
}

func (d *runtimeContractDelivery) DeliverKubeconfig(_ context.Context, owner, _ string, data []byte) error {
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
		name := pipeline.AuthResourceName(string(cluster.UID))
		for _, object := range []client.Object{&corev1.Secret{}, &corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}} {
			if err := admin.Get(ctx, types.NamespacedName{Namespace: runtimeContractNamespace, Name: name}, object); err != nil {
				return fmt.Errorf("get %T: %w", object, err)
			}
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
		if err := admin.Get(ctx, cleanupKey, cluster); err != nil {
			return err
		}
		if cluster.Status.Phase != servitorv1alpha1.PhaseCleanupComplete {
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
	deadline := metav1.NewTime(time.Now().UTC().Add(time.Minute))
	cluster.Status = servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseAwaitingApproval,
		ResolvedOptions:   contractResolvedOptions("publication-cluster"),
		LifecycleSnapshot: contractLifecycleSnapshot(true),
		Backend:           &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "publication.tfstate", Region: "us-south", Endpoint: "https://s3.example.invalid"},
		ExecutionImage:    "registry.example.invalid/task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Recovery:          contractRecovery("publication-cluster"),
		ReviewDeadline:    &deadline,
		ReviewGeneration:  cluster.Generation,
	}
	if err := kube.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, key, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.Lifecycle.Approval = "approved"
	if err := kube.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	return key
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
	secret := contractAuthSecret(cluster, []byte("synthetic-kubeconfig"))
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
	cluster.Status = servitorv1alpha1.ServitorClusterStatus{
		Phase:             servitorv1alpha1.PhaseCleanupPending,
		ResolvedOptions:   contractResolvedOptions("cleanup-cluster"),
		LifecycleSnapshot: contractLifecycleSnapshot(true),
		CleanupRequested:  true,
		Cleanup: &servitorv1alpha1.CleanupStatus{
			Reason:      servitorv1alpha1.CleanupReasonExplicit,
			RequestedAt: metav1.Now(),
		},
	}
	if err := kube.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	name := pipeline.AuthResourceName(string(cluster.UID))
	labels := map[string]string{authUIDLabel: string(cluster.UID)}
	secret := contractAuthSecret(cluster, nil)
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: runtimeContractNamespace, Labels: labels}, AutomountServiceAccountToken: boolPointer(false)}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: runtimeContractNamespace, Labels: labels}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{name}, Verbs: []string{"get", "update", "patch"}}}}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: runtimeContractNamespace, Labels: labels}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: runtimeContractNamespace}}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}}
	for _, object := range []client.Object{secret, serviceAccount, role, binding} {
		if err := kube.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}

	runLabels := map[string]string{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: "plan"}
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
		UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default", Zone: "us-south-1", Flavor: "bx2.4x16"},
		Platform:    "openshift",
		ClusterName: name,
		Region:      "us-south",
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
		Values:       servitorv1alpha1.RecoveryValues{ClusterName: clusterName, ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 1, Zone: "us-south-1", Flavor: "bx2.4x16"},
		TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
}

func contractAuthSecret(cluster *servitorv1alpha1.ServitorCluster, data []byte) *corev1.Secret {
	name := pipeline.AuthResourceName(string(cluster.UID))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: runtimeContractNamespace,
		Labels: map[string]string{authUIDLabel: string(cluster.UID)}, Annotations: map[string]string{authOperationKey: applyID(string(cluster.UID))},
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
