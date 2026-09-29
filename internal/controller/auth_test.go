package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type publisherCacheUnavailableClient struct{ client.Client }

func (c publisherCacheUnavailableClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	switch object.(type) {
	case *corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding:
		return apierrors.NewServiceUnavailable("publisher informer cache unavailable")
	default:
		return c.Client.Get(ctx, key, object, options...)
	}
}

func TestApprovalResumesPartialPublisherResourcesWithoutInformerCache(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{servitorv1alpha1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Minute))
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "allocation-uid", Generation: 2},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack:     servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
		},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhaseAwaitingApproval,
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{},
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}, PublicAuthEligible: true},
			ReviewDeadline:    &deadline,
			ReviewGeneration:  1,
		},
	}
	name := authResourceName(cluster)
	operation := applyID(string(cluster.UID))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: map[string]string{authUIDLabel: string(cluster.UID)}, Annotations: map[string]string{authOperationKey: operation}}}
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: map[string]string{authUIDLabel: string(cluster.UID)}}, AutomountServiceAccountToken: boolPointer(false)}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(cluster, secret, serviceAccount).Build()
	approved := &servitorv1alpha1.ServitorCluster{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, approved); err != nil {
		t.Fatal(err)
	}
	approved.Spec.Lifecycle.Approval = "approved"
	approved.Generation = approved.Status.ReviewGeneration + 1
	cached := publisherCacheUnavailableClient{Client: base}
	reconciler := &Reconciler{Client: cached, DirectReader: base, Config: Config{Namespace: "ns"}, Now: func() time.Time { return now }}
	if _, err := reconciler.reconcileApproval(context.Background(), approved); err != nil {
		t.Fatal(err)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != servitorv1alpha1.PhaseApplying || !stored.Status.ApplyDispatched || stored.Status.Operation == nil || stored.Status.Operation.ID != operation {
		t.Fatalf("approval did not persist Applying after partial publisher creation: %+v", stored.Status)
	}
	attemptName := pipeline.AuthAttemptResourceName(string(cluster.UID), initialAuthAttemptID(string(cluster.UID), operation, ""))
	for _, object := range []client.Object{&rbacv1.Role{}, &rbacv1.RoleBinding{}} {
		if err := base.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: attemptName}, object); err != nil {
			t.Fatalf("approval did not resume %T creation: %v", object, err)
		}
	}
}

func TestGeneratedPublisherRolePermissionsAreHeldByController(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: "allocation-uid"}}
	name := authResourceName(cluster)
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	if err := (&Reconciler{Client: client}).ensurePublisherRole(context.Background(), cluster, name, name, applyID(string(cluster.UID)), false, map[string]string{authUIDLabel: string(cluster.UID)}); err != nil {
		t.Fatal(err)
	}
	publisher := &rbacv1.Role{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: name}, publisher); err != nil {
		t.Fatal(err)
	}
	if len(publisher.Rules) != 1 || !publisherRuleAllows(publisher.Rules[0], name, "get") || !publisherRuleAllows(publisher.Rules[0], name, "update") || !publisherRuleAllows(publisher.Rules[0], name, "patch") {
		t.Fatalf("generated publisher Role does not have its expected Secret permissions: %#v", publisher.Rules)
	}
	for _, verb := range []string{"create", "list", "watch"} {
		if publisherRuleAllows(publisher.Rules[0], name, verb) {
			t.Errorf("generated publisher Role must not allow Secret %s", verb)
		}
	}
	if publisherRuleAllows(publisher.Rules[0], "other-allocation-secret", "get") {
		t.Error("generated publisher Role can read another allocation Secret")
	}

	data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var parent controllerRoleManifest
	if err := yaml.Unmarshal(data, &parent); err != nil {
		t.Fatal(err)
	}
	for _, publisherRule := range publisher.Rules {
		for _, verb := range publisherRule.Verbs {
			if !parentAllows(parent.Rules, publisherRule, verb) {
				t.Errorf("controller Role does not hold delegated publisher permission %q on %q", verb, publisherRule.Resources)
			}
		}
	}
}

func TestReplacementAllocationUIDCannotReusePublishedSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &Reconciler{Client: client}
	old := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: "old-uid"}, Status: servitorv1alpha1.ServitorClusterStatus{LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true}}}
	replacement := old.DeepCopy()
	replacement.UID = "new-uid"
	for _, cluster := range []*servitorv1alpha1.ServitorCluster{old, replacement} {
		if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, &servitorv1alpha1.OperationReference{ID: applyID(string(cluster.UID)), Kind: "apply"}); err != nil {
			t.Fatal(err)
		}
	}
	if authResourceName(old) == authResourceName(replacement) {
		t.Fatal("replacement allocation reused the prior publisher resource name")
	}
	for _, cluster := range []*servitorv1alpha1.ServitorCluster{old, replacement} {
		secret := &corev1.Secret{}
		if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: authResourceName(cluster)}, secret); err != nil {
			t.Fatal(err)
		}
		if err := validateAuthSecret(secret, cluster, applyID(string(cluster.UID))); err != nil {
			t.Fatalf("replacement Secret ownership: %v", err)
		}
	}
}

func publisherRuleAllows(rule rbacv1.PolicyRule, secretName, verb string) bool {
	return containsString(rule.APIGroups, "") && containsString(rule.Resources, "secrets") && containsString(rule.ResourceNames, secretName) && containsString(rule.Verbs, verb)
}

type controllerRoleManifest struct {
	Rules []controllerRoleRule `yaml:"rules"`
}

type controllerRoleRule struct {
	APIGroups     []string `yaml:"apiGroups"`
	Resources     []string `yaml:"resources"`
	ResourceNames []string `yaml:"resourceNames"`
	Verbs         []string `yaml:"verbs"`
}

func parentAllows(parentRules []controllerRoleRule, publisherRule rbacv1.PolicyRule, verb string) bool {
	for _, parentRule := range parentRules {
		if !containsString(parentRule.APIGroups, "") || !containsString(parentRule.Resources, "secrets") || !containsString(parentRule.Verbs, verb) {
			continue
		}
		if len(parentRule.ResourceNames) == 0 {
			return true
		}
		for _, name := range publisherRule.ResourceNames {
			if !containsString(parentRule.ResourceNames, name) {
				return false
			}
		}
		return true
	}
	return false
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func TestPublisherRoleUsesOperationKindForRetryFenceRead(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: "allocation"}, Status: servitorv1alpha1.ServitorClusterStatus{LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true}}}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &Reconciler{Client: client}
	applyAttempt := "auth-retry-looking-apply-attempt"
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, &servitorv1alpha1.OperationReference{ID: "apply-operation", Kind: "apply", AuthAttemptID: applyAttempt}); err != nil {
		t.Fatal(err)
	}
	if attemptAllows(t, client, cluster, applyAttempt, "servitorclusters", cluster.Name, "get") {
		t.Fatal("apply attempt received retry fence read authority")
	}
	retryAttempt := "retry-attempt"
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, &servitorv1alpha1.OperationReference{ID: "non-prefix-operation", Kind: "auth-retry", AuthAttemptID: retryAttempt}); err != nil {
		t.Fatal(err)
	}
	if !attemptAllows(t, client, cluster, retryAttempt, "servitorclusters", cluster.Name, "get") || attemptAllows(t, client, cluster, retryAttempt, "servitorclusters", "other", "get") {
		t.Fatal("retry attempt CR read scope is not exactly its allocation")
	}
}

func TestAuthAttemptRBACRevokesStaleIdentityAndScopesCurrentIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, rbacv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cluster", UID: "allocation"}, Status: servitorv1alpha1.ServitorClusterStatus{LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{AuthEligible: true}}}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &Reconciler{Client: client}
	stale, current := "retry-attempt-old", "retry-attempt-current"
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, &servitorv1alpha1.OperationReference{ID: "auth-retry-old", Kind: "auth-retry", AuthAttemptID: stale}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, &servitorv1alpha1.OperationReference{ID: "auth-retry-current", Kind: "auth-retry", AuthAttemptID: current}); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.revokeAuthAttempt(context.Background(), cluster, stale); err != nil {
		t.Fatal(err)
	}
	secret := authResourceName(cluster)
	if attemptAllows(t, client, cluster, stale, "secrets", secret, "update") {
		t.Fatal("stale identity retained Secret update authority after RoleBinding removal")
	}
	if !attemptAllows(t, client, cluster, current, "secrets", secret, "update") {
		t.Fatal("current identity cannot update its current delivery Secret")
	}
	if attemptAllows(t, client, cluster, current, "secrets", "other", "update") {
		t.Fatal("current identity can update another Secret")
	}
	if !attemptAllows(t, client, cluster, current, "servitorclusters", cluster.Name, "get") || attemptAllows(t, client, cluster, current, "servitorclusters", "other", "get") {
		t.Fatal("current retry identity CR read scope is not resourceName-bound")
	}
}

// attemptAllows models the exact RoleBinding subject, RoleRef, resourceName, and
// verb evaluation used by Kubernetes RBAC. The fake client supplies the policy
// objects while avoiding an unrelated API server dependency in this unit test.
func attemptAllows(t *testing.T, client client.Client, cluster *servitorv1alpha1.ServitorCluster, operation, resource, name, verb string) bool {
	t.Helper()
	attempt := pipeline.AuthAttemptResourceName(string(cluster.UID), operation)
	binding := &rbacv1.RoleBinding{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: attempt}, binding); err != nil {
		return false
	}
	if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != attempt || binding.RoleRef.APIGroup != rbacv1.GroupName || binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != attempt {
		return false
	}
	role := &rbacv1.Role{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: attempt}, role); err != nil {
		return false
	}
	for _, rule := range role.Rules {
		if containsString(rule.Resources, resource) && containsString(rule.ResourceNames, name) && containsString(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

func TestPublicAuthResourcesAreUIDBoundAndRevokedBeforePublisherRemoval(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	operation := applyID("new-allocation")
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: "new-allocation"}, Status: servitorv1alpha1.ServitorClusterStatus{LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{PublicAuthEligible: true}, Operation: &servitorv1alpha1.OperationReference{ID: operation}}}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &Reconciler{Client: client}
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, &servitorv1alpha1.OperationReference{ID: operation, Kind: "apply"}); err != nil {
		t.Fatal(err)
	}
	name := pipeline.AuthResourceName(string(cluster.UID))
	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, secret); err != nil {
		t.Fatal(err)
	}
	if secret.Labels[authUIDLabel] != string(cluster.UID) || secret.Annotations[authOperationKey] != operation {
		t.Fatalf("Secret ownership = %#v", secret.ObjectMeta)
	}
	attemptName := pipeline.AuthAttemptResourceName(string(cluster.UID), operation)
	role := &rbacv1.Role{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: attemptName}, role); err != nil {
		t.Fatal(err)
	}
	if !publisherRoleMatches(role, name, cluster.Name, false) || len(role.Rules[0].ResourceNames) != 1 || role.Rules[0].ResourceNames[0] != name {
		t.Fatalf("publisher Role is not Secret-name-scoped: %#v", role.Rules)
	}
	if err := reconciler.revokeAuthPublication(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, secret); !apierrors.IsNotFound(err) {
		t.Fatalf("Secret survived publication revocation: %v", err)
	}
	binding := &rbacv1.RoleBinding{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: attemptName}, binding); !apierrors.IsNotFound(err) {
		t.Fatalf("RoleBinding survived publication revocation: %v", err)
	}
	if err := reconciler.removeAuthPublisher(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: attemptName}, role); !apierrors.IsNotFound(err) {
		t.Fatalf("Role survived publisher cleanup: %v", err)
	}
	serviceAccount := &corev1.ServiceAccount{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: attemptName}, serviceAccount); !apierrors.IsNotFound(err) {
		t.Fatalf("ServiceAccount survived publisher cleanup: %v", err)
	}
}
