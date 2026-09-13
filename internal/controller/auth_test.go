package controller

import (
	"context"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

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
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: "new-allocation"}, Status: servitorv1alpha1.ServitorClusterStatus{LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{PublicAuthEligible: true}}}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &Reconciler{Client: client}
	operation := applyID(string(cluster.UID))
	if err := reconciler.ensureAuthPublicationResources(context.Background(), cluster, operation); err != nil {
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
	role := &rbacv1.Role{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, role); err != nil {
		t.Fatal(err)
	}
	if !publisherRoleMatches(role, name) || len(role.Rules[0].ResourceNames) != 1 || role.Rules[0].ResourceNames[0] != name {
		t.Fatalf("publisher Role is not Secret-name-scoped: %#v", role.Rules)
	}
	if err := reconciler.revokeAuthPublication(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, secret); !apierrors.IsNotFound(err) {
		t.Fatalf("Secret survived publication revocation: %v", err)
	}
	binding := &rbacv1.RoleBinding{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, binding); !apierrors.IsNotFound(err) {
		t.Fatalf("RoleBinding survived publication revocation: %v", err)
	}
	if err := reconciler.removeAuthPublisher(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, role); !apierrors.IsNotFound(err) {
		t.Fatalf("Role survived publisher cleanup: %v", err)
	}
	serviceAccount := &corev1.ServiceAccount{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, serviceAccount); !apierrors.IsNotFound(err) {
		t.Fatalf("ServiceAccount survived publisher cleanup: %v", err)
	}
}
