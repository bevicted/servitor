package controller

import (
	"context"
	"errors"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	authUIDLabel          = "servitor.bevicted.github.io/auth-uid"
	authOperationKey      = "servitor.bevicted.github.io/auth-operation"
	authSecretDataName    = "kubeconfig.yaml"
	authSecretVPNDataName = "client.ovpn"
)

func authResourceName(cluster *servitorv1alpha1.ServitorCluster) string {
	return pipeline.AuthResourceName(string(cluster.UID))
}

func (r *Reconciler) ensureAuthPublicationResources(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, operation string) error {
	if !authEligible(cluster) {
		return nil
	}
	name := authResourceName(cluster)
	labels := map[string]string{authUIDLabel: string(cluster.UID)}
	annotations := map[string]string{authOperationKey: operation}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	if err := r.directReader().Get(ctx, key, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels, Annotations: annotations}, Type: corev1.SecretTypeOpaque}
		if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		if apierrors.IsAlreadyExists(err) {
			return r.ensureAuthPublicationResources(ctx, cluster, operation)
		}
	} else if err := validateAuthSecret(secret, cluster, operation); err != nil {
		return err
	}

	if err := r.ensurePublisherServiceAccount(ctx, cluster, name, labels); err != nil {
		return err
	}
	if err := r.ensurePublisherRole(ctx, cluster, name, labels); err != nil {
		return err
	}
	return r.ensurePublisherRoleBinding(ctx, cluster, name, labels)
}

func validateAuthSecret(secret *corev1.Secret, cluster *servitorv1alpha1.ServitorCluster, operation string) error {
	if secret.Labels[authUIDLabel] != string(cluster.UID) || secret.Annotations[authOperationKey] != operation {
		return errors.New("public auth Secret ownership does not match the allocation")
	}
	return nil
}

func (r *Reconciler) ensurePublisherServiceAccount(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, name string, labels map[string]string) error {
	current := &corev1.ServiceAccount{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	if err := r.directReader().Get(ctx, key, current); err == nil {
		if current.Labels[authUIDLabel] != string(cluster.UID) {
			return errors.New("public auth ServiceAccount ownership does not match the allocation")
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return r.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels}, AutomountServiceAccountToken: boolPointer(false)})
}

func (r *Reconciler) ensurePublisherRole(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, name string, labels map[string]string) error {
	current := &rbacv1.Role{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	if err := r.directReader().Get(ctx, key, current); err == nil {
		if current.Labels[authUIDLabel] != string(cluster.UID) || !publisherRoleMatches(current, name) {
			return errors.New("public auth Role does not match the allocation scope")
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return r.Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{name}, Verbs: []string{"get", "update", "patch"}}}})
}

func publisherRoleMatches(role *rbacv1.Role, name string) bool {
	return len(role.Rules) == 1 && len(role.Rules[0].APIGroups) == 1 && role.Rules[0].APIGroups[0] == "" && len(role.Rules[0].Resources) == 1 && role.Rules[0].Resources[0] == "secrets" && len(role.Rules[0].ResourceNames) == 1 && role.Rules[0].ResourceNames[0] == name && sameStrings(role.Rules[0].Verbs, []string{"get", "update", "patch"})
}

func (r *Reconciler) ensurePublisherRoleBinding(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, name string, labels map[string]string) error {
	current := &rbacv1.RoleBinding{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	if err := r.directReader().Get(ctx, key, current); err == nil {
		if current.Labels[authUIDLabel] != string(cluster.UID) || len(current.Subjects) != 1 || current.Subjects[0].Kind != "ServiceAccount" || current.Subjects[0].Name != name || current.Subjects[0].Namespace != cluster.Namespace || current.RoleRef.APIGroup != rbacv1.GroupName || current.RoleRef.Kind != "Role" || current.RoleRef.Name != name {
			return errors.New("public auth RoleBinding does not match the allocation scope")
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return r.Create(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: cluster.Namespace}}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}})
}

// revokeAuthPublication removes authority before deleting data, so a late task
// cannot recreate a Secret after cleanup begins.
func (r *Reconciler) revokeAuthPublication(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) error {
	if !authEligible(cluster) {
		return nil
	}
	name := authResourceName(cluster)
	if err := r.Delete(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: name}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	if err := r.directReader().Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := validateAuthSecret(secret, cluster, applyID(string(cluster.UID))); err != nil {
		return err
	}
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *Reconciler) removeAuthPublisher(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) error {
	if !authEligible(cluster) {
		return nil
	}
	name := authResourceName(cluster)
	if err := r.Delete(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: name}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.Delete(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: name}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func authEligible(cluster *servitorv1alpha1.ServitorCluster) bool {
	if cluster.Status.LifecycleSnapshot == nil {
		return false
	}
	return cluster.Status.LifecycleSnapshot.AuthEligible || cluster.Status.LifecycleSnapshot.PublicAuthEligible
}

func sameStrings(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	seen := make(map[string]bool, len(actual))
	for _, value := range actual {
		seen[value] = true
	}
	for _, value := range expected {
		if !seen[value] {
			return false
		}
	}
	return true
}

func boolPointer(value bool) *bool { return &value }

func (r *Reconciler) directReader() client.Reader {
	if r.DirectReader != nil {
		return r.DirectReader
	}
	return r.Client
}
