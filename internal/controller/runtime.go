package controller

import (
	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

// NewScheme returns the exact runtime scheme used by the controller manager.
func NewScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for _, addToScheme := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		servitorv1alpha1.AddToScheme,
		tektonv1.AddToScheme,
		rbacv1.AddToScheme,
	} {
		if err := addToScheme(scheme); err != nil {
			return nil, err
		}
	}
	return scheme, nil
}

// NewReconciler wires a reconciler to the manager's cached client and direct API reader.
func NewReconciler(manager ctrl.Manager, config Config, logs pipeline.LogReader, delivery AuthFileDelivery) *Reconciler {
	return &Reconciler{
		Client:       manager.GetClient(),
		DirectReader: manager.GetAPIReader(),
		Scheme:       manager.GetScheme(),
		Config:       config,
		Logs:         logs,
		AuthDelivery: delivery,
	}
}
