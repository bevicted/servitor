// Package v1alpha1 contains the ServitorCluster API.
// +kubebuilder:object:generate=true
// +groupName=servitor.bevicted.github.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "servitor.bevicted.github.io", Version: "v1alpha1"}

func Kind(kind string) schema.GroupKind { return GroupVersion.WithKind(kind).GroupKind() }
func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}

var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
var AddToScheme = SchemeBuilder.AddToScheme
