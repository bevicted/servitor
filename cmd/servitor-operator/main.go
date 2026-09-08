// servitor-operator starts the namespaced ServitorCluster planning controller.
package main

import (
	"flag"
	"os"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/controller"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func main() {
	var namespace, image, backendPrefix string
	var defaults servitorv1alpha1.UserOptions
	backend := servitorv1alpha1.BackendIdentity{Version: 1}
	flag.StringVar(&namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace to watch")
	flag.StringVar(&image, "execution-image", os.Getenv("SERVITOR_EXECUTION_IMAGE"), "pinned ICT execution image")
	flag.StringVar(&backendPrefix, "backend-prefix", os.Getenv("SERVITOR_BACKEND_PREFIX"), "COS backend key prefix")
	flag.StringVar(&defaults.Target, "default-target", os.Getenv("SERVITOR_DEFAULT_TARGET"), "default ICT target")
	flag.StringVar(&defaults.Provider, "default-provider", os.Getenv("SERVITOR_DEFAULT_PROVIDER"), "default ICT provider")
	flag.StringVar(&defaults.Platform, "default-platform", os.Getenv("SERVITOR_DEFAULT_PLATFORM"), "default Kubernetes platform")
	flag.StringVar(&defaults.Version, "default-version", os.Getenv("SERVITOR_DEFAULT_VERSION"), "default Kubernetes version")
	flag.StringVar(&defaults.ResourceGroup, "default-resource-group", os.Getenv("SERVITOR_DEFAULT_RESOURCE_GROUP"), "default IBM Cloud resource group")
	flag.StringVar(&defaults.Zone, "default-zone", os.Getenv("SERVITOR_DEFAULT_ZONE"), "default VPC zone")
	flag.StringVar(&defaults.Flavor, "default-flavor", os.Getenv("SERVITOR_DEFAULT_FLAVOR"), "default worker flavor")
	flag.StringVar(&defaults.VPCID, "default-vpc-id", os.Getenv("SERVITOR_DEFAULT_VPC_ID"), "default VPC ID")
	flag.StringVar(&backend.Bucket, "backend-bucket", os.Getenv("SERVITOR_BACKEND_BUCKET"), "COS backend bucket")
	flag.StringVar(&backend.Region, "backend-region", os.Getenv("SERVITOR_BACKEND_REGION"), "COS backend region")
	flag.StringVar(&backend.Endpoint, "backend-endpoint", os.Getenv("SERVITOR_BACKEND_ENDPOINT"), "COS backend HTTPS endpoint")
	flag.BoolVar(&backend.SkipCredentialsValidation, "backend-skip-credentials-validation", true, "skip Terraform backend credential validation")
	flag.BoolVar(&backend.SkipMetadataAPICheck, "backend-skip-metadata-api-check", true, "skip Terraform backend metadata API check")
	flag.BoolVar(&backend.SkipRegionValidation, "backend-skip-region-validation", true, "skip Terraform backend region validation")
	flag.BoolVar(&backend.SkipRequestingAccountID, "backend-skip-requesting-account-id", true, "skip Terraform backend account ID lookup")
	flag.BoolVar(&backend.ForcePathStyle, "backend-force-path-style", true, "use path-style COS backend requests")
	flag.Parse()
	if namespace == "" || image == "" || backendPrefix == "" || backend.Bucket == "" || backend.Region == "" || backend.Endpoint == "" {
		panic("namespace, execution image, backend prefix, bucket, region, and endpoint are required")
	}
	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(servitorv1alpha1.AddToScheme(scheme))
	must(tektonv1.AddToScheme(scheme))
	config := ctrl.GetConfigOrDie()
	manager, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme, Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}}})
	must(err)
	kube, err := kubernetes.NewForConfig(config)
	must(err)
	reconciler := &controller.Reconciler{Client: manager.GetClient(), Scheme: scheme, Logs: controller.NewPodLogReader(kube), Config: controller.Config{Namespace: namespace, Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: defaults}, Backend: backend, BackendPrefix: backendPrefix, ExecutionImage: image}}
	must(reconciler.SetupWithManager(manager))
	must(manager.Start(ctrl.SetupSignalHandler()))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
