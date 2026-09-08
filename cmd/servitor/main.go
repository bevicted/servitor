// Servitor runs the namespaced controller and leader-elected Slack Socket Mode intake.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/config"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
)

func main() {
	configPath := flag.String("config", "", "mounted operator configuration YAML path")
	flag.Usage = func() {
		output := flag.CommandLine.Output()
		_, _ = output.Write([]byte("Servitor reconciles namespaced ServitorCluster resources through Tekton PipelineRuns and COS-backed ICT.\nThe controller alone writes CR status; Slack writes authorized spec.userOptions and spec.lifecycle intent.\nTerraform plans are ephemeral and approval starts a fresh auto-approved apply from frozen resolved options.\n"))
		flag.PrintDefaults()
	}
	flag.Parse()
	path, err := config.ResolvePath(*configPath)
	if err != nil {
		fail(err)
	}
	operator, err := config.Load(path)
	if err != nil {
		fail(err)
	}
	// Validate non-secret deployment inputs before obtaining credentials or accepting events.
	secrets, err := config.SecretsFromEnv()
	if err != nil {
		fail(err)
	}
	restConfig, err := config2()
	if err != nil {
		fail(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		fail(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		fail(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		fail(err)
	}
	manager, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:           scheme,
		LeaderElection:   true,
		LeaderElectionID: "servitor.bevicted.github.io",
		Cache:            cache.Options{DefaultNamespaces: map[string]cache.Config{operator.Namespace: {}}},
	})
	if err != nil {
		fail(fmt.Errorf("create manager: %w", err))
	}
	reconciler := &controller.Reconciler{
		Client: manager.GetClient(), Scheme: manager.GetScheme(), Logs: controller.NewPodLogReader(kubernetes.NewForConfigOrDie(restConfig)),
		Config: controllerConfig(operator),
	}
	if err := reconciler.SetupWithManager(manager); err != nil {
		fail(fmt.Errorf("configure controller: %w", err))
	}
	transport := slackbot.NewSocketMode(secrets.BotToken, secrets.AppToken)
	bot := slackbot.Bot{
		ChannelID: operator.Slack.ChannelID, Namespace: operator.Namespace, Client: manager.GetClient(),
		Events: state.NewEventStore(manager.GetClient(), operator.Namespace), Defaults: commandDefaults(operator),
		ConfirmationTimeout: operator.Lifecycle.ConfirmationTimeout, Lease: operator.Lifecycle.Lease,
		RetryIntervals: operator.Lifecycle.RetryIntervals, Responder: transport,
	}
	if err := manager.Add(slackbot.NewLeaderRunnable(transport, bot)); err != nil {
		fail(fmt.Errorf("configure Slack intake: %w", err))
	}
	if err := manager.Add(&slackbot.StatusNotifier{Client: manager.GetClient(), Namespace: operator.Namespace, Responder: transport, Receipts: state.NewEventStore(manager.GetClient(), operator.Namespace)}); err != nil {
		fail(fmt.Errorf("configure Slack notifications: %w", err))
	}
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil && !errors.Is(err, context.Canceled) {
		fail(err)
	}
}

func controllerConfig(operator config.Config) controller.Config {
	return controller.Config{
		Namespace: operator.Namespace,
		Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{
			Version: operator.Defaults.Version, Target: operator.Defaults.Target, Provider: operator.Defaults.Provider,
			ResourceGroup: operator.Defaults.ResourceGroup, Zone: operator.Defaults.Zone, VPCID: operator.Defaults.VPCID,
		}},
		Backend: servitorv1alpha1.BackendIdentity{
			Version: 1, Bucket: operator.COS.Bucket, Region: operator.COS.Region, Endpoint: operator.COS.Endpoint,
			SkipCredentialsValidation: operator.COS.SkipCredentialsValidation, SkipMetadataAPICheck: operator.COS.SkipMetadataAPICheck,
			SkipRegionValidation: operator.COS.SkipRegionValidation, SkipRequestingAccountID: operator.COS.SkipRequestingAccountID,
			ForcePathStyle: operator.COS.ForcePathStyle, UseLockfile: operator.COS.UseLockfile,
		},
		BackendPrefix: operator.COS.KeyPrefix, ExecutionImage: operator.Images.Execution,
		TaskConfig: pipeline.TaskConfig{
			ICTConfigMap: operator.ICT.TargetConfigMap, ICTConfigKey: operator.ICT.TargetConfigKey,
			COSSecret: operator.Secrets.COS, IBMSecret: operator.Secrets.IBM,
		},
		ReviewTimeout: operator.Lifecycle.ConfirmationTimeout,
	}
}

func config2() (*rest.Config, error) { return ctrlconfig.GetConfig() }
func commandDefaults(operator config.Config) command.CreateDefaults {
	return command.CreateDefaults{Version: operator.Defaults.Version, Target: operator.Defaults.Target, Provider: operator.Defaults.Provider, ResourceGroup: operator.Defaults.ResourceGroup, Zone: operator.Defaults.Zone, VPCID: operator.Defaults.VPCID, OpenShiftFlavor: operator.Defaults.OpenShiftFlavor, KubernetesFlavor: operator.Defaults.KubernetesFlavor}
}

func fail(err error) { _, _ = os.Stderr.WriteString("servitor: " + err.Error() + "\n"); os.Exit(1) }
