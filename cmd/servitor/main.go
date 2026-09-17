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
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
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
	restConfig, err := ctrlconfig.GetConfig()
	if err != nil {
		fail(err)
	}
	scheme, err := controller.NewScheme()
	if err != nil {
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
	controllerSettings, err := controllerConfig(operator)
	if err != nil {
		fail(err)
	}
	logs := controller.NewPodLogReader(kubernetes.NewForConfigOrDie(restConfig))
	transport := slackbot.NewSocketMode(secrets.BotToken, secrets.AppToken)
	reconciler := controller.NewReconciler(manager, controllerSettings, logs, transport)
	if err := reconciler.SetupWithManager(manager); err != nil {
		fail(fmt.Errorf("configure controller: %w", err))
	}
	inventorySettings := inventoryControllerConfig(operator)
	inventoryReconciler := &controller.InventoryReconciler{Client: manager.GetClient(), Logs: logs, Config: inventorySettings}
	if err := inventoryReconciler.SetupWithManager(manager); err != nil {
		fail(fmt.Errorf("configure inventory refresh: %w", err))
	}
	bot := newSlackBot(operator, manager.GetClient(), manager.GetAPIReader(), transport, transport)
	if err := manager.Add(slackbot.NewLeaderRunnable(transport, bot)); err != nil {
		fail(fmt.Errorf("configure Slack intake: %w", err))
	}
	if err := manager.Add(&slackbot.StatusNotifier{Client: manager.GetClient(), Namespace: operator.Namespace, Responder: transport, Receipts: state.NewEventStore(manager.GetClient(), operator.Namespace)}); err != nil {
		fail(fmt.Errorf("configure Slack notifications: %w", err))
	}
	if err := manager.Add(&slackbot.InventoryRefreshNotifier{Client: manager.GetClient(), Namespace: operator.Namespace, Responder: transport, Receipts: state.NewEventStore(manager.GetClient(), operator.Namespace)}); err != nil {
		fail(fmt.Errorf("configure inventory refresh notifications: %w", err))
	}
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil && !errors.Is(err, context.Canceled) {
		fail(err)
	}
}

func controllerConfig(operator config.Config) (controller.Config, error) {
	if command.IsCloudDefault(operator.Defaults.Version) {
		return controller.Config{}, fmt.Errorf("resolve controller startup defaults: configured default must be numeric")
	}
	if _, err := command.InferPlatform(operator.Defaults.Version); err != nil {
		return controller.Config{}, fmt.Errorf("resolve controller startup defaults: %w", err)
	}
	networkBindings := make(map[string]servitorv1alpha1.FrozenNetwork, len(operator.Network.TargetBindings))
	for target, bindingID := range operator.Network.TargetBindings {
		binding := operator.Network.Bindings[bindingID]
		networkBindings[target] = servitorv1alpha1.FrozenNetwork{BindingID: bindingID, AccountID: binding.AccountID, VPCID: binding.VPCID, SubnetID: binding.SubnetID, PublicGatewayID: binding.PublicGatewayID, Zone: binding.Zone}
	}
	return controller.Config{
		Namespace: operator.Namespace,
		Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{
			Version: operator.Defaults.Version, Target: operator.Defaults.Target, Provider: operator.Defaults.Provider,
			ResourceGroup: operator.Defaults.ResourceGroup,
		}},
		NetworkBindings: networkBindings,
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
		ReviewTimeout:   operator.Lifecycle.ConfirmationTimeout,
		OpenShiftFlavor: operator.Defaults.OpenShiftFlavor, KubernetesFlavor: operator.Defaults.KubernetesFlavor,
		PublicAuthTargets: append([]string(nil), operator.Auth.PublicTargets...),
	}, nil
}

func inventoryControllerConfig(operator config.Config) controller.InventoryConfig {
	return controller.InventoryConfig{
		Namespace: operator.Namespace, TargetConfigMap: operator.ICT.TargetConfigMap, TargetConfigKey: operator.ICT.TargetConfigKey,
		ExecutionImage: operator.Images.Execution, TaskConfig: pipeline.TaskConfig{ICTConfigMap: operator.ICT.TargetConfigMap, ICTConfigKey: operator.ICT.TargetConfigKey, IBMSecret: operator.Secrets.IBM},
		RefreshInterval: operator.InventoryRefreshInterval(), MaximumAge: operator.InventoryMaximumAge(),
	}
}

type allocationClient struct {
	client.Client
	reader client.Reader
}

func (c allocationClient) AllocationReader() client.Reader { return c.reader }

func commandDefaults(operator config.Config) command.CreateDefaults {
	return command.CreateDefaults{Version: operator.Defaults.Version, Target: operator.Defaults.Target, Provider: operator.Defaults.Provider, ResourceGroup: operator.Defaults.ResourceGroup, OpenShiftFlavor: operator.Defaults.OpenShiftFlavor, KubernetesFlavor: operator.Defaults.KubernetesFlavor}
}

func newSlackBot(operator config.Config, kube client.Client, reader client.Reader, responder slackbot.Responder, permalinks slackbot.PermalinkLookup) slackbot.Bot {
	return slackbot.Bot{
		ChannelID: operator.Slack.ChannelID, Namespace: operator.Namespace, Client: allocationClient{Client: kube, reader: reader},
		MaxAllocationsPerUser: operator.MaxAllocationsPerUser(), Events: state.NewEventStore(kube, operator.Namespace), Defaults: commandDefaults(operator), PublicAuthTargets: append([]string(nil), operator.Auth.PublicTargets...),
		InventoryConfigMap: operator.ICT.TargetConfigMap, InventoryConfigKey: operator.ICT.TargetConfigKey, InventoryMaximumAge: operator.InventoryMaximumAge(), MaintainerIDs: operator.Slack.MaintainerIDs,
		Lease: operator.Lifecycle.Lease, RetryIntervals: operator.Lifecycle.RetryIntervals, Responder: responder, Permalinks: permalinks,
	}
}

func fail(err error) { _, _ = os.Stderr.WriteString("servitor: " + err.Error() + "\n"); os.Exit(1) }
