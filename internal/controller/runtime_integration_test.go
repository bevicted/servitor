//go:build integration

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
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
	verifyAutoApproveTransitionContract(t, ctx, admin)
	autoKey, autoResponses := seedAutoApproveContract(t, ctx, admin)
	manualKey := seedManualApproveFalseContract(t, ctx, admin, autoResponses)
	timeoutKey := seedApprovalContractWithResponder(t, ctx, admin, "timeout", "1710000000.000101", "approve", true, autoResponses, 3*time.Second)

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

	notifier := &slackbot.StatusNotifier{Client: admin, Namespace: runtimeContractNamespace, Responder: autoResponses, Receipts: state.NewEventStore(admin, runtimeContractNamespace), Interval: 10 * time.Millisecond}
	notifierContext, stopNotifier := context.WithCancel(ctx)
	notifierDone := make(chan error, 1)
	go func() { notifierDone <- notifier.Start(notifierContext) }()
	notifierStopped := false
	stopStatusNotifier := func() {
		if notifierStopped {
			return
		}
		notifierStopped = true
		stopNotifier()
		if err := <-notifierDone; err != nil {
			t.Errorf("stop notifier: %v", err)
		}
	}
	t.Cleanup(stopStatusNotifier)

	eventually(t, managerDone, "automatic delivery-gated apply", func() error {
		cluster := &servitorv1alpha1.ServitorCluster{}
		if err := admin.Get(ctx, autoKey, cluster); err != nil {
			return err
		}
		if autoResponses.count() != 3 || cluster.Spec.Lifecycle.Approval != "approved" || cluster.Status.Phase != servitorv1alpha1.PhaseApplying {
			return fmt.Errorf("responses=%d approval=%q phase=%q", autoResponses.count(), cluster.Spec.Lifecycle.Approval, cluster.Status.Phase)
		}
		var runs tektonv1.PipelineRunList
		if err := admin.List(ctx, &runs, client.InNamespace(runtimeContractNamespace), client.MatchingLabels{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: applyID(string(cluster.UID))}); err != nil {
			return err
		}
		if len(runs.Items) != 1 {
			return fmt.Errorf("apply PipelineRuns=%d", len(runs.Items))
		}
		return nil
	})

	eventually(t, managerDone, "approve=false remains manual", func() error {
		cluster := &servitorv1alpha1.ServitorCluster{}
		if err := admin.Get(ctx, manualKey, cluster); err != nil {
			return err
		}
		if autoResponses.manualCount() != 3 || cluster.Spec.Lifecycle.AutoApprove || cluster.Spec.Lifecycle.Approval != "" || cluster.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval {
			return fmt.Errorf("manual replies=%d autoApprove=%t approval=%q phase=%q", autoResponses.manualCount(), cluster.Spec.Lifecycle.AutoApprove, cluster.Spec.Lifecycle.Approval, cluster.Status.Phase)
		}
		var runs tektonv1.PipelineRunList
		if err := admin.List(ctx, &runs, client.InNamespace(runtimeContractNamespace), client.MatchingLabels{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: applyID(string(cluster.UID))}); err != nil {
			return err
		}
		if len(runs.Items) != 0 {
			return fmt.Errorf("apply PipelineRuns=%d", len(runs.Items))
		}
		return nil
	})

	stopStatusNotifier()

	if failures := autoResponses.failureCount(); failures == 0 {
		t.Fatal("failing responder did not reject a review reply")
	}
	eventually(t, managerDone, "failed review delivery deadline cleanup", func() error {
		cluster := &servitorv1alpha1.ServitorCluster{}
		if err := admin.Get(ctx, timeoutKey, cluster); err != nil {
			return err
		}
		if cluster.Spec.Lifecycle.Approval != "" || cluster.Status.Phase != servitorv1alpha1.PhaseCleanupComplete || cluster.Status.Cleanup == nil || cluster.Status.Cleanup.Reason != servitorv1alpha1.CleanupReasonReviewExpired {
			return fmt.Errorf("approval=%q phase=%q cleanup=%+v", cluster.Spec.Lifecycle.Approval, cluster.Status.Phase, cluster.Status.Cleanup)
		}
		var runs tektonv1.PipelineRunList
		if err := admin.List(ctx, &runs, client.InNamespace(runtimeContractNamespace), client.MatchingLabels{pipeline.ClusterUIDLabel: string(cluster.UID), pipeline.OperationLabel: applyID(string(cluster.UID))}); err != nil {
			return err
		}
		if len(runs.Items) != 0 {
			return fmt.Errorf("apply PipelineRuns=%d", len(runs.Items))
		}
		return nil
	})

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

func verifyAutoApproveTransitionContract(t *testing.T, ctx context.Context, kube client.Client) {
	t.Helper()
	updateMustFail := func(cluster *servitorv1alpha1.ServitorCluster) {
		t.Helper()
		if err := kube.Update(ctx, cluster); err == nil {
			t.Fatalf("autoApprove transition unexpectedly succeeded: %+v", cluster.Spec.Lifecycle)
		}
	}
	auto := contractCluster("auto-approve-true")
	auto.Spec.Lifecycle.AutoApprove = true
	if err := kube.Create(ctx, auto); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, client.ObjectKeyFromObject(auto), auto); err != nil || !auto.Spec.Lifecycle.AutoApprove {
		t.Fatalf("persist autoApprove = %+v, %v", auto.Spec.Lifecycle, err)
	}
	auto.Spec.Lifecycle.AutoApprove = false
	updateMustFail(auto)
	if err := kube.Get(ctx, client.ObjectKeyFromObject(auto), auto); err != nil {
		t.Fatal(err)
	}
	auto.Spec.Lifecycle.AutoApprove = false
	updateMustFail(auto)

	manual := contractCluster("auto-approve-omitted")
	if err := kube.Create(ctx, manual); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, client.ObjectKeyFromObject(manual), manual); err != nil || manual.Spec.Lifecycle.AutoApprove {
		t.Fatalf("omitted autoApprove = %+v, %v", manual.Spec.Lifecycle, err)
	}
	manual.Spec.Lifecycle.AutoApprove = true
	updateMustFail(manual)
	for _, cluster := range []*servitorv1alpha1.ServitorCluster{auto, manual} {
		if err := kube.Delete(ctx, cluster); err != nil {
			t.Fatal(err)
		}
	}
}

func seedAutoApproveContract(t *testing.T, ctx context.Context, kube client.Client) (types.NamespacedName, *runtimeContractResponder) {
	t.Helper()
	responses := &runtimeContractResponder{thread: "1710000000.000100", manualThread: "1710000000.000102", failingThread: "1710000000.000101"}
	return seedApprovalContractWithResponder(t, ctx, kube, "auto", "1710000000.000100", "approve", true, responses, time.Minute), responses
}

func seedManualApproveFalseContract(t *testing.T, ctx context.Context, kube client.Client, responder slackbot.Responder) types.NamespacedName {
	t.Helper()
	return seedApprovalContractWithResponder(t, ctx, kube, "manual", "1710000000.000102", "approve=false", false, responder, time.Minute)
}

func seedApprovalContractWithResponder(t *testing.T, ctx context.Context, kube client.Client, name, thread, approvalOption string, wantAutoApprove bool, responder slackbot.Responder, reviewTimeout time.Duration) types.NamespacedName {
	t.Helper()
	bot := slackbot.Bot{
		ChannelID: "CCHANNEL", SelfUserID: "BOT", Namespace: runtimeContractNamespace, Client: kube,
		Events: state.NewEventStore(kube, runtimeContractNamespace), Responder: responder,
		Defaults: command.CreateDefaults{Version: "4.22"}, Lease: time.Hour, RetryIntervals: []time.Duration{time.Minute},
	}
	message := slackbot.Message{Channel: "CCHANNEL", ChannelType: "channel", User: "UAUTO", Text: fmt.Sprintf("<@BOT> create %s version=4.22", approvalOption), Timestamp: thread}
	if err := bot.Handle(ctx, slackbot.Envelope{ID: "auto-create-" + name, Message: message}); err != nil {
		t.Fatal(err)
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := kube.List(ctx, &clusters, client.InNamespace(runtimeContractNamespace)); err != nil {
		t.Fatal(err)
	}
	for index := range clusters.Items {
		cluster := &clusters.Items[index]
		if cluster.Spec.Slack.ThreadTimestamp != message.Timestamp {
			continue
		}
		if cluster.Spec.Lifecycle.AutoApprove != wantAutoApprove || cluster.Spec.Lifecycle.Approval != "" {
			t.Fatalf("create approval state = lifecycle=%+v", cluster.Spec.Lifecycle)
		}
		key := client.ObjectKeyFromObject(cluster)
		now := time.Now().UTC()
		reconciler := &Reconciler{Client: kube, Config: Config{
			Namespace:       runtimeContractNamespace,
			Defaults:        *contractResolvedOptions("auto-cluster"),
			NetworkBindings: runtimeContractNetworkBindings(),
			Backend:         servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Region: "us-south", Endpoint: "https://s3.example.invalid"},
			BackendPrefix:   "runtime-contract",
			ExecutionImage:  "registry.example.invalid/task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ReviewTimeout:   reviewTimeout,
		}, Now: func() time.Time { return now }}
		request := ctrl.Request{NamespacedName: key}
		for range 4 {
			if _, err := reconciler.Reconcile(ctx, request); err != nil {
				t.Fatal(err)
			}
		}
		if err := kube.Get(ctx, key, cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Status.Operation == nil {
			t.Fatalf("synthetic plan operation missing: status=%+v", cluster.Status)
		}
		run := &tektonv1.PipelineRun{}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: runtimeContractNamespace, Name: cluster.Status.Operation.PipelineRunName}, run); err != nil {
			t.Fatal(err)
		}
		run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
		run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: k8sruntime.TypeMeta{APIVersion: "tekton.dev/v1", Kind: "TaskRun"}, Name: "auto-plan-report-" + name, PipelineTaskName: "operation"}}
		if err := kube.Status().Update(ctx, run); err != nil {
			t.Fatal(err)
		}
		task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "auto-plan-report-" + name, Namespace: runtimeContractNamespace}}
		if err := kube.Create(ctx, task); err != nil {
			t.Fatal(err)
		}
		task.Status.PodName = "auto-plan-pod"
		task.Status.Steps = []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}
		if err := kube.Status().Update(ctx, task); err != nil {
			t.Fatal(err)
		}
		report, err := json.Marshal(pipeline.Report{Version: 1, ClusterUID: string(cluster.UID), OperationID: cluster.Status.Operation.ID, ResolvedOptions: *cluster.Status.ResolvedOptions, Recovery: *contractRecovery("auto-cluster-" + name), Review: servitorv1alpha1.ReviewSummary{Resources: []servitorv1alpha1.SummaryResource{{Role: "Cluster", Actions: []string{"create"}}}}})
		if err != nil {
			t.Fatal(err)
		}
		reconciler.Logs = reportLogs{data: report}
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(ctx, key, cluster); err != nil || cluster.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval || cluster.Status.Review == nil || cluster.Status.ReviewDeadline == nil {
			t.Fatalf("synthetic plan report adoption = status=%+v err=%v", cluster.Status, err)
		}
		return key
	}
	t.Fatal("bot did not create allocation")
	return types.NamespacedName{}
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
		Values:       servitorv1alpha1.RecoveryValues{ClusterName: clusterName, ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 1, Zone: "us-south-1", Flavor: "bx2.4x16", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}},
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
