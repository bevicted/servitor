package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type authDeliveryRecorder struct {
	calls   int
	owner   string
	cluster string
	bytes   []byte
	err     error
	before  func()
}

func (d *authDeliveryRecorder) DeliverKubeconfig(_ context.Context, owner, cluster string, kubeconfig []byte) error {
	if d.before != nil {
		d.before()
	}
	d.calls++
	d.owner, d.cluster = owner, cluster
	d.bytes = append([]byte(nil), kubeconfig...)
	if len(kubeconfig) > 0 {
		kubeconfig[0] = 'x'
	}
	return d.err
}

func readyAuthCluster(request string, expiry time.Time) *servitorv1alpha1.ServitorCluster {
	return &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "ns", UID: "allocation-uid"},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack:     servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{AuthRequestTimestamp: request},
		},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:             servitorv1alpha1.PhaseReady,
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{PublicAuthEligible: true},
			PublicAuth:        &servitorv1alpha1.PublicAuthStatus{Availability: "available"},
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{ClusterName: "example-cluster"},
			LeaseExpiresAt:    &metav1.Time{Time: expiry},
		},
	}
}

func newAuthDeliveryClient(t *testing.T, cluster *servitorv1alpha1.ServitorCluster, contents []byte) (client.Client, *corev1.Secret) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:        authResourceName(cluster),
		Namespace:   cluster.Namespace,
		Labels:      map[string]string{authUIDLabel: string(cluster.UID)},
		Annotations: map[string]string{authOperationKey: applyID(string(cluster.UID))},
	}, Data: map[string][]byte{authSecretDataName: append([]byte(nil), contents...)}}
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithRuntimeObjects(cluster, secret).Build(), secret
}

type authDeliveryStatusClient struct {
	client.Client
	failNextStatusUpdate bool
	afterStatusUpdate    func()
}

func (c *authDeliveryStatusClient) Status() client.SubResourceWriter {
	return authDeliveryStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type authDeliveryStatusWriter struct {
	client.SubResourceWriter
	client *authDeliveryStatusClient
}

func (w authDeliveryStatusWriter) Update(ctx context.Context, object client.Object, options ...client.SubResourceUpdateOption) error {
	if w.client.failNextStatusUpdate {
		w.client.failNextStatusUpdate = false
		return errors.New("controlled status write failure")
	}
	if err := w.SubResourceWriter.Update(ctx, object, options...); err != nil {
		return err
	}
	if w.client.afterStatusUpdate != nil {
		after := w.client.afterStatusUpdate
		w.client.afterStatusUpdate = nil
		after()
	}
	return nil
}

type authSecretReadRecorder struct {
	client.Reader
	reads int
}

func (r *authSecretReadRecorder) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1.Secret); ok {
		r.reads++
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func TestReadyAuthDeliveryConsumesBeforeReadingAndDoesNotReplay(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := readyAuthCluster("1710000000.000100", now.Add(time.Hour))
	kube, secret := newAuthDeliveryClient(t, cluster, []byte("synthetic-kubeconfig"))
	recorder := &authDeliveryRecorder{}
	reconciler := &Reconciler{Client: kube, DirectReader: kube, AuthDelivery: recorder, Now: func() time.Time { return now }}
	recorder.before = func() {
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.AttemptTimestamp != "1710000000.000100" || stored.Status.AuthDelivery.Outcome != authDeliveryPending {
			t.Fatalf("delivery started before consumed status persisted: %#v", stored.Status.AuthDelivery)
		}
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 || recorder.owner != "U1" || recorder.cluster != "example-cluster" || string(recorder.bytes) != "synthetic-kubeconfig" {
		t.Fatalf("delivery = calls:%d owner:%q cluster:%q", recorder.calls, recorder.owner, recorder.cluster)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != authDeliveryDelivered {
		t.Fatalf("delivery status = %#v", stored.Status.AuthDelivery)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data[authSecretDataName]) != "synthetic-kubeconfig" {
		t.Fatal("delivery modified stored kubeconfig")
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 {
		t.Fatalf("reconciliation replayed consumed delivery: %d calls", recorder.calls)
	}
}

func TestFailedAuthDeliveryRemainsConsumedUntilNewerRequest(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := readyAuthCluster("1710000000.000100", now.Add(time.Hour))
	kube, _ := newAuthDeliveryClient(t, cluster, []byte("synthetic-kubeconfig"))
	recorder := &authDeliveryRecorder{err: errors.New("controlled upload failure")}
	reconciler := &Reconciler{Client: kube, DirectReader: kube, AuthDelivery: recorder, Now: func() time.Time { return now }}
	stored := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: "ns", Name: "cluster"}
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 {
		t.Fatalf("failed delivery calls = %d", recorder.calls)
	}
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != authDeliveryFailed || stored.Status.Phase != servitorv1alpha1.PhaseReady {
		t.Fatalf("failed delivery changed lifecycle: %#v", stored.Status)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 {
		t.Fatalf("failed delivery replayed: %d calls", recorder.calls)
	}
	stored.Spec.Lifecycle.AuthRequestTimestamp = "1710000001.000100"
	if err := kube.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	recorder.err = nil
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 2 || string(recorder.bytes) != "synthetic-kubeconfig" {
		t.Fatalf("newer request did not resend stored file: calls=%d", recorder.calls)
	}
}

func TestPendingAuthDeliveryBecomesFailedWhenOutcomeWriteFails(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := readyAuthCluster("1710000000.000100", now.Add(time.Hour))
	base, _ := newAuthDeliveryClient(t, cluster, []byte("synthetic-kubeconfig"))
	kube := &authDeliveryStatusClient{Client: base}
	recorder := &authDeliveryRecorder{before: func() { kube.failNextStatusUpdate = true }}
	reconciler := &Reconciler{Client: kube, DirectReader: kube, AuthDelivery: recorder, Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: "ns", Name: "cluster"}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err == nil {
		t.Fatal("delivery outcome status write unexpectedly succeeded")
	}
	if err := base.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 || stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != authDeliveryPending {
		t.Fatalf("failed outcome write did not retain only the consumed pending attempt: calls=%d status=%#v", recorder.calls, stored.Status.AuthDelivery)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 || stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != authDeliveryFailed || stored.Status.Phase != servitorv1alpha1.PhaseReady {
		t.Fatalf("pending delivery was not finalized safely: calls=%d status=%#v", recorder.calls, stored.Status)
	}
}

func TestCleanupAfterPendingAuthDeliveryPreventsSecretReadAndUpload(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := readyAuthCluster("1710000000.000100", now.Add(time.Hour))
	base, _ := newAuthDeliveryClient(t, cluster, []byte("synthetic-kubeconfig"))
	kube := &authDeliveryStatusClient{Client: base}
	key := types.NamespacedName{Namespace: "ns", Name: "cluster"}
	kube.afterStatusUpdate = func() {
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := base.Get(context.Background(), key, stored); err != nil {
			t.Fatal(err)
		}
		stored.Status.CleanupRequested = true
		stored.Status.Cleanup = &servitorv1alpha1.CleanupStatus{}
		if err := base.Status().Update(context.Background(), stored); err != nil {
			t.Fatal(err)
		}
	}
	secretReader := &authSecretReadRecorder{Reader: kube}
	recorder := &authDeliveryRecorder{}
	reconciler := &Reconciler{Client: kube, DirectReader: secretReader, AuthDelivery: recorder, Now: func() time.Time { return now }}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if secretReader.reads != 0 || recorder.calls != 0 || stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != authDeliveryCancelled {
		t.Fatalf("cleanup allowed auth delivery: reads=%d calls=%d status=%#v", secretReader.reads, recorder.calls, stored.Status.AuthDelivery)
	}
}

func TestCleanupCancelsPendingAuthDelivery(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := readyAuthCluster("1710000000.000100", now.Add(time.Hour))
	cluster.Status.AuthDelivery = &servitorv1alpha1.AuthDeliveryStatus{
		RequestTimestamp: cluster.Spec.Lifecycle.AuthRequestTimestamp,
		AttemptTimestamp: cluster.Spec.Lifecycle.AuthRequestTimestamp,
		Outcome:          authDeliveryPending,
	}
	kube, _ := newAuthDeliveryClient(t, cluster, []byte("synthetic-kubeconfig"))
	reconciler := &Reconciler{Client: kube, Now: func() time.Time { return now }}
	stored := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: "ns", Name: "cluster"}
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.requestCleanup(context.Background(), stored, servitorv1alpha1.CleanupReasonExplicit); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Cleanup == nil || stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != authDeliveryCancelled {
		t.Fatalf("cleanup did not cancel pending auth delivery: %#v", stored.Status)
	}
}

func TestExpiredAuthRequestCancelsWithoutDelivery(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := readyAuthCluster("1710000000.000100", now)
	kube, _ := newAuthDeliveryClient(t, cluster, []byte("synthetic-kubeconfig"))
	recorder := &authDeliveryRecorder{}
	reconciler := &Reconciler{Client: kube, DirectReader: kube, AuthDelivery: recorder, Now: func() time.Time { return now }}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "cluster"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReady(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 0 || stored.Status.AuthDelivery == nil || stored.Status.AuthDelivery.Outcome != "Cancelled" || stored.Status.Cleanup == nil {
		t.Fatalf("expired auth delivery = calls:%d status:%#v", recorder.calls, stored.Status)
	}
}
