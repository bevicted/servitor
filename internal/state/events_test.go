package state

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEventClaimsPersistAcrossRestartAndRemainBounded(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	store := NewEventStore(kube, "servitor")
	store.Now = func() time.Time { return time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC) }
	claimed, err := store.Claim(context.Background(), "Ev1")
	if err != nil || !claimed {
		t.Fatalf("first Claim() = (%v, %v), want (true, nil)", claimed, err)
	}
	store = NewEventStore(kube, "servitor")
	claimed, err = store.Claim(context.Background(), "Ev1")
	if err != nil || claimed {
		t.Fatalf("restarted Claim() = (%v, %v), want (false, nil)", claimed, err)
	}
	for i := 0; i < maxReceipts+10; i++ {
		if _, err := store.Claim(context.Background(), fmt.Sprintf("Ev-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	cm := &corev1.ConfigMap{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: defaultReceiptConfigMap}, cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.Data) > maxReceipts {
		t.Fatalf("receipts = %d, want <= %d", len(cm.Data), maxReceipts)
	}
}
func TestEventStoreTracksClaimedDeliveryAcrossRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	store := NewEventStore(kube, "servitor")
	if claimed, err := store.Claim(context.Background(), "cleanup-cause"); err != nil || !claimed {
		t.Fatalf("Claim() = (%v, %v), want (true, nil)", claimed, err)
	}
	if delivered, err := store.Delivered(context.Background(), "cleanup-cause"); err != nil || delivered {
		t.Fatalf("Delivered() before mark = (%v, %v), want (false, nil)", delivered, err)
	}
	if err := store.MarkDelivered(context.Background(), "cleanup-cause"); err != nil {
		t.Fatal(err)
	}
	store = NewEventStore(kube, "servitor")
	if delivered, err := store.Delivered(context.Background(), "cleanup-cause"); err != nil || !delivered {
		t.Fatalf("Delivered() after restart = (%v, %v), want (true, nil)", delivered, err)
	}
}

func TestEventStoreReleaseAllowsFailedDeliveryRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	store := NewEventStore(fake.NewClientBuilder().WithScheme(scheme).Build(), "servitor")
	claimed, err := store.Claim(context.Background(), "cleanup-cause")
	if err != nil || !claimed {
		t.Fatalf("Claim() = (%v, %v), want (true, nil)", claimed, err)
	}
	if err := store.Release(context.Background(), "cleanup-cause"); err != nil {
		t.Fatal(err)
	}
	if seen, err := store.Seen(context.Background(), "cleanup-cause"); err != nil || seen {
		t.Fatalf("Seen() after Release = (%v, %v), want (false, nil)", seen, err)
	}
	claimed, err = store.Claim(context.Background(), "cleanup-cause")
	if err != nil || !claimed {
		t.Fatalf("Claim() after Release = (%v, %v), want (true, nil)", claimed, err)
	}
}

func TestEventStoreRejectsEmptyID(t *testing.T) {
	store := NewEventStore(fake.NewClientBuilder().Build(), "servitor")
	if _, err := store.Claim(context.Background(), ""); err == nil {
		t.Fatal("Claim(empty) error = nil")
	}
	if _, err := store.Delivered(context.Background(), ""); err == nil {
		t.Fatal("Delivered(empty) error = nil")
	}
	if err := store.MarkDelivered(context.Background(), ""); err == nil {
		t.Fatal("MarkDelivered(empty) error = nil")
	}
	if err := store.Release(context.Background(), ""); err == nil {
		t.Fatal("Release(empty) error = nil")
	}
}
