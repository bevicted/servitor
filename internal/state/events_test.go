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
func TestEventStoreRejectsEmptyID(t *testing.T) {
	if _, err := NewEventStore(fake.NewClientBuilder().Build(), "servitor").Claim(context.Background(), ""); err == nil {
		t.Fatal("Claim(empty) error = nil")
	}
}
