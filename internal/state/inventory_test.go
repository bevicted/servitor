package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/inventory"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestInventorySnapshotReportsMissingExpiredUsableAndUnusable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	store := NewInventoryStore(fake.NewClientBuilder().WithScheme(scheme).Build(), "servitor")
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	if _, disposition, err := store.Snapshot(context.Background(), "target-a", "revision-a", now, time.Hour); err != nil || disposition != InventoryMissing {
		t.Fatalf("missing snapshot = %s, %v", disposition, err)
	}
	catalog := inventory.Catalog{Version: inventory.CatalogVersion, Target: "target-a", Providers: []string{"vpc-gen2"}, Versions: []inventory.Version{{Name: "4.22_openshift", Platform: "openshift", Default: true, Supported: true}}, ResourceGroups: []string{"Default"}, VPCLocations: []inventory.Location{{Name: "us-south-1", Flavors: []string{"bx2.4x16"}}}}
	if _, err := store.Update(context.Background(), "target-a", func(current *InventoryState) error {
		current.Revision = "revision-a"
		current.Catalog = &catalog
		published := metav1.NewTime(now.Add(-2 * time.Hour))
		current.PublishedAt = &published
		current.Disposition = InventorySucceeded
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, disposition, err := store.Snapshot(context.Background(), "target-a", "revision-a", now, time.Hour); err != nil || disposition != InventoryExpired {
		t.Fatalf("expired snapshot = %s, %v", disposition, err)
	}
	if got, disposition, err := store.Snapshot(context.Background(), "target-a", "revision-a", now, 3*time.Hour); err != nil || disposition != InventorySucceeded || got.ResourceGroups[0] != "Default" {
		t.Fatalf("usable snapshot = %+v, %s, %v", got, disposition, err)
	}

	catalog.Versions = nil
	published := metav1.NewTime(now)
	data, err := json.Marshal(InventoryState{Version: 1, Target: "target-b", Revision: "revision-b", Catalog: &catalog, PublishedAt: &published, Disposition: InventorySucceeded})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Client.Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: store.Name("target-b"), Namespace: "servitor", Labels: map[string]string{inventoryStateLabel: "true"}}, Data: map[string]string{inventoryStateKey: string(data)}}); err != nil {
		t.Fatal(err)
	}
	if _, disposition, err := store.Snapshot(context.Background(), "target-b", "revision-b", now, time.Hour); err != nil || disposition != InventoryUnusable {
		t.Fatalf("unusable persisted snapshot = %s, %v", disposition, err)
	}
}
