package controller

import (
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSnapshotFreezesVPNPolicyAcrossConfigurationChanges(t *testing.T) {
	network := frozenNetwork()
	network.AuthPolicy = &servitorv1alpha1.FrozenAuthPolicy{VPNServerID: "vpn-original", SecretsManagerID: "sm", SecretsManagerRegion: "eu-gb", SecretGroupID: "group", CertificateTemplate: "template", Issuer: "issuer", TTL: "168h"}
	reconciler := Reconciler{Config: Config{Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "private-target", Provider: "vpc-gen2", Version: "4.22"}}, NetworkBindings: map[string]servitorv1alpha1.FrozenNetwork{"private-target": network}, OpenShiftFlavor: "bx2.4x16"}}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{UID: types.UID("allocation-uid")}}
	if err := reconciler.snapshot(cluster); err != nil {
		t.Fatal(err)
	}
	frozen := cluster.Status.ResolvedOptions.Network.AuthPolicy
	if frozen == nil || frozen.AllocationUID != "allocation-uid" || frozen.VPNServerID != "vpn-original" || cluster.Status.LifecycleSnapshot == nil || !cluster.Status.LifecycleSnapshot.AuthEligible {
		t.Fatalf("snapshot did not freeze private auth policy: %#v", cluster.Status)
	}
	changed := reconciler.Config.NetworkBindings["private-target"]
	changed.AuthPolicy.VPNServerID = "vpn-changed"
	reconciler.Config.NetworkBindings["private-target"] = changed
	if cluster.Status.ResolvedOptions.Network.AuthPolicy.VPNServerID != "vpn-original" {
		t.Fatal("later configuration redirected frozen auth policy")
	}
}

func TestSnapshotFreezesPublicAuthEligibility(t *testing.T) {
	reconciler := Reconciler{Config: Config{
		Defaults:          servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "public-target", Provider: "vpc-gen2", Version: "4.22"}},
		NetworkBindings:   map[string]servitorv1alpha1.FrozenNetwork{"public-target": frozenNetwork()},
		PublicAuthTargets: []string{"public-target"}, OpenShiftFlavor: "bx2.4x16",
	}}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := reconciler.snapshot(cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.LifecycleSnapshot == nil || !cluster.Status.LifecycleSnapshot.PublicAuthEligible {
		t.Fatalf("public eligibility was not frozen: %#v", cluster.Status.LifecycleSnapshot)
	}
	reconciler.Config.PublicAuthTargets = nil
	if !cluster.Status.LifecycleSnapshot.PublicAuthEligible {
		t.Fatal("later configuration changed frozen public auth eligibility")
	}
}
