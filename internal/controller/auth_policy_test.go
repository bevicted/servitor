package controller

import (
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
)

func TestSnapshotFreezesPublicAuthEligibilityAndExcludesSatellite(t *testing.T) {
	reconciler := Reconciler{Config: Config{
		Defaults:          servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "public-target", Provider: "vpc-gen2", Version: "4.22"}},
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
	satellite := &servitorv1alpha1.ServitorCluster{Spec: servitorv1alpha1.ServitorClusterSpec{UserOptions: servitorv1alpha1.UserOptions{Provider: "satellite"}}}
	satelliteReconciler := Reconciler{Config: Config{Defaults: reconciler.Config.Defaults, PublicAuthTargets: []string{"public-target"}, OpenShiftFlavor: "bx2.4x16"}}
	if err := satelliteReconciler.snapshot(satellite); err != nil {
		t.Fatal(err)
	}
	if satellite.Status.LifecycleSnapshot.PublicAuthEligible {
		t.Fatal("Satellite allocation was eligible for Phase 1 public auth")
	}
}
