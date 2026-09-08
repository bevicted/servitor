package admission

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPausePersistsAndRepeatedTransitionsAreNoOps(t *testing.T) {
	directory := t.TempDir()
	control, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !control.Accepting() {
		t.Fatal("new control is not accepting")
	}
	status, changed, err := control.Pause()
	if err != nil || !changed || status.Mode != Paused || control.Accepting() {
		t.Fatalf("pause = (%+v, %v, %v)", status, changed, err)
	}
	if _, changed, err := control.Pause(); err != nil || changed {
		t.Fatalf("repeated pause changed=%v err=%v", changed, err)
	}
	restored, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status().Mode != Paused {
		t.Fatalf("restored mode = %q, want paused", restored.Status().Mode)
	}
	if _, changed, err := restored.Unpause(); err != nil || !changed || !restored.Accepting() {
		t.Fatalf("unpause changed=%v err=%v accepting=%v", changed, err, restored.Accepting())
	}
	if _, err := os.Stat(filepath.Join(directory, filename)); !os.IsNotExist(err) {
		t.Fatalf("pause marker stat error = %v, want absent", err)
	}
	if _, changed, err := restored.Unpause(); err != nil || changed {
		t.Fatalf("repeated unpause changed=%v err=%v", changed, err)
	}
}

func TestPausedStateRestoresWithoutLifecycleRecords(t *testing.T) {
	directory := t.TempDir()
	control, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := control.Pause(); err != nil || !changed {
		t.Fatalf("pause changed=%v err=%v", changed, err)
	}

	// Lifecycle records reside in ICT workspaces, not the Servitor state path.
	// Their absence must not change the durable admission mode on restart.
	workspace := filepath.Join(t.TempDir(), "U1")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".servitor-lifecycle.json"), []byte(`{"status":"ready"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(workspace); err != nil {
		t.Fatal(err)
	}

	restored, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if status := restored.Status(); status.Mode != Paused || restored.Accepting() {
		t.Fatalf("restored status = %+v, accepting=%v", status, restored.Accepting())
	}
}

func TestAdmitAndPauseHaveOneLinearizationPoint(t *testing.T) {
	control, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	admittedResult := make(chan struct {
		admitted   bool
		generation uint64
		err        error
	}, 1)
	go func() {
		admitted, generation, err := control.Admit(func() error {
			close(entered)
			<-release
			return nil
		})
		admittedResult <- struct {
			admitted   bool
			generation uint64
			err        error
		}{admitted, generation, err}
	}()
	<-entered

	pauseResult := make(chan struct {
		status  Status
		changed bool
		err     error
	}, 1)
	go func() {
		status, changed, err := control.Pause()
		pauseResult <- struct {
			status  Status
			changed bool
			err     error
		}{status, changed, err}
	}()
	select {
	case result := <-pauseResult:
		t.Fatalf("pause completed during admitted operation: %+v", result)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	result := <-admittedResult
	if !result.admitted || result.generation != 0 || result.err != nil {
		t.Fatalf("admit = %+v", result)
	}
	paused := <-pauseResult
	if !paused.changed || paused.err != nil || paused.status.Mode != Paused {
		t.Fatalf("pause = %+v", paused)
	}

	called := false
	admitted, generation, err := control.Admit(func() error {
		called = true
		return nil
	})
	if admitted || called || generation != 1 || err != nil {
		t.Fatalf("paused admit = (admitted=%v generation=%d called=%v err=%v)", admitted, generation, called, err)
	}
}

func TestStopDrainsActivityAndRestoresPausedAfterInterruptedDrain(t *testing.T) {
	control, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	applyDone := control.Activity("apply")
	status, changed, drained, err := control.Stop()
	if err != nil || !changed || status.Mode != DrainingToStop || control.Accepting() {
		t.Fatalf("stop = (%+v, %v, %v), accepting=%v", status, changed, err, control.Accepting())
	}
	if _, changed, repeated, err := control.Stop(); err != nil || changed || repeated != drained {
		t.Fatalf("repeated stop = (changed=%v drained=%v err=%v)", changed, repeated, err)
	}
	for name, transition := range map[string]func() (Status, bool, error){
		"pause":   control.Pause,
		"unpause": control.Unpause,
	} {
		status, changed, err := transition()
		if err == nil || err.Error() != "admission is draining to stop" || changed || status.Mode != DrainingToStop || control.Accepting() {
			t.Fatalf("%s during drain = (%+v, changed=%v, err=%v), accepting=%v", name, status, changed, err, control.Accepting())
		}
	}
	select {
	case <-drained:
		t.Fatal("drain completed with active apply")
	default:
	}
	restarted, err := Open(filepath.Dir(control.path))
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Status().Mode != Paused {
		t.Fatalf("restart during drain mode = %q, want paused", restarted.Status().Mode)
	}
	applyDone()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain did not complete after apply")
	}
	if err := control.CompleteStop(); err != nil {
		t.Fatal(err)
	}
	restarted, err = Open(filepath.Dir(control.path))
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Status().Mode != Accepting {
		t.Fatalf("restart after completed stop mode = %q, want accepting", restarted.Status().Mode)
	}
}

func TestStopPreservesPreexistingPause(t *testing.T) {
	directory := t.TempDir()
	control, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := control.Pause(); err != nil || !changed {
		t.Fatalf("pause changed=%v err=%v", changed, err)
	}
	_, changed, drained, err := control.Stop()
	if err != nil || !changed {
		t.Fatalf("stop changed=%v err=%v", changed, err)
	}
	<-drained
	if err := control.CompleteStop(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Status().Mode != Paused {
		t.Fatalf("restart after stop from pause = %q, want paused", restarted.Status().Mode)
	}
}

func TestActivityIsBalancedAndDoesNotCountReadyLeases(t *testing.T) {
	control, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reviewDone := control.Activity("review")
	applyDone := control.Activity("apply")
	cleanupDone := control.Activity("cleanup")
	if got := control.Status(); got.Reviews != 1 || got.Applies != 1 || got.Cleanup != 1 {
		t.Fatalf("active status = %+v", got)
	}
	reviewDone()
	applyDone()
	cleanupDone()
	cleanupDone()
	if got := control.Status(); got.Reviews != 0 || got.Applies != 0 || got.Cleanup != 0 {
		t.Fatalf("balanced status = %+v", got)
	}
	control.Activity("ready")()
	if got := control.Status(); got.Reviews != 0 || got.Applies != 0 || got.Cleanup != 0 {
		t.Fatalf("ready lease changed status = %+v", got)
	}
}
