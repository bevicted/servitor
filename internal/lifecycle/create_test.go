package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/admission"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/diagnostics"
	ictclient "github.com/bevicted/servitor/internal/ict"
	"github.com/bevicted/servitor/internal/state"
)

type admissionActivityRecorder struct {
	control *admission.Control
	mu      sync.Mutex
	done    map[string][]func()
	events  []string
}

func newAdmissionActivityRecorder(t *testing.T) *admissionActivityRecorder {
	t.Helper()
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &admissionActivityRecorder{control: control, done: make(map[string][]func())}
}

func (r *admissionActivityRecorder) callback(kind string, active bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if active {
		r.done[kind] = append(r.done[kind], r.control.Activity(kind))
		r.events = append(r.events, kind+":start")
		return
	}
	callbacks := r.done[kind]
	if len(callbacks) == 0 {
		r.events = append(r.events, kind+":unexpected-stop")
		return
	}
	done := callbacks[len(callbacks)-1]
	r.done[kind] = callbacks[:len(callbacks)-1]
	r.events = append(r.events, kind+":stop")
	done()
}

func (r *admissionActivityRecorder) assertIdle(t *testing.T, want ...string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status := r.control.Status()
		if status.Reviews == 0 && status.Applies == 0 && status.Cleanup == 0 {
			r.mu.Lock()
			got := append([]string(nil), r.events...)
			r.mu.Unlock()
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("activity events = %v, want %v", got, want)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("activity did not drain: %+v", r.control.Status())
}

func TestCreatorReviewsThenAppliesOneProcess(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	t.Setenv("XDG_STATE_HOME", stateHome)
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) if [ -f "$XDG_STATE_HOME/ict-active" ]; then cat "$XDG_STATE_HOME/ict-active"; fi ;;
create) mkdir -p "$XDG_STATE_HOME/ict/$2/.cluster"; echo "$2" > "$XDG_STATE_HOME/ict-active"; while [ ! -f "$XDG_STATE_HOME/review-ready" ]; do sleep 0.01; done; echo 'Do you want to perform these actions?'; read answer; [ "$answer" = yes ] || { rm -f "$XDG_STATE_HOME/ict-active"; exit 0; } ;;
destroy) rm -rf "$XDG_STATE_HOME/ict/$2"; rm -f "$XDG_STATE_HOME/ict-active" ;;
*) exit 2 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
case "$*" in
*create.tfplan*) printf '%s' '{"format_version":"1.2","padding":"'; head -c 131072 /dev/zero | tr '\000' x; printf '%s' '","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"],"after":{"token":"secret"}}}]}' ;;
*) touch "$XDG_STATE_HOME/state-show-started"; while [ ! -f "$XDG_STATE_HOME/release-state-show" ]; do sleep 0.01; done; printf '%s' '{"format_version":"1.2","values":{"root_module":{"resources":[{"address":"data.ibm_is_vpc.shared","mode":"data","type":"ibm_is_vpc","name":"shared","values":{"id":"vpc-id"}},{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","values":{"id":"cluster-id"}}]}}}' ;;
esac
`)
	storePath := filepath.Join(directory, "servitor-state")
	if err := os.Mkdir(storePath, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	manager := NewManager(client, store, []time.Duration{time.Minute}, "M1")
	appliedAt := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: appliedAt}
	manager.Clock = clock
	activity := newAdmissionActivityRecorder(t)
	creator := &Creator{ICT: &client, Store: store, Destroyer: manager, ConfigPath: filepath.Join(directory, "ict.yaml"), TerraformPath: terraformPath, ConfirmationTimeout: time.Minute, Lease: 4 * time.Hour, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}, Clock: clock, OnActivity: activity.callback}
	sent := &notices{}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err != nil {
		t.Fatal(err)
	}
	if err := creator.Confirm(context.Background(), request, "yes", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, store, "U1", statusReview)
	if err := os.WriteFile(filepath.Join(stateHome, "review-ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForText(t, sent, "Planned resources:")
	if got := strings.Join(sent.texts(), "\n"); !strings.Contains(got, "Cluster:") || strings.Contains(got, "secret") {
		t.Fatalf("review = %q", got)
	}
	if err := creator.Confirm(context.Background(), request, "yes", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, filepath.Join(stateHome, "state-show-started"))
	clock.mu.Lock()
	clock.now = clock.now.Add(30 * time.Minute)
	clock.mu.Unlock()
	if err := os.WriteFile(filepath.Join(stateHome, "release-state-show"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, store, "U1", statusReady)
	waitForText(t, sent, "Your cluster is ready.")
	record, found := store.Get("U1")
	if !found || !record.UpdatedAt.Equal(appliedAt) || !record.LeaseExpiresAt.Equal(appliedAt.Add(4*time.Hour)) {
		t.Fatalf("lease record = %+v, found=%v", record, found)
	}
	got := strings.Join(sent.texts(), "\n")
	if !strings.Contains(got, "Reused") || !strings.Contains(got, "VPC") || strings.Contains(got, "private") {
		t.Fatalf("ready notices = %q", got)
	}
	activity.assertIdle(t, "review:start", "review:stop", "apply:start", "apply:stop")
}
func TestCreatorDetectsPromptPastBoundedOperationLog(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) ;;
create) mkdir -p "$XDG_STATE_HOME/ict/$2"; yes x | head -c 65537; printf 'Do you want to perform these actions?'; read answer ;;
destroy) rm -rf "$XDG_STATE_HOME/ict/$2" ;;
*) exit 2 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
case "$*" in
*create.tfplan*) printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"]}}]}' ;;
*) printf '%s' '{"format_version":"1.2","values":{"root_module":{"resources":[]}}}' ;;
esac
`)
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, diagnosticLogs := newDiagnosticClient(t, ictPath)
	manager := NewManager(client, store, []time.Duration{time.Minute}, "M1")
	creator := &Creator{ICT: &client, Store: store, Destroyer: manager, ConfigPath: filepath.Join(directory, "ict.yaml"), TerraformPath: terraformPath, ConfirmationTimeout: time.Minute, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}}
	sent := &notices{}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForText(t, sent, "Planned resources:")
	record, found := store.Get("U1")
	if !found {
		t.Fatal("missing lifecycle record")
	}
	if !strings.Contains(diagnosticContents(t, diagnosticLogs, record.DiagnosticRef, "U1"), "Do you want to perform these actions?") {
		t.Fatal("private operation log did not preserve output past bounded Slack capture")
	}
	waitForStatus(t, store, "U1", statusReview)
	if err := creator.Confirm(context.Background(), request, "no", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, store, "U1", statusResolved)
}

func TestCreatorDeclinedReviewAcceptsUnreportedSensitivePlanMetadataAndICTWorkspaceRemoval(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
create) mkdir -p "$XDG_STATE_HOME/ict/$2"; printf 'Do you want to perform these actions?'; read answer; [ "$answer" = no ] && rm -rf "$XDG_STATE_HOME/ict/$2" ;;
destroy) touch "$XDG_STATE_HOME/destroy-ran" ;;
*) exit 2 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"],"after":{"name":"cluster","token":"secret"},"after_sensitive":{"token":true}}}]}'
`)
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	creator := &Creator{
		ICT:                 &client,
		Store:               store,
		Destroyer:           NewManager(client, store, []time.Duration{time.Minute}, "M1"),
		ConfigPath:          filepath.Join(directory, "ict.yaml"),
		TerraformPath:       terraformPath,
		ConfirmationTimeout: time.Minute,
		Defaults:            command.CreateDefaults{Version: "1.36", Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", KubernetesFlavor: "bx2.2x8"},
	}
	sent := &notices{}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	notify := func(ctx context.Context, notice Notice) error {
		if strings.Contains(notice.Text, "Planned resources:") {
			return errors.New("inject review delivery failure")
		}
		return sent.send(ctx, notice)
	}
	if err := creator.Start(context.Background(), request, "create", notify); err != nil {
		t.Fatal(err)
	}
	waitForText(t, sent, "Cleanup complete.")
	if _, err := os.Stat(filepath.Join(stateHome, "destroy-ran")); !os.IsNotExist(err) {
		t.Fatalf("destroy ran after ICT removed the declined workspace: %v", err)
	}
	if notices := strings.Join(sent.texts(), "\n"); strings.Contains(notices, "could not persist its lifecycle") {
		t.Fatalf("notices reported a persistence failure after ICT cleanup: %q", notices)
	}
	if _, found := store.Get(request.UserID); found {
		t.Fatal("lifecycle record survived ICT workspace removal")
	}
	if _, found := store.WorkspacePath(request.UserID); found {
		t.Fatal("removed ICT workspace remained reserved in memory")
	}
}

func TestCreatorRejectsOverLimitTerraformPlanAndCleansUp(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) if [ -f "$XDG_STATE_HOME/ict-active" ]; then printf 'U1'; fi ;;
create) mkdir -p "$XDG_STATE_HOME/ict/U1/.cluster"; touch "$XDG_STATE_HOME/ict-active"; printf 'Do you want to perform these actions?'; read answer ;;
destroy) touch "$XDG_STATE_HOME/cleanup-started"; rm -rf "$XDG_STATE_HOME/ict/$2"; rm -f "$XDG_STATE_HOME/ict-active" ;;
*) exit 2 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
printf '%s' '{"format_version":"1.2","padding":"'
head -c 1048576 /dev/zero | tr '\000' x
printf '%s' 'private-tail","resource_changes":[]}'
`)
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, diagnosticLogs := newDiagnosticClient(t, ictPath)
	creator := &Creator{
		ICT:                 &client,
		Store:               store,
		Destroyer:           NewManager(client, store, []time.Duration{time.Minute}, "M1"),
		ConfigPath:          filepath.Join(directory, "ict.yaml"),
		TerraformPath:       terraformPath,
		ConfirmationTimeout: time.Minute,
		Defaults:            command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"},
	}
	sent := &notices{}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, filepath.Join(stateHome, "cleanup-started"))
	waitForStatus(t, store, "U1", statusResolved)
	if _, found := store.Get("U1"); found {
		t.Fatal("lifecycle record survived successful workspace removal")
	}

	notices := strings.Join(sent.texts(), "\n")
	for _, forbidden := range []string{"private-tail", "[output truncated]", `"format_version"`} {
		if strings.Contains(notices, forbidden) {
			t.Fatalf("Slack notices exposed Terraform output %q: %s", forbidden, notices)
		}
	}
	if !strings.Contains(notices, "Unable to review the saved Terraform plan.") {
		t.Fatalf("Slack notices = %q", notices)
	}
	_, diagnosticReference, found := strings.Cut(notices, "Diagnostic ID: `")
	if !found {
		t.Fatalf("Slack notices omitted code-formatted diagnostic ID: %s", notices)
	}
	diagnosticID, _, found := strings.Cut(diagnosticReference, "`")
	if !found || !diagnostics.ValidID(diagnosticID) {
		t.Fatalf("diagnostic ID = %q", diagnosticID)
	}
	diagnostic := diagnosticContents(t, diagnosticLogs, diagnosticID, request.UserID)
	for _, wanted := range []string{"terraform JSON stdout exceeds 1048576 byte limit", "private-tail"} {
		if !strings.Contains(diagnostic, wanted) {
			t.Fatalf("private diagnostic missing %q", wanted)
		}
	}
}

func TestCreatorCreateAcceptanceFailurePreventsICTPreflight(t *testing.T) {
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := ictclient.NewClient("/must-not-run", nil)
	creator := &Creator{
		ICT:           &client,
		Store:         store,
		Destroyer:     cleanupRecorder{requests: make(chan Request, 1)},
		ConfigPath:    "ict.yaml",
		TerraformPath: "terraform",
		Defaults:      command.CreateDefaults{Version: "1.36", Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"},
	}
	err = creator.Start(context.Background(), Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}, "create", func(context.Context, Notice) error {
		return errors.New("Slack unavailable")
	})
	if err == nil || !strings.Contains(err.Error(), "deliver create acceptance") {
		t.Fatalf("Start error = %v, want failed acceptance", err)
	}
}

func TestCreatorPreflightFailureHasSafeCorrelatedNotice(t *testing.T) {
	directory := t.TempDir()
	ictPath := createExecutable(t, directory, "ict", "#!/bin/sh\nprintf 'xoxb-secret /private/state {terraform_values}\\n' >&2\nexit 1\n")
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, diagnosticLogs := newDiagnosticClient(t, ictPath)
	client.InventorySource = nil
	creator := &Creator{
		ICT:           &client,
		Store:         store,
		Destroyer:     NewManager(client, store, []time.Duration{time.Minute}, "M1"),
		ConfigPath:    "ict.yaml",
		TerraformPath: "terraform",
		Defaults:      command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"},
	}

	err = creator.Start(context.Background(), Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}, "create --version 4.22", nil)
	if err == nil {
		t.Fatal("Start succeeded after ICT preflight failure")
	}
	var noticeError UserNoticeError
	if !errors.As(err, &noticeError) {
		t.Fatalf("Start error %T does not provide a Slack-safe notice", err)
	}
	text := noticeError.UserNotice()
	for _, forbidden := range []string{"xoxb-secret", "/private/state", "terraform_values"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Slack notice leaked %q: %s", forbidden, text)
		}
	}
	_, diagnosticReference, found := strings.Cut(text, "Diagnostic ID: `")
	if !found {
		t.Fatalf("Slack notice omitted code-formatted diagnostic ID: %s", text)
	}
	id, _, found := strings.Cut(diagnosticReference, "`")
	if !found || !diagnostics.ValidID(id) {
		t.Fatalf("diagnostic ID = %q", id)
	}
	if !strings.Contains(text, "No cleanup is running; this requires maintainer attention.") {
		t.Fatalf("Slack notice did not explain cleanup state: %s", text)
	}
	if contents := diagnosticContents(t, diagnosticLogs, id, "U1"); !strings.Contains(contents, "xoxb-secret /private/state {terraform_values}") {
		t.Fatalf("private diagnostic log omitted preflight output: %s", contents)
	}
}

func TestCreatorFailsClosedWhenTerraformDiagnosticWriterCannotOpen(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "terraform-ran")
	terraformPath := createExecutable(t, directory, "terraform", "#!/bin/sh\ntouch \"$SERVITOR_TERRAFORM_MARKER\"\n")
	t.Setenv("SERVITOR_TERRAFORM_MARKER", marker)
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	refreshStoreWithWorkspace(t, store, "U1")
	const diagnosticID = "111111111111111111111111"
	putRecord(t, store, state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2", Status: statusReview, DiagnosticRef: diagnosticID, UpdatedAt: time.Now()})
	root := filepath.Join(directory, "diagnostics")
	logger, err := diagnostics.Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	creator := &Creator{Store: store, TerraformPath: terraformPath, Diagnostics: logger}

	if _, err := creator.terraformShow("U1"); err == nil {
		t.Fatal("terraform show succeeded without a private diagnostic writer")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("terraform ran after diagnostic writer failure: %v", err)
	}
}

func TestCreatorPrivateWriterFailureHasSafeDiagnosticNotice(t *testing.T) {
	root := filepath.Join(t.TempDir(), "diagnostics")
	logger, err := diagnostics.Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := ictclient.NewClient("/unused/ict", nil)
	client.Diagnostics = logger
	creator := &Creator{ICT: &client, Store: store, Destroyer: NewManager(client, store, nil, "M1"), ConfigPath: "ict.yaml", TerraformPath: "terraform", Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}}

	err = creator.Start(context.Background(), Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}, "create --version 4.22", nil)
	var noticeError UserNoticeError
	if !errors.As(err, &noticeError) {
		t.Fatalf("Start error %T does not provide a Slack-safe notice", err)
	}
	text := noticeError.UserNotice()
	if !strings.Contains(text, "No cleanup is running; this requires maintainer attention.") {
		t.Fatalf("diagnostic writer failure notice = %q", text)
	}
	assertDiagnosticNotice(t, text)
}

func TestCreatorStartFailureHasSafeDiagnosticNotice(t *testing.T) {
	directory := t.TempDir()
	ictPath := createExecutable(t, directory, "ict", "#!/bin/sh\ncase \"$1\" in\nlist) rm -- \"$0\" ;;\nesac\n")
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	creator := &Creator{ICT: &client, Store: store, Destroyer: NewManager(client, store, nil, "M1"), ConfigPath: "ict.yaml", TerraformPath: "terraform", Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}}

	err = creator.Start(context.Background(), Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}, "create --version 4.22", nil)
	var noticeError UserNoticeError
	if !errors.As(err, &noticeError) {
		t.Fatalf("Start error %T does not provide a Slack-safe notice", err)
	}
	text := noticeError.UserNotice()
	if !strings.Contains(text, "Unable to verify reserved ICT state.") || !strings.Contains(text, "No cleanup is running; this requires maintainer attention.") {
		t.Fatalf("create start failure notice = %q", text)
	}
	assertDiagnosticNotice(t, text)
}

type cleanupRecorder struct{ requests chan Request }

func (r cleanupRecorder) RequestDestroy(_ context.Context, request Request, _ Notifier) error {
	r.requests <- request
	return nil
}
func (cleanupRecorder) ScheduleLease(Request, state.LifecycleRecord, Notifier) {}

func TestCreatorPersistenceFailureStartsCleanupWithDiagnosticNotice(t *testing.T) {
	directory := t.TempDir()
	ictPath := createExecutable(t, directory, "ict", "#!/bin/sh\ncase \"$1\" in\nlist) exit 0 ;;\ncreate) mkdir -p \"$SERVITOR_TEST_ICT_ROOT/$2/.servitor-lifecycle.json\"; read answer ;;\nesac\n")
	storeRoot := filepath.Join(directory, "state")
	if err := os.Mkdir(storeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(storeRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storeRoot, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	cleanups := cleanupRecorder{requests: make(chan Request, 1)}
	creator := &Creator{ICT: &client, Store: store, Destroyer: cleanups, ConfigPath: "ict.yaml", TerraformPath: "terraform", Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}

	err = creator.Start(context.Background(), request, "create --version 4.22", nil)
	var noticeError UserNoticeError
	if !errors.As(err, &noticeError) {
		t.Fatalf("Start error %T does not provide a Slack-safe notice", err)
	}
	text := noticeError.UserNotice()
	if !strings.Contains(text, "Cleanup is running.") {
		t.Fatalf("persistence failure notice = %q", text)
	}
	assertDiagnosticNotice(t, text)
	select {
	case got := <-cleanups.requests:
		if got != request {
			t.Fatalf("cleanup request = %+v, want %+v", got, request)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start after lifecycle persistence failure")
	}
}

func TestCreatorApprovalPersistenceFailureDeclinesAndRetainsOwnership(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) ;;
create) mkdir -p "$XDG_STATE_HOME/ict/$2"; printf 'Do you want to perform these actions?'; read answer; printf '%s' "$answer" > "$XDG_STATE_HOME/answer" ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"]}}]}'
`)
	storeRoot := filepath.Join(directory, "state")
	if err := os.Mkdir(storeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	cleanups := cleanupRecorder{requests: make(chan Request, 1)}
	activity := newAdmissionActivityRecorder(t)
	creator := &Creator{ICT: &client, Store: store, Destroyer: cleanups, ConfigPath: "ict.yaml", TerraformPath: terraformPath, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}, OnActivity: activity.callback}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	sent := &notices{}

	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForText(t, sent, "Planned resources:")
	waitForStatus(t, store, "U1", statusReview)
	for deadline := time.Now().Add(time.Second); ; time.Sleep(time.Millisecond) {
		creator.mu.Lock()
		_, reviewed := creator.reviews[request.UserID]
		creator.mu.Unlock()
		if reviewed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("review was not ready for confirmation")
		}
	}
	if err := os.RemoveAll(filepath.Join(stateHome, "ict", request.UserID)); err != nil {
		t.Fatal(err)
	}

	err = creator.Confirm(context.Background(), request, "yes", sent.send)
	var noticeError UserNoticeError
	if !errors.As(err, &noticeError) {
		t.Fatalf("Confirm error %T does not provide a Slack-safe notice", err)
	}
	text := noticeError.UserNotice()
	if !strings.Contains(text, "Unable to record create approval.") || !strings.Contains(text, "Cleanup is running.") {
		t.Fatalf("approval persistence notice = %q", text)
	}
	assertDiagnosticNotice(t, text)
	waitForFile(t, filepath.Join(stateHome, "answer"))
	if answer, err := os.ReadFile(filepath.Join(stateHome, "answer")); err != nil || string(answer) != "no" {
		t.Fatalf("approval response = %q, err=%v, want no", answer, err)
	}
	select {
	case got := <-cleanups.requests:
		if got != request {
			t.Fatalf("cleanup request = %+v, want %+v", got, request)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start after approval persistence failure")
	}
	if record, found := store.Get(request.UserID); !found || record.Status != statusReview {
		t.Fatalf("lifecycle record = %+v, found=%v, want retained review ownership", record, found)
	}
	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err == nil || !strings.Contains(err.Error(), "already have a cluster lifecycle") {
		t.Fatalf("second create error = %v, want retained lifecycle ownership", err)
	}
	activity.assertIdle(t, "review:start", "review:stop")
}

func TestCreatorConfirmationWriteFailureStartsCleanup(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) ;;
create) mkdir -p "$XDG_STATE_HOME/ict/$2"; printf 'Do you want to perform these actions?'; exec 0<&-; while [ ! -f "$XDG_STATE_HOME/release-create" ]; do sleep 0.01; done; exit 1 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"]}}]}'
`)
	storeRoot := filepath.Join(directory, "state")
	if err := os.Mkdir(storeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	cleanups := cleanupRecorder{requests: make(chan Request, 1)}
	activity := newAdmissionActivityRecorder(t)
	creator := &Creator{ICT: &client, Store: store, Destroyer: cleanups, ConfigPath: "ict.yaml", TerraformPath: terraformPath, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}, OnActivity: activity.callback}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	sent := &notices{}

	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForText(t, sent, "Planned resources:")
	for deadline := time.Now().Add(time.Second); ; time.Sleep(time.Millisecond) {
		creator.mu.Lock()
		_, reviewed := creator.reviews[request.UserID]
		creator.mu.Unlock()
		if reviewed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("review was not ready for confirmation")
		}
	}
	if err := creator.Confirm(context.Background(), request, "yes", sent.send); err != nil {
		t.Fatalf("Confirm returned confirmation write error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateHome, "release-create"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-cleanups.requests:
		if got != request {
			t.Fatalf("cleanup request = %+v, want %+v", got, request)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start after confirmation write failure")
	}
	creator.mu.Lock()
	_, active := creator.processes[request.UserID]
	_, reviewed := creator.reviews[request.UserID]
	creator.mu.Unlock()
	if active || reviewed {
		t.Fatalf("failed create remains active=%v reviewed=%v", active, reviewed)
	}
	activity.assertIdle(t, "review:start", "review:stop", "apply:start", "apply:stop")
}

func assertDiagnosticNotice(t *testing.T, text string) {
	t.Helper()
	_, diagnosticReference, found := strings.Cut(text, "Diagnostic ID: `")
	if !found {
		t.Fatalf("Slack notice omitted code-formatted diagnostic ID: %s", text)
	}
	id, _, found := strings.Cut(diagnosticReference, "`")
	if !found || !diagnostics.ValidID(id) {
		t.Fatalf("diagnostic ID = %q", id)
	}
}

func TestCreatorSerializesOneUserWithoutBlockingOtherUsers(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) sleep 0.1 ;;
create) mkdir -p "$XDG_STATE_HOME/ict/$2/.cluster"; echo 'Do you want to perform these actions?'; read answer; [ "$answer" = yes ] || exit 0; touch "$XDG_STATE_HOME/applying-$2"; while [ ! -f "$XDG_STATE_HOME/release" ]; do sleep 0.01; done ;;
destroy) rm -rf "$XDG_STATE_HOME/ict/$2" ;;
*) exit 2 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
case "$*" in
*create.tfplan*) printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"]}}]}' ;;
*) printf '%s' '{"format_version":"1.2","values":{"root_module":{"resources":[]}}}' ;;
esac
`)
	storePath := filepath.Join(directory, "servitor-state")
	if err := os.Mkdir(storePath, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	manager := NewManager(client, store, []time.Duration{time.Minute}, "M1")
	creator := &Creator{ICT: &client, Store: store, Destroyer: manager, ConfigPath: filepath.Join(directory, "ict.yaml"), TerraformPath: terraformPath, ConfirmationTimeout: time.Minute, Lease: 4 * time.Hour, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}}
	sent := &notices{}
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(stateHome, "release"), nil, 0o600)
		creator.Shutdown(sent.send)
	})

	requests := []Request{
		{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.1"},
		{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"},
		{UserID: "U2", Channel: "C1", ThreadTimestamp: "2.1"},
	}
	start := make(chan struct{})
	results := make(chan error, len(requests))
	for _, request := range requests {
		go func(request Request) {
			<-start
			results <- creator.Start(context.Background(), request, "create --version 4.22", sent.send)
		}(request)
	}
	close(start)
	var successes, duplicates int
	for range requests {
		if err := <-results; err == nil {
			successes++
		} else if strings.Contains(err.Error(), "already have a cluster lifecycle") {
			duplicates++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 2 || duplicates != 1 {
		t.Fatalf("start results: successes=%d duplicates=%d", successes, duplicates)
	}
	waitForNoticeCount(t, sent, 2)
	for _, user := range []string{"U1", "U2"} {
		waitForReviewReady(t, creator, user)
		waitForStatus(t, store, user, statusReview)
		record, found := store.Get(user)
		if !found {
			t.Fatalf("missing lifecycle record for %s", user)
		}
		request := Request{UserID: user, Channel: record.Channel, ThreadTimestamp: record.ThreadTimestamp}
		if err := creator.Confirm(context.Background(), request, "yes", sent.send); err != nil {
			t.Fatal(err)
		}
		waitForFile(t, filepath.Join(stateHome, "applying-"+user))
	}
	if err := os.WriteFile(filepath.Join(stateHome, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"U1", "U2"} {
		waitForStatus(t, store, user, statusReady)
	}
}

func TestCreatorCancelAndShutdownDuringApplyUseOneTerminalCleanup(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) if [ -f "$XDG_STATE_HOME/ict-active" ]; then cat "$XDG_STATE_HOME/ict-active"; fi ;;
create) mkdir -p "$XDG_STATE_HOME/ict/$2/.cluster"; echo "$2" > "$XDG_STATE_HOME/ict-active"; echo 'Do you want to perform these actions?'; read answer; [ "$answer" = yes ] || exit 0; trap 'exit 0' INT; touch "$XDG_STATE_HOME/applying"; while :; do sleep 0.01; done ;;
destroy) rm -rf "$XDG_STATE_HOME/ict/$2"; rm -f "$XDG_STATE_HOME/ict-active" ;;
*) exit 2 ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
case "$*" in
*create.tfplan*) printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"]}}]}' ;;
*) touch "$XDG_STATE_HOME/state-show-started"; while [ ! -f "$XDG_STATE_HOME/release-state-show" ]; do sleep 0.01; done; printf '%s' '{"format_version":"1.2","values":{"root_module":{"resources":[]}}}' ;;
esac
`)
	storePath := filepath.Join(directory, "servitor-state")
	if err := os.Mkdir(storePath, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenLifecycleStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	manager := NewManager(client, store, []time.Duration{time.Minute}, "M1")
	activity := newAdmissionActivityRecorder(t)
	creator := &Creator{ICT: &client, Store: store, Destroyer: manager, ConfigPath: filepath.Join(directory, "ict.yaml"), TerraformPath: terraformPath, ConfirmationTimeout: time.Minute, Lease: 4 * time.Hour, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}, OnActivity: activity.callback}
	sent := &notices{}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	if err := creator.Start(context.Background(), request, "create --version 4.22", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForText(t, sent, "Planned resources:")
	if err := creator.Confirm(context.Background(), request, "yes", sent.send); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, filepath.Join(stateHome, "applying"))
	unrelated := request
	unrelated.ThreadTimestamp = "unrelated"
	unrelated.RawThread = true
	if creator.Cancel(context.Background(), unrelated, sent.send) {
		t.Fatal("Cancel accepted an unrelated raw thread")
	}
	if !creator.Cancel(context.Background(), request, sent.send) {
		t.Fatal("Cancel returned false while create was applying")
	}
	creator.Shutdown(sent.send)
	waitForFile(t, filepath.Join(stateHome, "state-show-started"))
	waitForStatus(t, store, "U1", statusResolved)
	if err := os.WriteFile(filepath.Join(stateHome, "release-state-show"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, found := store.Get("U1"); found {
		t.Fatal("lifecycle record survived successful workspace removal")
	}
	terminalNotices := 0
	for _, text := range sent.texts() {
		if strings.HasPrefix(text, "Create cancelled; starting cleanup.") || strings.HasPrefix(text, "Servitor is shutting down; starting cleanup.") {
			terminalNotices++
		}
	}
	if terminalNotices != 1 {
		t.Fatalf("terminal cleanup notices = %d, want one: %q", terminalNotices, sent.texts())
	}
	activity.assertIdle(t, "review:start", "review:stop", "apply:start", "apply:stop")
}

func TestCreatorActivatesReviewOnlyAfterFinalChunkDelivery(t *testing.T) {
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
create) mkdir -p "$XDG_STATE_HOME/ict/$2"; printf 'Do you want to perform these actions?'; read answer; [ "$answer" = yes ] && touch "$XDG_STATE_HOME/applied" ;;
destroy) touch "$XDG_STATE_HOME/cleanup"; rm -rf "$XDG_STATE_HOME/ict/$2" ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
case "$*" in
*create.tfplan*)
  printf '%s' '{"format_version":"1.2","resource_changes":['
  i=0; while [ "$i" -lt 240 ]; do
    [ "$i" -gt 0 ] && printf ','
    printf '%s' '{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"],"after":{"name":"servitor-cluster"}}}'
    i=$((i+1))
  done
  printf '%s' ']}' ;;
*) printf '%s' '{"format_version":"1.2","values":{"root_module":{"resources":[]}}}' ;;
esac
`)
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	creator := &Creator{ICT: &client, Store: store, Destroyer: NewManager(client, store, nil, "M1"), ConfigPath: "ict.yaml", TerraformPath: terraformPath, Defaults: command.CreateDefaults{Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "shared", KubernetesFlavor: "bx2.2x8"}}
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	finalDelivery := make(chan struct{})
	releaseDelivery := make(chan struct{})
	var once sync.Once
	notify := func(_ context.Context, notice Notice) error {
		if strings.Contains(notice.Text, "Reply with exact `yes`") {
			once.Do(func() { close(finalDelivery) })
			<-releaseDelivery
		}
		return nil
	}
	if err := creator.Start(context.Background(), request, "create --version 1.36", notify); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finalDelivery:
	case <-time.After(time.Second):
		t.Fatal("final review chunk was not delivered")
	}
	confirmed := make(chan error, 1)
	go func() { confirmed <- creator.Confirm(context.Background(), request, "yes", notify) }()
	select {
	case err := <-confirmed:
		t.Fatalf("confirmation returned before final delivery completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := os.Stat(filepath.Join(stateHome, "applied")); !os.IsNotExist(err) {
		t.Fatalf("apply started before review activation: %v", err)
	}
	close(releaseDelivery)
	if err := <-confirmed; err != nil {
		t.Fatal(err)
	}
	waitForFile(t, filepath.Join(stateHome, "applied"))
	waitForStatus(t, store, "U1", statusReady)

	failedRequest := Request{UserID: "U2", Channel: "C1", ThreadTimestamp: "2.3"}
	failureNotifier := func(_ context.Context, notice Notice) error {
		if strings.Contains(notice.Text, "Planned resources:") {
			return errors.New("inject intermediate review chunk failure")
		}
		return nil
	}
	if err := creator.Start(context.Background(), failedRequest, "create --version 1.36", failureNotifier); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, filepath.Join(stateHome, "cleanup"))
	waitForStatus(t, store, "U2", statusResolved)
}

func TestCreatorPauseRacingStartCannotCreateAfterPause(t *testing.T) {
	creator, control, gate, stateHome, store := newAdmissionRaceCreator(t)
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}

	entered, release := gate.arm()
	started := make(chan error, 1)
	go func() {
		started <- creator.Start(context.Background(), request, "create --version 1.36", nil)
	}()
	<-entered

	if status, changed, err := control.Pause(); err != nil || !changed || status.Mode != admission.Paused {
		t.Fatalf("pause = (%+v, %v, %v)", status, changed, err)
	}
	close(release)
	if err := <-started; err == nil || err.Error() != "admission is paused" {
		t.Fatalf("start error = %v, want paused admission", err)
	}
	if _, err := os.Stat(filepath.Join(stateHome, "create-started")); !os.IsNotExist(err) {
		t.Fatalf("ICT create started after pause: %v", err)
	}
	if _, found := store.Get(request.UserID); found {
		t.Fatal("create persisted a lifecycle after pause")
	}
}

func TestCreatorPauseRacingConfirmationCannotPersistApproval(t *testing.T) {
	creator, control, gate, stateHome, store := newAdmissionRaceCreator(t)
	request := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.2"}
	if err := creator.Start(context.Background(), request, "create --version 1.36", nil); err != nil {
		t.Fatal(err)
	}
	waitForReviewReady(t, creator, request.UserID)

	entered, release := gate.arm()
	confirmed := make(chan struct {
		outcome ConfirmationOutcome
		err     error
	}, 1)
	go func() {
		outcome, err := creator.ConfirmResult(context.Background(), request, "yes", nil)
		confirmed <- struct {
			outcome ConfirmationOutcome
			err     error
		}{outcome, err}
	}()
	<-entered

	if status, changed, err := control.Pause(); err != nil || !changed || status.Mode != admission.Paused {
		t.Fatalf("pause = (%+v, %v, %v)", status, changed, err)
	}
	close(release)
	result := <-confirmed
	if result.outcome != ConfirmationRejected || result.err != nil {
		t.Fatalf("confirmation = (%q, %v), want rejected without error", result.outcome, result.err)
	}
	if record, found := store.Get(request.UserID); found && record.Status == statusApplying {
		t.Fatalf("approval persisted after pause: %+v", record)
	}
	if _, err := os.Stat(filepath.Join(stateHome, "applied")); !os.IsNotExist(err) {
		t.Fatalf("apply started after pause: %v", err)
	}
	waitForFile(t, filepath.Join(stateHome, "cleanup"))
	waitForStatus(t, store, request.UserID, statusResolved)
}

type admissionGate struct {
	*admission.Control
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
}

func (g *admissionGate) arm() (entered, release chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	return g.entered, g.release
}

func (g *admissionGate) Admit(operation func() error) (bool, uint64, error) {
	g.mu.Lock()
	entered, release := g.entered, g.release
	g.entered, g.release = nil, nil
	g.mu.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
	return g.Control.Admit(operation)
}

func newAdmissionRaceCreator(t *testing.T) (*Creator, *admission.Control, *admissionGate, string, *state.LifecycleStore) {
	t.Helper()
	directory := t.TempDir()
	stateHome := filepath.Join(directory, "state-home")
	if err := os.Mkdir(stateHome, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", stateHome)
	ictPath := createExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
create) touch "$XDG_STATE_HOME/create-started"; mkdir -p "$XDG_STATE_HOME/ict/$2"; printf 'Do you want to perform these actions?'; read answer; [ "$answer" = yes ] && touch "$XDG_STATE_HOME/applied" ;;
destroy) touch "$XDG_STATE_HOME/cleanup"; rm -rf "$XDG_STATE_HOME/ict/$2" ;;
esac
`)
	terraformPath := createExecutable(t, directory, "terraform", `#!/bin/sh
case "$*" in
*create.tfplan*) printf '%s' '{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"]}}]}' ;;
*) printf '%s' '{"format_version":"1.2","values":{"root_module":{"resources":[]}}}' ;;
esac
`)
	store, err := state.OpenLifecycleStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newDiagnosticClient(t, ictPath)
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gate := &admissionGate{Control: control}
	manager := NewManager(client, store, nil, "M1")
	creator := &Creator{
		ICT:                 &client,
		Store:               store,
		Destroyer:           manager,
		ConfigPath:          filepath.Join(directory, "ict.yaml"),
		TerraformPath:       terraformPath,
		ConfirmationTimeout: time.Minute,
		Defaults: command.CreateDefaults{
			Target:           "synthetic-target",
			Provider:         "vpc-gen2",
			ResourceGroup:    "Default",
			Zone:             "us-south-1",
			VPCID:            "shared",
			KubernetesFlavor: "bx2.2x8",
		},
		Admission: gate,
	}
	return creator, control, gate, stateHome, store
}

func newDiagnosticClient(t *testing.T, path string) (ictclient.Client, *diagnostics.Logger) {
	t.Helper()
	logger, err := diagnostics.Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	client := ictclient.NewClient(path, nil)
	client.Diagnostics = logger
	root := os.Getenv("SERVITOR_TEST_ICT_ROOT")
	if root == "" && os.Getenv("XDG_STATE_HOME") != "" {
		root = filepath.Join(os.Getenv("XDG_STATE_HOME"), "ict")
	}
	if root == "" {
		root = filepath.Join(t.TempDir(), "ict")
		t.Setenv("SERVITOR_TEST_ICT_ROOT", root)
	}
	client.InventorySource = func(context.Context) (state.WorkspaceInventory, error) {
		if err := os.MkdirAll(root, 0o700); err != nil {
			return state.WorkspaceInventory{}, err
		}
		root, err := filepath.EvalSymlinks(root)
		if err != nil {
			return state.WorkspaceInventory{}, err
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return state.WorkspaceInventory{}, err
		}
		inventory := state.WorkspaceInventory{Version: 1, StateRoot: root}
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "U") {
				inventory.Workspaces = append(inventory.Workspaces, state.WorkspaceLocation{ID: entry.Name(), Path: filepath.Join(root, entry.Name())})
			}
		}
		sort.Slice(inventory.Workspaces, func(i, j int) bool { return inventory.Workspaces[i].ID < inventory.Workspaces[j].ID })
		return inventory, nil
	}
	return client, logger
}

func refreshStoreWithWorkspace(t *testing.T, store *state.LifecycleStore, id string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "ict")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(root, id)
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(state.WorkspaceInventory{Version: 1, StateRoot: root, Workspaces: []state.WorkspaceLocation{{ID: id, Path: workspace}}}); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func diagnosticContents(t *testing.T, logger *diagnostics.Logger, id, stateID string) string {
	t.Helper()
	writer, err := logger.Writer(id, stateID)
	if err != nil {
		t.Fatal(err)
	}
	segments, err := writer.Segments()
	if err != nil {
		t.Fatal(err)
	}
	var contents strings.Builder
	for _, segment := range segments {
		data, err := os.ReadFile(segment)
		if err != nil {
			t.Fatal(err)
		}
		contents.Write(data)
	}
	return contents.String()
}

func createExecutable(t *testing.T, directory, name, body string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
func waitForText(t *testing.T, sent *notices, wanted string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(strings.Join(sent.texts(), "\n"), wanted) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("notices did not contain %q: %q", wanted, sent.texts())
}
func waitForNoticeCount(t *testing.T, sent *notices, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(sent.texts()) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("notice count = %d, want at least %d", len(sent.texts()), want)
}

func waitForReviewReady(t *testing.T, creator *Creator, user string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		creator.mu.Lock()
		_, ready := creator.reviews[user]
		creator.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("review was not ready for confirmation for %s", user)
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("file was not created: %s", path)
}

func waitForStatus(t *testing.T, store *state.LifecycleStore, user, status string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if record, ok := store.Get(user); ok && record.Status == status {
			return
		}
		if status == statusResolved {
			if _, ok := store.Get(user); !ok {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	record, _ := store.Get(user)
	t.Fatalf("status = %q, want %q", record.Status, status)
}
