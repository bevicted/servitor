package lifecycle

import (
	"context"
	"fmt"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/ict"
	"github.com/bevicted/servitor/internal/state"
	"github.com/bevicted/servitor/internal/terraformview"
)

const (
	statusReview           = "review"
	statusApplying         = "applying"
	statusReady            = "ready"
	terraformJSONMaxOutput = 1024 * 1024
)

// Creator owns a user create process until it converges on the existing destroy path.
// CreateHandler lets Slack route creation, confirmations, and graceful cancellation.
type CreateHandler interface {
	Start(context.Context, Request, string, Notifier) error
	ConfirmResult(context.Context, Request, string, Notifier) (ConfirmationOutcome, error)
	Cancel(context.Context, Request, Notifier) bool
}

// Admission is the linearizable user-command admission boundary.
type Admission interface{ Accepting() bool }

type admissionGeneration interface{ Generation() uint64 }

// ConfirmationOutcome describes the lifecycle state reached by a raw reply.
// It lets transport routing give feedback only to an authorized lifecycle thread.
type ConfirmationOutcome string

const (
	ConfirmationNoLifecycle ConfirmationOutcome = "no-lifecycle"
	ConfirmationWrongThread ConfirmationOutcome = "wrong-thread"
	ConfirmationPreReview   ConfirmationOutcome = "pre-review"
	ConfirmationReview      ConfirmationOutcome = "review"
	ConfirmationApplying    ConfirmationOutcome = "applying"
	ConfirmationReady       ConfirmationOutcome = "ready"
	ConfirmationCleanup     ConfirmationOutcome = "cleanup"
	ConfirmationUnresolved  ConfirmationOutcome = "unresolved"
	ConfirmationApproved    ConfirmationOutcome = "approved"
	ConfirmationRejected    ConfirmationOutcome = "rejected"
)

type existingLifecycleError struct{ text string }

func (e existingLifecycleError) Error() string      { return "you already have a cluster lifecycle" }
func (e existingLifecycleError) UserNotice() string { return e.text }

// AcceptanceDeliveryError means Slack did not receive create acceptance, so no
// ICT preflight or process was started.
type AcceptanceDeliveryError struct{ Err error }

func (e *AcceptanceDeliveryError) Error() string { return "deliver create acceptance" }
func (e *AcceptanceDeliveryError) Unwrap() error { return e.Err }

// UserNoticeError carries a concise Slack-safe explanation for an operation error.
type UserNoticeError interface {
	error
	UserNotice() string
}

type createPreflightError struct {
	err            error
	diagnosticID   string
	summary        string
	cleanupRunning bool
}

func (e createPreflightError) Error() string { return e.summary }
func (e createPreflightError) Unwrap() error { return e.err }
func (e createPreflightError) UserNotice() string {
	cleanup := "No cleanup is running; this requires maintainer attention."
	if e.cleanupRunning {
		cleanup = "Cleanup is running."
	}
	return e.summary + ". " + cleanup + " Diagnostic ID: " + DiagnosticReference(e.diagnosticID) + "."
}

type Creator struct {
	ICT                        *ict.Client
	Store                      *state.LifecycleStore
	Destroyer                  Destroyer
	Defaults                   command.CreateDefaults
	ConfigPath                 string
	ConfirmationTimeout, Lease time.Duration
	TerraformPath              string
	Diagnostics                *diagnostics.Logger
	Logf                       func(string, ...any)
	Clock                      Clock
	Admission                  Admission
	OnActivity                 func(string, bool)
	mu                         sync.Mutex
	processes                  map[string]*ict.CreateProcess
	cancelling                 map[string]*ict.CreateProcess
	finishing                  map[string]*ict.CreateProcess
	reviews                    map[string]review
	activities                 map[string]string
	userLocks                  map[string]*sync.Mutex
}

type review struct {
	process  *ict.CreateProcess
	deadline time.Time
}

func (c *Creator) Start(ctx context.Context, request Request, text string, notify Notifier) error {
	if c.ICT == nil || c.Store == nil || c.Destroyer == nil || c.ConfigPath == "" || c.TerraformPath == "" {
		return fmt.Errorf("create is not configured")
	}
	parsed, err := command.ParseCreate(text, c.Defaults)
	if err != nil {
		return err
	}
	unlock := c.lockUser(request.UserID)
	defer unlock()

	c.mu.Lock()
	if c.processes == nil {
		c.processes = map[string]*ict.CreateProcess{}
	}
	if _, busy := c.processes[request.UserID]; busy {
		c.mu.Unlock()
		return fmt.Errorf("you already have a cluster lifecycle")
	}
	c.mu.Unlock()
	if record, found := c.Store.Get(request.UserID); found {
		return existingLifecycleError{text: existingAllocationNotice(record, c.now())}
	}
	if _, found := c.Store.WorkspacePath(request.UserID); found {
		return existingLifecycleError{text: "Existing resources or incomplete cleanup were found for your account. Use `@servitor done` to clean up."}
	}
	diagnosticID, err := diagnostics.NewID()
	if err != nil {
		return err
	}
	c.logDiagnostic(request.UserID, diagnosticID, "create admitted")
	if err := c.notice(notify, request, "Command accepted.\nPlanning..."); err != nil {
		return &AcceptanceDeliveryError{Err: err}
	}
	client, err := c.ICT.WithDiagnostic(diagnosticID, request.UserID)
	if err != nil {
		return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to prepare private diagnostics"}
	}
	inventory, err := client.WorkspaceInventory(ctx)
	if err != nil {
		return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to verify existing ICT state"}
	}
	if err := c.Store.Refresh(inventory); err != nil {
		return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to verify existing ICT state"}
	}
	if _, has := c.Store.WorkspacePath(request.UserID); has {
		return existingLifecycleError{text: "Existing resources or incomplete cleanup were found for your account. Use `@servitor done` to clean up."}
	}
	var process *ict.CreateProcess
	admitted, admissionGeneration, err := c.admit(func() error {
		var startErr error
		process, startErr = client.StartCreate(request.UserID, c.ConfigPath, parsed.Args)
		return startErr
	})
	if !admitted {
		return admissionClosedError{}
	}
	if err != nil {
		return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to start create"}
	}
	// ICT reserves the workspace in its child process. Wait briefly for the
	// authoritative inventory to observe that reservation before writing inside it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		inventory, err = client.WorkspaceInventory(ctx)
		if err == nil {
			err = c.Store.Refresh(inventory)
		}
		if err == nil {
			if _, found := c.Store.WorkspacePath(request.UserID); found {
				break
			}
			err = fmt.Errorf("reserved ICT workspace is absent")
		}
		select {
		case <-process.Done():
			return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to verify reserved ICT state"}
		default:
		}
		if time.Now().After(deadline) {
			_ = process.Confirm(false)
			go c.cleanupUnrecordedProcess(request, process, notify)
			return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to verify reserved ICT state", cleanupRunning: true}
		}
		time.Sleep(10 * time.Millisecond)
	}
	record := state.LifecycleRecord{UserID: request.UserID, Channel: request.Channel, ThreadTimestamp: request.ThreadTimestamp, Status: statusReview, DiagnosticRef: diagnosticID, UpdatedAt: c.now()}
	admitted, _, err = c.admit(func() error { return c.Store.Put(record) })
	if !admitted {
		_ = process.Confirm(false)
		go c.cleanupUnrecordedProcess(request, process, notify)
		return admissionClosedError{}
	}
	if err != nil {
		_ = process.Confirm(false)
		go c.cleanupUnrecordedProcess(request, process, notify)
		return createPreflightError{err: err, diagnosticID: diagnosticID, summary: "Unable to record create lifecycle", cleanupRunning: true}
	}
	c.event(record, "review lifecycle started")
	c.mu.Lock()
	c.processes[request.UserID] = process
	c.mu.Unlock()
	c.setActivity(request.UserID, "review")
	if !c.admissionOpen() || c.admissionGeneration() != admissionGeneration {
		go c.DeclinePendingReviews(notify)
		return nil
	}
	go c.awaitReview(request, process, parsed, notify)
	return nil
}

func (c *Creator) awaitReview(request Request, process *ict.CreateProcess, parsed command.CreateRequest, notify Notifier) {
	deadline := c.now().Add(c.confirmationTimeout())
review:
	for {
		select {
		case <-process.Done():
			c.cleanupAfterFailure(request, process, notify, "Create failed before review.")
			return
		default:
		}
		select {
		case <-process.Prompted():
			break review
		default:
		}
		if !c.now().Before(deadline) {
			_ = process.Confirm(false)
			_ = c.notice(notify, request, "Plan rejected.\nCleaning up...")
			_ = process.Wait()
			c.cleanupAfterReviewDecline(request, process, notify, "Create review timed out.")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	plan, err := c.showPlan(request.UserID)
	if err != nil {
		_ = process.Confirm(false)
		_ = process.Wait()
		c.cleanupAfterReviewDecline(request, process, notify, "Unable to review the saved Terraform plan.")
		return
	}

	// Confirmation and review activation share this lock. A fast yes therefore
	// waits until every review chunk has been delivered and authority is active.
	unlock := c.lockUser(request.UserID)
	defer unlock()
	record, found := c.Store.Get(request.UserID)
	if !found || record.Status != statusReview || !c.active(request.UserID, process) {
		return
	}
	record.ClusterName = plannedClusterName(plan)
	if record.ClusterName == "-" {
		record.ClusterName = ""
	}
	record.Location = parsed.Location
	record.UpdatedAt = c.now()
	if err := c.Store.Put(record); err != nil {
		_ = process.Confirm(false)
		_ = process.Wait()
		c.cleanupAfterReviewDecline(request, process, notify, "Unable to record the cluster review.")
		return
	}
	for _, text := range reviewText(parsed, plan) {
		if err := c.notice(notify, request, text); err != nil {
			_ = process.Confirm(false)
			_ = process.Wait()
			c.cleanupAfterReviewDecline(request, process, notify, "Review delivery failed.")
			return
		}
	}
	reviewDeadline := c.now().Add(c.confirmationTimeout())
	c.mu.Lock()
	if c.processes[request.UserID] != process {
		c.mu.Unlock()
		return
	}
	if c.reviews == nil {
		c.reviews = make(map[string]review)
	}
	c.reviews[request.UserID] = review{process: process, deadline: reviewDeadline}
	c.mu.Unlock()
	go c.expireReview(request, process, notify, reviewDeadline)
}
func (c *Creator) Confirm(ctx context.Context, request Request, text string, notify Notifier) error {
	_, err := c.ConfirmResult(ctx, request, text, notify)
	return err
}

// ConfirmResult handles an exact raw review reply and exposes why it was or was
// not accepted. Callers must keep non-owner outcomes silent.
func (c *Creator) ConfirmResult(_ context.Context, request Request, text string, notify Notifier) (ConfirmationOutcome, error) {
	unlock := c.lockUser(request.UserID)
	defer unlock()

	record, found := c.Store.Get(request.UserID)
	if !found {
		return ConfirmationNoLifecycle, nil
	}
	if record.Channel != request.Channel || record.ThreadTimestamp != request.ThreadTimestamp {
		return ConfirmationWrongThread, nil
	}
	c.mu.Lock()
	process := c.processes[request.UserID]
	activeReview, reviewed := c.reviews[request.UserID]
	c.mu.Unlock()
	switch record.Status {
	case statusApplying:
		return ConfirmationApplying, nil
	case statusReady:
		return ConfirmationReady, nil
	case statusCleanup:
		return ConfirmationCleanup, nil
	case statusUnresolved:
		return ConfirmationUnresolved, nil
	case statusReview:
		if process == nil || !reviewed || activeReview.process != process {
			return ConfirmationPreReview, nil
		}
	default:
		return ConfirmationNoLifecycle, nil
	}
	if !c.now().Before(activeReview.deadline) {
		c.clearReview(request.UserID, process)
		_ = process.Confirm(false)
		_ = c.notice(notify, request, "Plan rejected.\nCleaning up...")
		go c.waitDeclined(request, process, notify)
		return ConfirmationRejected, nil
	}
	switch text {
	case "no":
		c.clearReview(request.UserID, process)
		if err := process.Confirm(false); err != nil {
			return ConfirmationReview, err
		}
		_ = c.notice(notify, request, "Plan rejected.\nCleaning up...")
		go c.waitDeclined(request, process, notify)
		return ConfirmationRejected, nil
	case "yes":
		applying := record
		applying.Status = statusApplying
		applying.UpdatedAt = c.now()
		admitted, _, err := c.admit(func() error { return c.Store.Put(applying) })
		if !admitted {
			c.clearReview(request.UserID, process)
			_ = process.Confirm(false)
			_ = c.notice(notify, request, "Plan rejected.\nCleaning up...")
			go c.waitDeclined(request, process, notify)
			return ConfirmationRejected, nil
		}
		if err != nil {
			c.event(record, "approval persistence failed; cleanup started")
			c.clearReview(request.UserID, process)
			if declineErr := process.Confirm(false); declineErr != nil {
				c.event(record, "approval decline failed: "+declineErr.Error())
			}
			go c.waitApprovalPersistenceFailure(request, process, notify)
			return ConfirmationReview, createPreflightError{err: err, diagnosticID: record.DiagnosticRef, summary: "Unable to record create approval", cleanupRunning: true}
		}
		c.event(applying, "review approved; apply started")
		c.logDiagnostic(request.UserID, applying.DiagnosticRef, "lifecycle transition=applying")
		c.clearReview(request.UserID, process)
		c.setActivity(request.UserID, "apply")
		_ = c.notice(notify, request, "Plan approved.\nCreating... This may take 30m-90m.\n\nDiagnostic ID: "+DiagnosticReference(record.DiagnosticRef))
		if err := process.Confirm(true); err != nil {
			c.event(applying, "approval confirmation delivery failed: "+err.Error())
		}
		go c.waitApply(request, process, notify)
		return ConfirmationApproved, nil
	default:
		return ConfirmationReview, nil
	}
}
func (c *Creator) expireReview(request Request, process *ict.CreateProcess, notify Notifier, deadline time.Time) {
	for c.now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	unlock := c.lockUser(request.UserID)
	defer unlock()

	c.mu.Lock()
	activeReview, reviewed := c.reviews[request.UserID]
	c.mu.Unlock()
	record, found := c.Store.Get(request.UserID)
	if found && record.Status == statusReview && reviewed && activeReview.process == process && activeReview.deadline.Equal(deadline) {
		c.clearReview(request.UserID, process)
		_ = process.Confirm(false)
		_ = c.notice(notify, request, "Plan rejected.\nCleaning up...")
		go c.waitDeclined(request, process, notify)
	}
}
func (c *Creator) waitDeclined(request Request, process *ict.CreateProcess, notify Notifier) {
	_ = process.Wait()
	c.cleanupAfterReviewDecline(request, process, notify, "Create review declined.")
}

func (c *Creator) waitApprovalPersistenceFailure(request Request, process *ict.CreateProcess, notify Notifier) {
	_ = process.Wait()
	c.cleanupAfterFailure(request, process, notify, "Create approval could not be recorded; starting cleanup.")
}
func (c *Creator) waitApply(request Request, process *ict.CreateProcess, notify Notifier) {
	if err := process.Wait(); err != nil {
		unlock := c.lockUser(request.UserID)
		defer unlock()
		if !c.active(request.UserID, process) {
			return
		}
		c.cleanupAfterFailure(request, process, notify, "Create failed; starting cleanup.")
		return
	}
	completedAt := c.now()
	view, err := c.showState(request.UserID)
	unlock := c.lockUser(request.UserID)
	defer unlock()
	if !c.active(request.UserID, process) {
		return
	}
	if err != nil {
		c.cleanupAfterFailure(request, process, notify, "Create completed but resource reporting failed; starting cleanup.")
		return
	}
	record, _ := c.Store.Get(request.UserID)
	expiry := completedAt.Add(c.lease())
	record.Status = statusReady
	c.event(record, "apply completed; lifecycle ready")
	c.logDiagnostic(request.UserID, record.DiagnosticRef, "lifecycle transition=ready")
	record.LeaseExpiresAt = expiry
	record.UpdatedAt = completedAt
	if err := c.Store.Put(record); err != nil {
		c.cleanupAfterFailure(request, process, notify, "Create completed but lifecycle persistence failed; starting cleanup.")
		return
	}
	c.remove(request.UserID, process)
	c.clearActivity(request.UserID)
	for _, text := range readyTexts(view, expiry, completedAt) {
		c.notice(notify, request, text)
	}
	c.Destroyer.ScheduleLease(request, record, notify)
}

// Shutdown declines pending reviews or gracefully interrupts applies before cleanup.
func (c *Creator) Shutdown(notify Notifier) {
	c.mu.Lock()
	processes := make(map[string]*ict.CreateProcess, len(c.processes))
	maps.Copy(processes, c.processes)
	c.mu.Unlock()
	for user, process := range processes {
		unlock := c.lockUser(user)
		c.mu.Lock()
		current := c.processes[user]
		if current != process || c.cancelling[user] == process {
			c.mu.Unlock()
			unlock()
			continue
		}
		c.mu.Unlock()
		record, found := c.Store.Get(user)
		if !found {
			unlock()
			continue
		}
		c.mu.Lock()
		if c.processes[user] != process || c.cancelling[user] == process {
			c.mu.Unlock()
			unlock()
			continue
		}
		if c.cancelling == nil {
			c.cancelling = make(map[string]*ict.CreateProcess)
		}
		c.cancelling[user] = process
		c.mu.Unlock()
		unlock()
		request := Request{UserID: user, Channel: record.Channel, ThreadTimestamp: record.ThreadTimestamp}
		if record.Status == statusReview {
			_ = process.Confirm(false)
		} else {
			_ = process.Interrupt()
		}
		_ = process.Wait()
		if record.Status == statusReview {
			c.cleanupAfterReviewDecline(request, process, notify, "Servitor is shutting down; starting cleanup.")
		} else {
			c.cleanupAfterFailure(request, process, notify, "Servitor is shutting down; starting cleanup.")
		}
	}
}
func (c *Creator) Cancel(_ context.Context, request Request, notify Notifier) bool {
	unlock := c.lockUser(request.UserID)
	defer unlock()

	record, found := c.Store.Get(request.UserID)
	if request.RawThread && (!found || record.Channel != request.Channel || record.ThreadTimestamp != request.ThreadTimestamp) {
		return false
	}

	c.mu.Lock()
	process := c.processes[request.UserID]
	if process != nil && c.cancelling[request.UserID] == process {
		c.mu.Unlock()
		return true
	}
	if process != nil {
		if c.cancelling == nil {
			c.cancelling = make(map[string]*ict.CreateProcess)
		}
		c.cancelling[request.UserID] = process
	}
	c.mu.Unlock()
	if process == nil {
		return false
	}
	c.clearReview(request.UserID, process)
	if record.Status == statusReview {
		_ = process.Confirm(false)
	} else {
		_ = process.Interrupt()
	}
	go func() {
		_ = process.Wait()
		unlock := c.lockUser(request.UserID)
		defer unlock()
		if record.Status == statusReview {
			c.cleanupAfterReviewDecline(request, process, notify, "Create cancelled; starting cleanup.")
		} else {
			c.cleanupAfterFailure(request, process, notify, "Create cancelled; starting cleanup.")
		}
	}()
	return true
}
func (c *Creator) cleanupUnrecordedProcess(request Request, process *ict.CreateProcess, notify Notifier) {
	_ = process.Wait()
	_ = c.Destroyer.RequestDestroy(context.Background(), request, notify)
}

func (c *Creator) cleanupAfterReviewDecline(request Request, process *ict.CreateProcess, notify Notifier, text string) {
	if !c.claimFinish(request.UserID, process) {
		return
	}
	c.remove(request.UserID, process)
	record, found := c.Store.Get(request.UserID)
	if found && c.workspaceAbsent(request.UserID) {
		c.event(record, "review declined; ICT removed workspace")
		c.resolveDiagnostic(record)
		c.Store.RemoveAbsentWorkspace(request.UserID)
		if diagnostics.ValidID(record.DiagnosticRef) {
			text += " Diagnostic ID: " + DiagnosticReference(record.DiagnosticRef) + "."
		}
		c.notice(notify, request, text)
		c.notice(notify, request, "Cleanup complete.")
		c.clearActivity(request.UserID)
		return
	}
	if found && diagnostics.ValidID(record.DiagnosticRef) {
		text += " Diagnostic ID: " + DiagnosticReference(record.DiagnosticRef) + "."
	}
	c.notice(notify, request, text)
	_ = c.Destroyer.RequestDestroy(context.Background(), request, notify)
	c.clearActivity(request.UserID)
}

func (c *Creator) cleanupAfterFailure(request Request, process *ict.CreateProcess, notify Notifier, text string) {
	if !c.claimFinish(request.UserID, process) {
		return
	}
	c.remove(request.UserID, process)
	if record, found := c.Store.Get(request.UserID); found && diagnostics.ValidID(record.DiagnosticRef) {
		text += " Diagnostic ID: " + DiagnosticReference(record.DiagnosticRef) + "."
	}
	c.notice(notify, request, text)
	_ = c.Destroyer.RequestDestroy(context.Background(), request, notify)
	c.clearActivity(request.UserID)
}

func (c *Creator) workspaceAbsent(user string) bool {
	workspace, found := c.Store.WorkspacePath(user)
	if !found {
		return false
	}
	_, err := os.Stat(workspace)
	return os.IsNotExist(err)
}

func (c *Creator) resolveDiagnostic(record state.LifecycleRecord) {
	logger := c.Diagnostics
	if logger == nil && c.ICT != nil {
		logger = c.ICT.Diagnostics
	}
	if logger != nil && diagnostics.ValidID(record.DiagnosticRef) {
		_ = logger.Resolve(record.DiagnosticRef)
	}
}
func (c *Creator) remove(user string, process *ict.CreateProcess) {
	c.mu.Lock()
	if c.processes[user] == process {
		delete(c.processes, user)
	}
	if c.cancelling[user] == process {
		delete(c.cancelling, user)
	}
	if c.finishing[user] == process {
		delete(c.finishing, user)
	}
	if activeReview, reviewed := c.reviews[user]; reviewed && activeReview.process == process {
		delete(c.reviews, user)
	}
	c.mu.Unlock()
}

func (c *Creator) claimFinish(user string, process *ict.CreateProcess) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.processes[user] != process || c.finishing[user] == process {
		return false
	}
	if c.finishing == nil {
		c.finishing = make(map[string]*ict.CreateProcess)
	}
	c.finishing[user] = process
	return true
}

// DeclinePendingReviews rejects every active review without waiting for its
// process. It is called after admission has been atomically paused.
func (c *Creator) DeclinePendingReviews(notify Notifier) {
	c.mu.Lock()
	processes := make(map[string]*ict.CreateProcess, len(c.processes))
	maps.Copy(processes, c.processes)
	c.mu.Unlock()
	for user, process := range processes {
		unlock := c.lockUser(user)
		record, found := c.Store.Get(user)
		c.mu.Lock()
		current := c.processes[user]
		c.mu.Unlock()
		if !found || record.Status != statusReview || current != process {
			unlock()
			continue
		}
		c.clearReview(user, process)
		_ = process.Confirm(false)
		request := requestFor(record)
		_ = c.notice(notify, request, "Plan rejected.\nCleaning up...")
		go c.waitDeclined(request, process, notify)
		unlock()
	}
}

func (c *Creator) active(user string, process *ict.CreateProcess) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processes[user] == process && c.cancelling[user] != process
}

func (c *Creator) lockUser(user string) func() {
	c.mu.Lock()
	if c.userLocks == nil {
		c.userLocks = make(map[string]*sync.Mutex)
	}
	lock := c.userLocks[user]
	if lock == nil {
		lock = &sync.Mutex{}
		c.userLocks[user] = lock
	}
	c.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (c *Creator) admissionOpen() bool {
	return c.Admission == nil || c.Admission.Accepting()
}

func (c *Creator) admit(operation func() error) (admitted bool, generation uint64, err error) {
	if admission, ok := c.Admission.(interface {
		Admit(func() error) (bool, uint64, error)
	}); ok {
		return admission.Admit(operation)
	}
	if !c.admissionOpen() {
		return false, c.admissionGeneration(), nil
	}
	return true, c.admissionGeneration(), operation()
}

func (c *Creator) admissionGeneration() uint64 {
	if admission, ok := c.Admission.(admissionGeneration); ok {
		return admission.Generation()
	}
	return 0
}

type admissionClosedError struct{}

func (admissionClosedError) Error() string { return "admission is paused" }
func (admissionClosedError) UserNotice() string {
	return "Servitor is paused; modifying commands are unavailable."
}

func (c *Creator) setActivity(user, kind string) {
	c.mu.Lock()
	if c.activities == nil {
		c.activities = make(map[string]string)
	}
	prior := c.activities[user]
	if prior == kind {
		c.mu.Unlock()
		return
	}
	c.activities[user] = kind
	c.mu.Unlock()
	if prior != "" && c.OnActivity != nil {
		c.OnActivity(prior, false)
	}
	if c.OnActivity != nil {
		c.OnActivity(kind, true)
	}
}

func (c *Creator) clearActivity(user string) {
	c.mu.Lock()
	prior := c.activities[user]
	delete(c.activities, user)
	c.mu.Unlock()
	if prior != "" && c.OnActivity != nil {
		c.OnActivity(prior, false)
	}
}

func (c *Creator) clearReview(user string, process *ict.CreateProcess) {
	c.mu.Lock()
	if activeReview, reviewed := c.reviews[user]; reviewed && activeReview.process == process {
		delete(c.reviews, user)
	}
	c.mu.Unlock()
}
func (c *Creator) showPlan(user string) (terraformview.Plan, error) {
	data, err := c.terraformShow(user, ".cluster/create.tfplan")
	if err != nil {
		return terraformview.Plan{}, err
	}
	return terraformview.ParsePlan(data)
}
func (c *Creator) showState(user string) (terraformview.State, error) {
	data, err := c.terraformShow(user)
	if err != nil {
		return terraformview.State{}, err
	}
	return terraformview.ParseState(data)
}
func (c *Creator) terraformShow(user string, path ...string) ([]byte, error) {
	workspace, found := c.Store.WorkspacePath(user)
	if !found {
		return nil, fmt.Errorf("terraform show: validated ICT workspace is missing")
	}
	args := []string{"-chdir=" + workspace, "show", "-json"}
	if len(path) > 0 {
		args = append(args, path[0])
	}
	record, found := c.Store.Get(user)
	if !found {
		return nil, fmt.Errorf("terraform show: lifecycle record is missing")
	}
	writer, err := c.writer(record)
	if err != nil {
		return nil, fmt.Errorf("open diagnostic log: %w", err)
	}
	runner := command.Runner{MaxOutput: terraformJSONMaxOutput, Log: writer}
	result, err := runner.Run(context.Background(), c.TerraformPath, args...)
	if result.StdoutTruncated {
		truncated := fmt.Errorf("terraform JSON stdout exceeds %d byte limit", terraformJSONMaxOutput)
		writer.Error(truncated)
		return nil, truncated
	}
	if err != nil {
		return nil, err
	}
	return []byte(result.Stdout), nil
}
func (c *Creator) writer(record state.LifecycleRecord) (*diagnostics.Writer, error) {
	logger := c.Diagnostics
	if logger == nil && c.ICT != nil {
		logger = c.ICT.Diagnostics
	}
	if logger == nil {
		return nil, fmt.Errorf("diagnostics are not configured")
	}
	if !diagnostics.ValidID(record.DiagnosticRef) {
		return nil, fmt.Errorf("invalid diagnostic ID")
	}
	return logger.Writer(record.DiagnosticRef, record.UserID)
}

func (c *Creator) event(record state.LifecycleRecord, message string) {
	if writer, err := c.writer(record); err == nil && writer != nil {
		writer.Event(message)
	}
}

func (c *Creator) logDiagnostic(stateID, id, message string) {
	if c.Logf == nil || c.Diagnostics == nil || !diagnostics.ValidID(id) {
		return
	}
	path, err := c.Diagnostics.Path(id)
	if err != nil {
		c.Logf("diagnostic path failed state_id=%q diagnostic_id=%q: %v", stateID, id, err)
		return
	}
	c.Logf("lifecycle state_id=%q diagnostic_id=%q diagnostic_path=%q %s", stateID, id, path, message)
}

func (c *Creator) notice(notify Notifier, request Request, text string) error {
	if notify == nil {
		return nil
	}
	return notify(context.Background(), Notice{Channel: request.Channel, ThreadTimestamp: request.ThreadTimestamp, Text: text})
}
func (c *Creator) now() time.Time {
	if c.Clock != nil {
		return c.Clock.Now()
	}
	return time.Now()
}
func (c *Creator) confirmationTimeout() time.Duration {
	if c.ConfirmationTimeout > 0 {
		return c.ConfirmationTimeout
	}
	return 5 * time.Minute
}
func (c *Creator) lease() time.Duration {
	if c.Lease > 0 {
		return c.Lease
	}
	return 4 * time.Hour
}
