// Package slackbot implements the Slack command boundary.
package slackbot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/config"
	"github.com/bevicted/servitor/internal/inventory"
	"github.com/bevicted/servitor/internal/lifecycle"
	"github.com/bevicted/servitor/internal/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const maxSlackMessage = 3000

type EventStore interface {
	Claim(context.Context, string) (bool, error)
	Delivered(context.Context, string) (bool, error)
	MarkDelivered(context.Context, string) error
	Release(context.Context, string) error
	Seen(context.Context, string) (bool, error)
}
type Responder interface {
	Reply(context.Context, Response) error
}
type PermalinkLookup interface {
	Permalink(context.Context, string, string) (string, error)
}
type Message struct{ Channel, ChannelType, User, Text, Timestamp, ThreadTimestamp, Subtype, BotID string }
type Envelope struct {
	ID          string
	Message     Message
	Acknowledge func(context.Context) error
}
type Response struct{ Channel, Text, ThreadTimestamp string }

// Bot accepts authorized Slack commands and mutates only ServitorCluster.spec.
// The reconciler exclusively owns status and all Tekton execution.
type Bot struct {
	ChannelID, SelfUserID string
	Namespace             string
	Client                client.Client
	MaxAllocationsPerUser int
	Events                EventStore
	Defaults              command.CreateDefaults
	InventoryConfigMap    string
	InventoryConfigKey    string
	MaintainerIDs         []string
	InventoryMaximumAge   time.Duration
	PublicAuthTargets     []string
	Lease                 time.Duration
	RetryIntervals        []time.Duration
	Responder             Responder
	Permalinks            PermalinkLookup
	Clock                 func() time.Time
	Logf                  func(string, ...any)
}

func (b Bot) Handle(ctx context.Context, envelope Envelope) error {
	if envelope.Acknowledge != nil {
		if err := envelope.Acknowledge(ctx); err != nil {
			return fmt.Errorf("acknowledge Slack envelope: %w", err)
		}
	}
	if envelope.ID != "" {
		if b.Events == nil {
			return fmt.Errorf("record Slack event: receipt store is not configured")
		}
		seen, err := b.Events.Seen(ctx, envelope.ID)
		if err != nil {
			return fmt.Errorf("read Slack event receipt: %w", err)
		}
		if seen {
			return nil
		}
	}
	claim := func() error {
		if envelope.ID == "" {
			return nil
		}
		if _, err := b.Events.Claim(ctx, envelope.ID); err != nil {
			return fmt.Errorf("record Slack event: %w", err)
		}
		return nil
	}
	message := envelope.Message
	if message.Subtype != "" || message.BotID != "" || (b.SelfUserID != "" && message.User == b.SelfUserID) {
		return claim()
	}
	if message.ChannelType == "im" {
		b.handleDM(ctx, message, envelope.ID)
		return claim()
	}
	if message.Channel != b.ChannelID {
		return claim()
	}
	thread := message.Timestamp
	if message.ThreadTimestamp != "" {
		thread = message.ThreadTimestamp
	}
	reply := func(text string) { b.respond(ctx, message.Channel, thread, text) }
	if message.ThreadTimestamp != "" {
		switch message.Text {
		case "yes", "no":
			b.confirm(ctx, message, thread)
		case "done":
			b.cleanup(ctx, message, thread, true)
		case "destroy":
			b.cleanup(ctx, message, thread, false)
		case "auth":
			b.auth(ctx, message, thread, reply)
		default:
			if firstToken(message.Text) == "extend" {
				b.extend(ctx, message, thread, reply)
			}
		}
		return claim()
	}
	text, mentioned := channelCommand(message.Text, b.SelfUserID)
	if !mentioned {
		return claim()
	}
	switch firstToken(text) {
	case "help":
		b.respondHelp(text, reply, false)
	case "list":
		b.list(ctx, message.User, reply)
	case "create":
		if !b.create(ctx, message, text, reply) {
			return nil
		}
	case "done":
		b.cleanup(ctx, message, thread, true)
	case "destroy":
		b.cleanup(ctx, message, thread, false)
	default:
		reply(b.unknownText())
	}
	return claim()
}

func (b Bot) handleDM(ctx context.Context, message Message, eventID string) {
	respond := func(text string) { b.respond(ctx, message.Channel, "", text) }
	maintainer := b.isMaintainer(message.User)
	switch firstToken(message.Text) {
	case "help":
		b.respondHelp(message.Text, respond, maintainer)
	case "list":
		b.list(ctx, message.User, respond)
	case "refresh":
		if message.Text != "refresh inventory" || !maintainer {
			b.respondHelp("help", respond, false)
			return
		}
		b.requestInventoryRefresh(ctx, message, eventID, respond)
	case "create", "done", "destroy":
		respond(rejectedText(channelOnlyText(b.ChannelID)))
	case "extend":
		respond(rejectedText("`extend [N[h]]` is available only in your lifecycle thread."))
	default:
		respond(b.unknownText())
	}
}

func (b Bot) isMaintainer(user string) bool {
	for _, id := range b.MaintainerIDs {
		if user == id {
			return true
		}
	}
	return false
}

func (b Bot) requestInventoryRefresh(ctx context.Context, message Message, eventID string, respond func(string)) {
	if b.Client == nil || b.Namespace == "" || b.InventoryConfigMap == "" || b.InventoryConfigKey == "" {
		respond("Inventory refresh is unavailable. Try again later.")
		return
	}
	configMap := &corev1.ConfigMap{}
	if err := b.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: b.InventoryConfigMap}, configMap); err != nil {
		respond("Inventory refresh is unavailable. Try again later.")
		return
	}
	configured, err := inventory.LoadConfig([]byte(configMap.Data[b.InventoryConfigKey]))
	if err != nil || len(configured.Targets) == 0 {
		respond("Inventory refresh is unavailable. Try again later.")
		return
	}
	targets := make([]string, 0, len(configured.Targets))
	for target := range configured.Targets {
		targets = append(targets, target)
	}
	slices.Sort(targets)
	requestID := eventID
	if requestID == "" {
		requestID = "dm:" + message.Channel + ":" + message.Timestamp + ":" + message.User
	}
	request := state.ManualRefreshRequest{ID: requestID, ChannelID: message.Channel, OwnerID: message.User, Targets: targets}
	store := state.NewInventoryStore(b.Client, b.Namespace)
	registered := make([]string, 0, len(targets))
	alreadyRunning := false
	for _, target := range targets {
		_, active, _, err := store.RequestManualRefresh(ctx, target, request)
		if err != nil {
			for _, registeredTarget := range registered {
				if failureErr := store.FailManualRefreshRegistration(ctx, registeredTarget, request.ID, registered); failureErr != nil {
					respond("Inventory refresh is unavailable. Try again later.")
					return
				}
			}
			respond("Inventory refresh is unavailable. Try again later.")
			return
		}
		// A process can stop after registering an earlier target and before
		// registering this one. Continue past duplicate registrations so a
		// redelivery completes the same durable request without dispatching it
		// twice.
		registered = append(registered, target)
		alreadyRunning = alreadyRunning || active
	}
	if alreadyRunning {
		respond("Inventory refresh already running.")
		return
	}
	respond("Inventory refresh started.")
}

func (b Bot) create(ctx context.Context, message Message, text string, respond func(string)) bool {
	if b.Client == nil || b.Namespace == "" {
		respond(rejectedText("Create is unavailable. Inspect the allocation CR status and private cluster logs."))
		return true
	}
	name := allocationClusterName(message.Channel, message.Timestamp)
	existing := &servitorv1alpha1.ServitorCluster{}
	err := b.allocationReader().Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, existing)
	if err == nil {
		respond(b.existingAllocationNotice(ctx, existing, message.User))
		return true
	}
	if !apierrors.IsNotFound(err) {
		b.logf("get existing allocation: %v", err)
		respond(rejectedText("Unable to check an existing allocation. No operation was started."))
		return false
	}
	options, err := command.ParseCreateOptions(text)
	if err != nil {
		respond(rejectedText(err.Error()))
		return true
	}
	if b.InventoryConfigMap != "" {
		options, err = b.matchBareOptions(ctx, options)
		if err != nil {
			respond(rejectedText(err.Error()))
			return true
		}
	} else if len(options.BareValues()) != 0 {
		respond(rejectedText("Common-option inventory is missing. Use an explicit key such as resource-group=value or retry later."))
		return true
	}
	userOptions, err := userOptions(options)
	if err != nil {
		respond(rejectedText(err.Error()))
		return true
	}
	authRequested := options.AuthRequested()
	allocations, err := b.ownerAllocations(ctx, message.User)
	if err != nil {
		b.logf("list allocations for admission: %v", err)
		respond(rejectedText("Unable to check your allocation limit. No operation was started."))
		return false
	}
	if b.countedAllocations(allocations) >= b.maxAllocationsPerUser() {
		respond(rejectedText(fmt.Sprintf("You have reached your allocation limit of %d. Wait for cleanup to complete, then create a new allocation.", b.maxAllocationsPerUser())))
		return true
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.Namespace}, Spec: servitorv1alpha1.ServitorClusterSpec{
		Slack:       servitorv1alpha1.SlackIdentity{OwnerID: message.User, ChannelID: message.Channel, ThreadTimestamp: message.Timestamp},
		UserOptions: userOptions,
		Lifecycle:   servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: int64(b.lease() / time.Second), RetrySeconds: seconds(b.RetryIntervals)},
	}}
	publicAuthEligible := b.publicAuthEligible(cluster)
	if authRequested && publicAuthEligible {
		if _, _, valid := splitSlackTimestamp(message.Timestamp); !valid {
			respond(rejectedText("Unable to record the authentication request."))
			return true
		}
		cluster.Spec.Lifecycle.AuthRequestTimestamp = message.Timestamp
	}
	// Delivery precedes CR creation. A controller can therefore never begin
	// planning an allocation the user was not told was accepted.
	if b.Responder == nil {
		b.logf("deliver create acceptance: responder is not configured")
		return false
	}
	if err := b.Responder.Reply(ctx, Response{Channel: message.Channel, ThreadTimestamp: message.Timestamp, Text: "Planning..."}); err != nil {
		b.logf("deliver create acceptance: %v", err)
		return false
	}
	if err := b.Client.Create(ctx, cluster); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if getErr := b.allocationReader().Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, existing); getErr == nil {
				respond(b.existingAllocationNotice(ctx, existing, message.User))
				return true
			}
		}
		b.logf("create allocation: %v", err)
		respond(rejectedText("Unable to record the create request. No operation was started."))
		return false
	}
	if authRequested && !publicAuthEligible {
		respond("VPN-backed authentication is not implemented yet")
	}
	return true
}

type reviewDecisionOutcome string

const (
	reviewDecisionRecorded reviewDecisionOutcome = "recorded"
	reviewDecisionExpired  reviewDecisionOutcome = "expired"
	reviewDecisionInvalid  reviewDecisionOutcome = "invalid"
	reviewDecisionStale    reviewDecisionOutcome = "stale"
)

func (b Bot) confirm(ctx context.Context, message Message, thread string) {
	cluster, err := b.ownerCluster(ctx, message, thread)
	if err != nil || !ownsThread(cluster, message, thread, true) {
		return
	}
	approval := "approved"
	if message.Text == "no" {
		approval = "rejected"
	}
	outcome, err := b.recordReviewDecision(ctx, cluster.Name, message, thread, approval)
	if err != nil {
		b.logf("record review decision: %v", err)
		return
	}
	switch outcome {
	case reviewDecisionRecorded:
		if approval == "rejected" {
			b.respondCleanupCause(ctx, message.Channel, thread, cluster, servitorv1alpha1.CleanupReasonRejected, "Plan rejected.\nCleaning up...")
			return
		}
		b.respond(ctx, message.Channel, thread, "Plan approved, creating...\nThis may take 30m-90m.")
	case reviewDecisionExpired:
		b.respond(ctx, message.Channel, thread, "The review deadline has passed. No decision was recorded; cleanup will begin.")
	case reviewDecisionInvalid:
		b.respond(ctx, message.Channel, thread, "This plan is no longer awaiting review. No decision was recorded.")
	case reviewDecisionStale:
		b.respond(ctx, message.Channel, thread, "A review decision has already been recorded. No decision was recorded.")
	}
}

func (b Bot) recordReviewDecision(ctx context.Context, name string, message Message, thread, approval string) (reviewDecisionOutcome, error) {
	outcome := reviewDecisionInvalid
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := b.allocationReader().Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, current); err != nil {
			return err
		}
		switch {
		case !ownsThread(current, message, thread, true), current.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval, current.Status.ReviewDeadline == nil:
			outcome = reviewDecisionInvalid
			return nil
		case !b.now().Before(current.Status.ReviewDeadline.Time):
			outcome = reviewDecisionExpired
			return nil
		case current.Spec.Lifecycle.Approval != "":
			outcome = reviewDecisionStale
			return nil
		}
		current.Spec.Lifecycle.Approval = approval
		outcome = reviewDecisionRecorded
		return b.Client.Update(ctx, current)
	})
	return outcome, err
}

func (b Bot) cleanup(ctx context.Context, message Message, thread string, acknowledge bool) {
	if message.ThreadTimestamp == "" {
		b.cleanupAll(ctx, message, thread, acknowledge)
		return
	}
	b.cleanupThread(ctx, message, thread, acknowledge)
}

func (b Bot) cleanupThread(ctx context.Context, message Message, thread string, acknowledge bool) {
	cluster, err := b.ownerCluster(ctx, message, thread)
	if err != nil {
		return
	}
	if !ownsThread(cluster, message, thread, true) {
		return
	}
	stateText := ""
	updated, err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) (bool, error) {
		if !ownsThread(current, message, thread, true) {
			return false, nil
		}
		if text := cleanupUnavailableText(current); text != "" {
			stateText = text
			return false, nil
		}
		current.Spec.Lifecycle.CleanupRequested = true
		return true, nil
	})
	if err != nil {
		b.logf("request cleanup: %v", err)
		if acknowledge {
			if apierrors.IsNotFound(err) {
				b.respond(ctx, message.Channel, thread, lifecycleLookupText(err))
			} else {
				b.respond(ctx, message.Channel, thread, rejectedText("Unable to record the cleanup request. No cleanup was started."))
			}
		}
		return
	}
	if !updated {
		if acknowledge && stateText != "" {
			b.respond(ctx, message.Channel, thread, stateText)
		}
		return
	}
	if acknowledge {
		b.respondCleanupCause(ctx, message.Channel, thread, cluster, servitorv1alpha1.CleanupReasonExplicit, "Cleaning up...")
	}
}

func (b Bot) cleanupAll(ctx context.Context, message Message, thread string, acknowledge bool) {
	clusters, err := b.ownerAllocations(ctx, message.User)
	if err != nil {
		b.logf("list cleanup allocations: %v", err)
		if acknowledge {
			b.respond(ctx, message.Channel, thread, rejectedText("Unable to list your allocations. No cleanup was started."))
		}
		return
	}
	selected := make([]servitorv1alpha1.ServitorCluster, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster.Spec.Slack.ChannelID == message.Channel {
			selected = append(selected, cluster)
		}
	}
	if len(selected) == 0 {
		if acknowledge {
			b.respond(ctx, message.Channel, thread, "You have no allocations in this channel. Use @servitor create to start one.")
		}
		return
	}

	summary := cleanupSummary{}
	for _, cluster := range selected {
		stateText := ""
		updated, err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) (bool, error) {
			if !ownsThread(current, message, thread, false) {
				stateText = "unavailable"
				return false, nil
			}
			if text := cleanupUnavailableText(current); text != "" {
				stateText = text
				return false, nil
			}
			current.Spec.Lifecycle.CleanupRequested = true
			return true, nil
		})
		if err != nil {
			b.logf("request cleanup for %s: %v", cluster.Name, err)
			summary.failed++
			continue
		}
		if !updated {
			summary.recordUnavailable(stateText)
			continue
		}
		summary.requested++
		if acknowledge {
			b.respondCleanupCause(ctx, message.Channel, cluster.Spec.Slack.ThreadTimestamp, &cluster, servitorv1alpha1.CleanupReasonExplicit, "Cleaning up...")
		}
	}
	if acknowledge {
		b.respond(ctx, message.Channel, thread, summary.text())
	}
}

type cleanupSummary struct{ requested, alreadyCleaning, completed, unresolved, unavailable, failed int }

func (s *cleanupSummary) recordUnavailable(text string) {
	switch text {
	case "Cleanup is already in progress. No further action is needed.":
		s.alreadyCleaning++
	case "Cleanup is complete. Use @servitor create to start a new allocation.":
		s.completed++
	case "Cleanup is unresolved. An administrator must inspect the allocation CR status and private cluster logs.":
		s.unresolved++
	default:
		s.unavailable++
	}
}

func (s cleanupSummary) text() string {
	parts := []string{fmt.Sprintf("Cleanup requested for %d allocation(s).", s.requested)}
	if s.alreadyCleaning > 0 {
		parts = append(parts, fmt.Sprintf("Already cleaning: %d allocation(s).", s.alreadyCleaning))
	}
	if s.completed > 0 {
		parts = append(parts, fmt.Sprintf("Cleanup complete: %d allocation(s).", s.completed))
	}
	if s.unresolved > 0 {
		parts = append(parts, fmt.Sprintf("Cleanup unresolved: %d allocation(s).", s.unresolved))
	}
	if s.unavailable > 0 {
		parts = append(parts, fmt.Sprintf("Skipped or unavailable: %d allocation(s).", s.unavailable))
	}
	if s.failed > 0 {
		parts = append(parts, fmt.Sprintf("Failed to record cleanup for %d allocation(s).", s.failed))
	}
	return strings.Join(parts, "\n")
}

// auth records one owner-thread request. The controller consumes it before
// reading the allocation Secret or calling Slack, so intake never handles credentials.
func (b Bot) auth(ctx context.Context, message Message, thread string, respond func(string)) {
	cluster, err := b.ownerCluster(ctx, message, thread)
	if err != nil || !ownsThread(cluster, message, thread, true) {
		return
	}
	if !b.publicAuthEligible(cluster) {
		respond("VPN-backed authentication is not implemented yet")
		return
	}
	if _, _, valid := splitSlackTimestamp(message.Timestamp); !valid {
		respond(rejectedText("Unable to record the authentication request."))
		return
	}
	stateText := ""
	updated, err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) (bool, error) {
		if !ownsThread(current, message, thread, true) {
			return false, nil
		}
		if !b.publicAuthEligible(current) {
			stateText = "VPN-backed authentication is not implemented yet"
			return false, nil
		}
		if staleAuthEvent(current.Spec.Lifecycle.AuthRequestTimestamp, message.Timestamp) {
			stateText = "This authentication request was already recorded. Send a new `auth` request to resend the stored file."
			return false, nil
		}
		current.Spec.Lifecycle.AuthRequestTimestamp = message.Timestamp
		return true, nil
	})
	if err != nil {
		b.logf("request authentication delivery: %v", err)
		if apierrors.IsNotFound(err) {
			respond(lifecycleLookupText(err))
		} else {
			respond(rejectedText("Unable to record the authentication request."))
		}
		return
	}
	if updated {
		respond("Authentication delivery has been queued for your DM.")
	} else if stateText != "" {
		respond(stateText)
	}
}

func (b Bot) publicAuthEligible(cluster *servitorv1alpha1.ServitorCluster) bool {
	if cluster.Status.LifecycleSnapshot != nil {
		return cluster.Status.LifecycleSnapshot.PublicAuthEligible
	}
	provider := cluster.Spec.UserOptions.Provider
	if provider == "" {
		provider = b.Defaults.Provider
	}
	if provider == "satellite" {
		return false
	}
	target := cluster.Spec.UserOptions.Target
	if target == "" {
		target = b.Defaults.Target
	}
	return slices.Contains(b.PublicAuthTargets, target)
}

func (b Bot) extend(ctx context.Context, message Message, thread string, respond func(string)) {
	cluster, err := b.ownerCluster(ctx, message, thread)
	if err != nil {
		return
	}
	if !ownsThread(cluster, message, thread, true) {
		return
	}
	increment, err := command.ParseExtend(message.Text)
	if err != nil {
		respond(rejectedText(err.Error()))
		return
	}
	if text := extensionUnavailableText(cluster, b.now()); text != "" {
		respond(text)
		return
	}
	stateText := ""
	updated, err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) (bool, error) {
		if !ownsThread(current, message, thread, true) {
			return false, nil
		}
		if text := extensionUnavailableText(current, b.now()); text != "" {
			stateText = text
			return false, nil
		}
		if staleExtensionEvent(current.Spec.Lifecycle.ExtensionEventTimestamp, message.Timestamp) {
			stateText = "This extension was already recorded. Wait for the controller outcome."
			return false, nil
		}
		target, err := command.ExtensionTarget(current.Status.LeaseExpiresAt.Time, increment, time.Duration(current.Status.LifecycleSnapshot.InitialLeaseSeconds)*time.Second)
		if err != nil {
			return false, err
		}
		current.Spec.Lifecycle.RequestedExpiry = &metav1.Time{Time: target}
		current.Spec.Lifecycle.ExtensionEventTimestamp = message.Timestamp
		return true, nil
	})
	if err != nil {
		b.logf("request lease extension: %v", err)
		if apierrors.IsNotFound(err) {
			respond(lifecycleLookupText(err))
		} else {
			respond(rejectedText("Unable to record the lease extension."))
		}
		return
	}
	if !updated && stateText != "" {
		respond(stateText)
	}
}

func lifecycleLookupText(err error) string {
	if apierrors.IsNotFound(err) {
		return "You have no allocation. Use @servitor create to start one."
	}
	return rejectedText("Unable to check your allocation state. Try again.")
}

func cleanupUnavailableText(cluster *servitorv1alpha1.ServitorCluster) string {
	switch {
	case cluster.Status.Phase == servitorv1alpha1.PhaseCleanupComplete:
		return "Cleanup is complete. Use @servitor create to start a new allocation."
	case cluster.Status.Phase == servitorv1alpha1.PhaseUnresolved:
		return "Cleanup is unresolved. An administrator must inspect the allocation CR status and private cluster logs."
	case cluster.Spec.Lifecycle.CleanupRequested || cluster.Status.Cleanup != nil || cluster.Status.Phase == servitorv1alpha1.PhaseCleanupPending:
		return "Cleanup is already in progress. No further action is needed."
	default:
		return ""
	}
}

func extensionUnavailableText(cluster *servitorv1alpha1.ServitorCluster, now time.Time) string {
	if cluster.Spec.Lifecycle.CleanupRequested || cluster.Status.Cleanup != nil || cluster.Status.Phase == servitorv1alpha1.PhaseCleanupPending {
		return "Cleanup is in progress. The lease cannot be extended."
	}
	switch cluster.Status.Phase {
	case servitorv1alpha1.PhasePending, servitorv1alpha1.PhasePlanning:
		return "Planning is in progress. Extend is available when your cluster is ready."
	case servitorv1alpha1.PhaseAwaitingApproval:
		return "This plan is awaiting review. Reply yes or no in this thread."
	case servitorv1alpha1.PhaseApplying:
		return "Your cluster is being created. Extend is available when your cluster is ready."
	case servitorv1alpha1.PhaseCleanupComplete:
		return "Cleanup is complete. Use @servitor create to start a new allocation."
	case servitorv1alpha1.PhaseUnresolved:
		return "This allocation is unresolved. An administrator must inspect the allocation CR status and private cluster logs."
	case servitorv1alpha1.PhaseReady:
		if cluster.Status.LeaseExpiresAt == nil || cluster.Status.LifecycleSnapshot == nil {
			return "The ready lease state is unavailable. Try again when status is updated."
		}
		if !now.Before(cluster.Status.LeaseExpiresAt.Time) {
			return "Your lease has expired. Cleanup will begin; use @servitor create after cleanup completes."
		}
		return ""
	default:
		return "This allocation is not ready to extend. Extend is available when your cluster is ready."
	}
}

func (b Bot) list(ctx context.Context, user string, respond func(string)) {
	if b.Client == nil || b.Namespace == "" {
		respond(rejectedText("List is unavailable. Inspect the allocation CR status and private cluster logs."))
		return
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := b.allocationReader().List(ctx, &clusters, client.InNamespace(b.Namespace)); err != nil {
		b.logf("list allocations: %v", err)
		respond(rejectedText("Unable to list clusters. No operation was started."))
		return
	}
	for _, text := range clusterListMessages(clusters.Items, user, b.now()) {
		respond(text)
	}
}

func (b Bot) ownerCluster(ctx context.Context, message Message, thread string) (*servitorv1alpha1.ServitorCluster, error) {
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := b.allocationReader().Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: allocationClusterName(message.Channel, thread)}, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

// ownerAllocations returns only allocations with the exact persisted owner in
// the configured namespace. Callers apply any channel or thread scope.
func (b Bot) ownerAllocations(ctx context.Context, owner string) ([]servitorv1alpha1.ServitorCluster, error) {
	if b.Namespace == "" {
		return nil, fmt.Errorf("allocation namespace is not configured")
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := b.allocationReader().List(ctx, &clusters, client.InNamespace(b.Namespace)); err != nil {
		return nil, err
	}
	allocations := make([]servitorv1alpha1.ServitorCluster, 0, len(clusters.Items))
	for _, cluster := range clusters.Items {
		if cluster.Spec.Slack.OwnerID == owner {
			allocations = append(allocations, cluster)
		}
	}
	return allocations, nil
}

type allocationReaderProvider interface {
	AllocationReader() client.Reader
}

func (b Bot) allocationReader() client.Reader {
	if provider, ok := b.Client.(allocationReaderProvider); ok && provider.AllocationReader() != nil {
		return provider.AllocationReader()
	}
	return b.Client
}

func (b Bot) updateIntent(ctx context.Context, name string, mutate func(*servitorv1alpha1.ServitorCluster) (bool, error)) (bool, error) {
	updated := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := b.allocationReader().Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, current); err != nil {
			return err
		}
		changed, err := mutate(current)
		if err != nil {
			return err
		}
		if !changed {
			updated = false
			return nil
		}
		if err := b.Client.Update(ctx, current); err != nil {
			return err
		}
		updated = true
		return nil
	})
	return updated, err
}
func ownsThread(cluster *servitorv1alpha1.ServitorCluster, message Message, thread string, requireThread bool) bool {
	if cluster == nil || cluster.Spec.Slack.OwnerID != message.User || cluster.Spec.Slack.ChannelID != message.Channel {
		return false
	}
	return !requireThread || cluster.Spec.Slack.ThreadTimestamp == thread
}

// allocationClusterName identifies one initiating Slack channel/thread pair.
// A NUL separator makes pairs such as ("ab", "c") and ("a", "bc") distinct.
func allocationClusterName(channel, thread string) string {
	digest := sha256.Sum256([]byte(channel + "\x00" + thread))
	return "slack-" + hex.EncodeToString(digest[:])[:24]
}

func (b Bot) countedAllocations(allocations []servitorv1alpha1.ServitorCluster) int {
	count := 0
	for _, allocation := range allocations {
		if allocation.Status.Phase != servitorv1alpha1.PhaseCleanupComplete {
			count++
		}
	}
	return count
}

func (b Bot) maxAllocationsPerUser() int {
	if b.MaxAllocationsPerUser > 0 {
		return b.MaxAllocationsPerUser
	}
	return config.DefaultMaxAllocationsPerUser
}

// staleExtensionEvent rejects a redelivery even after the bounded receipt
// cache evicts its ID. Slack message timestamps are monotonically increasing
// for a conversation and the latest accepted value is durable CR spec intent.
func staleAuthEvent(last, current string) bool { return staleExtensionEvent(last, current) }

func staleExtensionEvent(last, current string) bool {
	if last == "" {
		return false
	}
	if last == current {
		return true
	}
	comparison, comparable := compareSlackTimestamps(current, last)
	return comparable && comparison <= 0
}

func compareSlackTimestamps(left, right string) (int, bool) {
	leftWhole, leftFraction, leftOK := splitSlackTimestamp(left)
	rightWhole, rightFraction, rightOK := splitSlackTimestamp(right)
	if !leftOK || !rightOK {
		return 0, false
	}
	if len(leftWhole) != len(rightWhole) {
		if len(leftWhole) < len(rightWhole) {
			return -1, true
		}
		return 1, true
	}
	if leftWhole != rightWhole {
		if leftWhole < rightWhole {
			return -1, true
		}
		return 1, true
	}
	length := len(leftFraction)
	if len(rightFraction) > length {
		length = len(rightFraction)
	}
	leftFraction += strings.Repeat("0", length-len(leftFraction))
	rightFraction += strings.Repeat("0", length-len(rightFraction))
	if leftFraction < rightFraction {
		return -1, true
	}
	if leftFraction > rightFraction {
		return 1, true
	}
	return 0, true
}

func splitSlackTimestamp(value string) (string, string, bool) {
	whole, fraction, hasFraction := strings.Cut(value, ".")
	if !hasFraction {
		fraction = ""
	}
	if whole == "" || strings.Contains(fraction, ".") {
		return "", "", false
	}
	for _, character := range whole + fraction {
		if character < '0' || character > '9' {
			return "", "", false
		}
	}
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	return whole, strings.TrimRight(fraction, "0"), true
}
func (b Bot) matchBareOptions(ctx context.Context, options command.ExplicitCreateOptions) (command.ExplicitCreateOptions, error) {
	if b.Client == nil || b.Namespace == "" || b.InventoryConfigKey == "" {
		return command.ExplicitCreateOptions{}, fmt.Errorf("common-option inventory is missing; use an explicit key such as resource-group=value or retry later")
	}
	configMap := &corev1.ConfigMap{}
	if err := b.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: b.InventoryConfigMap}, configMap); err != nil {
		return command.ExplicitCreateOptions{}, fmt.Errorf("configured target inventory is unavailable; use an explicit key or retry later")
	}
	configured, err := inventory.LoadConfig([]byte(configMap.Data[b.InventoryConfigKey]))
	if err != nil {
		return command.ExplicitCreateOptions{}, fmt.Errorf("configured target inventory is unavailable; use an explicit key or retry later")
	}
	options, targetConfig, err := command.ResolveBareSelectors(options, b.Defaults, configured.Targets)
	if err != nil {
		return command.ExplicitCreateOptions{}, err
	}
	if len(options.BareValues()) == 0 {
		return options, nil
	}
	revision, err := inventory.Revision(targetConfig)
	if err != nil {
		return command.ExplicitCreateOptions{}, fmt.Errorf("configured target inventory is unavailable; use an explicit key or retry later")
	}
	catalog, disposition, err := state.NewInventoryStore(b.Client, b.Namespace).Snapshot(ctx, selectedTarget(options, b.Defaults), revision, b.now(), b.inventoryMaximumAge())
	if err != nil {
		return command.ExplicitCreateOptions{}, fmt.Errorf("configured target inventory is unavailable; use an explicit key or retry later")
	}
	if disposition != state.InventorySucceeded {
		return command.ExplicitCreateOptions{}, fmt.Errorf("common-option inventory is %s; use an explicit key such as resource-group=value or retry later", disposition)
	}
	return command.MatchBareCreateOptions(options, b.Defaults, catalog)
}

func selectedTarget(options command.ExplicitCreateOptions, defaults command.CreateDefaults) string {
	if values := options.Values()["--target"]; len(values) != 0 {
		return values[0]
	}
	return defaults.Target
}

func (b Bot) inventoryMaximumAge() time.Duration {
	if b.InventoryMaximumAge > 0 {
		return b.InventoryMaximumAge
	}
	return 24 * time.Hour
}

func userOptions(options command.ExplicitCreateOptions) (servitorv1alpha1.UserOptions, error) {
	values := options.Values()
	one := func(flag string) string {
		if len(values[flag]) == 0 {
			return ""
		}
		return values[flag][0]
	}
	workerCount, err := options.WorkerCount()
	if err != nil {
		return servitorv1alpha1.UserOptions{}, err
	}
	return servitorv1alpha1.UserOptions{Target: one("--target"), Provider: one("--provider"), Version: one("--version"), ResourceGroup: one("--resource-group"), Zone: one("--zone"), Flavor: one("--flavor"), VPCID: one("--vpc-id"), Datacenter: one("--datacenter"), MachineType: one("--machine-type"), PublicVLANID: one("--public-vlan-id"), PrivateVLANID: one("--private-vlan-id"), SubnetIDs: append([]string(nil), values["--subnet-id"]...), PublicGatewayIDs: append([]string(nil), values["--public-gateway-id"]...), SatelliteZones: append([]string(nil), values["--satellite-zone"]...), SatelliteManagedFrom: one("--satellite-managed-from"), SatelliteLocationID: one("--satellite-location-id"), SatelliteHostImage: one("--satellite-host-image"), SatelliteHostProfile: one("--satellite-host-profile"), SatelliteSSHKeyID: one("--satellite-ssh-key-id"), SatelliteWorkerInstanceIDs: append([]string(nil), values["--satellite-worker-instance-id"]...), SatelliteWorkerOperatingSystem: one("--satellite-worker-operating-system"), WorkerCount: workerCount}, nil
}
func seconds(values []time.Duration) []int64 {
	result := make([]int64, len(values))
	for i, value := range values {
		result[i] = int64(value / time.Second)
	}
	return result
}
func (b Bot) lease() time.Duration {
	if b.Lease > 0 {
		return b.Lease
	}
	return 4 * time.Hour
}
func (b Bot) now() time.Time {
	if b.Clock != nil {
		return b.Clock().UTC()
	}
	return time.Now().UTC()
}
func (b Bot) respond(ctx context.Context, channel, thread, text string) bool {
	if b.Responder == nil {
		return false
	}
	if err := b.Responder.Reply(ctx, Response{Channel: channel, ThreadTimestamp: thread, Text: text}); err != nil {
		b.logf("deliver Slack reply: %v", err)
		return false
	}
	return true
}

func (b Bot) respondCleanupCause(ctx context.Context, channel, thread string, cluster *servitorv1alpha1.ServitorCluster, reason servitorv1alpha1.CleanupReason, text string) {
	if b.Events == nil {
		b.respond(ctx, channel, thread, text)
		return
	}
	id := cleanupCauseNoticeID(clusterNoticeUID(cluster), reason)
	claimed, err := b.Events.Claim(ctx, id)
	if err != nil {
		b.logf("claim cleanup cause notice: %v", err)
		return
	}
	if !claimed {
		return
	}
	if b.respond(ctx, channel, thread, text) {
		if err := b.Events.MarkDelivered(ctx, id); err != nil {
			b.logf("mark cleanup cause delivery: %v", err)
		}
		return
	}
	if err := b.Events.Release(ctx, id); err != nil {
		b.logf("release cleanup cause notice: %v", err)
	}
}
func (b Bot) respondHelp(text string, respond func(string), maintainer bool) {
	words := strings.Fields(text)
	if len(words) > 2 {
		respond(b.unknownText())
		return
	}
	topic := ""
	if len(words) == 2 {
		topic = words[1]
	}
	var messages []string
	switch topic {
	case "":
		messages = helpOverview(maintainer, b.maxAllocationsPerUser())
	case "create":
		messages = createHelp(b.Defaults, b.maxAllocationsPerUser())
	case "done":
		messages = []string{"`done` in a lifecycle thread releases that allocation. `@servitor done` in the configured channel requests cleanup for all of your allocations in that channel. It is unavailable after cleanup completes; repeated requests report cleanup in progress."}
	case "extend":
		messages = []string{"`extend [N[h]]` extends your ready lease by the configured duration or by 1 through 24 whole hours. Use it only in your lifecycle thread. It is available only while the lease is ready, not during planning, cleanup, or after expiry."}
	case "auth":
		messages = []string{"`auth` sends the stored public kubeconfig to the allocation owner's DM. Use exact `auth` only in the initiating lifecycle thread. Requests made before Ready are queued; a failed or interrupted delivery is not retried automatically, so send a newer `auth` request to resend the stored file. Private-only and Satellite authentication is not implemented yet."}
	case "list":
		messages = []string{"`list` shows only your allocations in a table with cluster, state, location, and expires columns. Cleanup in progress and cleanup complete are shown separately. Use it in a DM or as `@servitor list` in the configured channel."}
	case "refresh":
		if maintainer {
			messages = []string{"`refresh inventory` starts or joins a private inventory refresh. Use it only in a DM; a safe completion summary follows."}
		} else {
			respond(b.unknownText())
			return
		}
	default:
		respond(b.unknownText())
		return
	}
	for _, message := range messages {
		for _, chunk := range boundedMessages(message) {
			respond(chunk)
		}
	}
}
func (b Bot) existingAllocationNotice(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, owner string) string {
	expiry := time.Time{}
	if cluster.Status.LeaseExpiresAt != nil {
		expiry = cluster.Status.LeaseExpiresAt.Time
	}
	text := "You already have an allocation in state " + listStatusCell(cluster.Status.Phase) + ". Lease: " + lifecycle.FormatLeaseExpiry(expiry, b.now()) + "."
	if cluster.Spec.Slack.OwnerID != owner || b.Permalinks == nil || cluster.Spec.Slack.ChannelID == "" || cluster.Spec.Slack.ThreadTimestamp == "" {
		return text + " Open the original allocation thread in the configured channel."
	}
	permalink, err := b.Permalinks.Permalink(ctx, cluster.Spec.Slack.ChannelID, cluster.Spec.Slack.ThreadTimestamp)
	if err != nil || !safePermalink(permalink) {
		return text + " Open the original allocation thread in the configured channel."
	}
	return text + " <" + permalink + "|Open allocation thread>."
}

func safePermalink(value string) bool {
	if len(value) == 0 || len(value) > 2048 || strings.ContainsAny(value, "<>|\r\n") {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

func channelOnlyText(channel string) string {
	if len(channel) > 0 && len(channel) <= 64 {
		for _, character := range channel {
			if !(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') {
				return "That command is available only in the configured channel."
			}
		}
		return "That command is available only in <#" + channel + ">."
	}
	return "That command is available only in the configured channel."
}
func channelCommand(text, self string) (string, bool) {
	if self == "" {
		return "", false
	}
	text = trimASCIIWhitespace(text)
	mention := "<@" + self + ">"
	if !strings.HasPrefix(text, mention) {
		return "", false
	}
	rest := strings.TrimPrefix(text, mention)
	if rest != "" && !isASCIIWhitespace(rune(rest[0])) {
		return "", false
	}
	return trimASCIIWhitespace(rest), true
}
func trimASCIIWhitespace(value string) string { return strings.TrimFunc(value, isASCIIWhitespace) }
func isASCIIWhitespace(character rune) bool {
	return character == ' ' || character == '\t' || character == '\n' || character == '\r' || character == '\f' || character == '\v'
}
func firstToken(text string) string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}
	return words[0]
}
func (b Bot) unknownText() string {
	return "Command unknown.\n\n" + helpOverview(false, b.maxAllocationsPerUser())[0]
}
func rejectedText(reason string) string { return "Command rejected.\n\n" + reason }
func helpOverview(maintainer bool, maxAllocationsPerUser int) []string {
	text := fmt.Sprintf("Servitor provisions up to %d temporary IBM Cloud clusters per Slack user, one per initiating lifecycle thread.\n\nCommands\n```\nDM\n  help [command]          print help\n  list                    list clusters\n\nConfigured channel\n  @servitor help [command]  print help\n  @servitor create [safe options]  provision a new cluster\n  @servitor done            request cleanup for all your allocations in this channel\n  @servitor list            list clusters\n\nLifecycle thread\n  yes                       approve the cluster plan\n  no                        reject the cluster plan\n  done                      release this allocation\n  extend [N[h]]             extend your lease\n  auth                      send stored public access to your DM\n", maxAllocationsPerUser)
	if maintainer {
		text += "\nMaintainer DM\n  refresh inventory         refresh private inventory\n"
	}
	return []string{text + "```"}
}
func createHelp(defaults command.CreateDefaults, maxAllocationsPerUser int) []string {
	intro := fmt.Sprintf("`create` starts planning from the configured channel root, up to %d active allocations per user. Repeating create in the same thread preserves that allocation. Configured defaults: version %s, target %s, provider %s.", maxAllocationsPerUser, safeHelpCell(defaults.Version), safeHelpCell(defaults.Target), safeHelpCell(defaults.Provider))
	return []string{intro + " `provider=value` chooses infrastructure; version chooses Kubernetes or OpenShift. Cluster names are generated internally.\n\nSpecify provisioning options as `key=value`. A current common-option inventory also recognizes unique bare values, for example `create target=synthetic-target vpc-gen2 us-south-1 bx2.4x16 resource-group=\"Platform Team\"`. The environment target synonyms `prestage`/`pretest` and `test`/`stage` are interchangeable; `dev` selects the configured dev target. `auth`, `auth=true`, and `auth=false` control only public kubeconfig delivery; the default is no delivery. Public `auth` queues one owner-DM delivery after Ready. Private-only and Satellite auth opt-ins continue creating the cluster but report that VPN-backed authentication is not implemented yet. Unknown or colliding shorthand must use a key such as `flavor=value`; worker counts and uncommon Satellite values stay keyed. Use `roks` (also `openshift` or `default_openshift`) for the cloud default OpenShift stream, or `iks` (also `kubernetes`, `k8s`, or `default_kubernetes`) for Kubernetes. A compatible numeric stream can refine an alias: `create roks 4.17`. These reserved aliases require a key when used as resource names.\n\nReview the resolved stream and configuration in its thread. The review prompt shows the persisted approval deadline; reply with exact `yes` or `no` before that deadline.", "Safe create options\n```\nCommon\n  target= provider= version= resource-group= worker-count=\n  auth | auth=true | auth=false (public delivery only)\n\nVPC Gen 2\n  zone= flavor= vpc-id= subnet-id= public-gateway-id=\n\nClassic\n  datacenter= machine-type= public-vlan-id= private-vlan-id=\n\nSatellite\n  satellite-zone= satellite-managed-from= satellite-location-id=\n  satellite-host-image= satellite-host-profile= satellite-ssh-key-id=\n  satellite-worker-instance-id= satellite-worker-operating-system=\n```"}
}
func safeHelpCell(value string) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) || character == '`' || character == '<' || character == '>' || character == '@' {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "-"
	}
	if len([]rune(value)) > 160 {
		return string([]rune(value)[:157]) + "..."
	}
	return value
}
func boundedMessages(text string) []string {
	if len(text) <= maxSlackMessage {
		return []string{text}
	}
	lines := strings.SplitAfter(text, "\n")
	chunks := make([]string, 0, len(lines)/2+1)
	var current strings.Builder
	inFence := false
	flush := func() {
		if current.Len() == 0 {
			return
		}
		if inFence {
			current.WriteString("```\n")
		}
		chunks = append(chunks, current.String())
		current.Reset()
		if inFence {
			current.WriteString("```\n")
		}
	}
	for _, line := range lines {
		if len(line) > maxSlackMessage-8 {
			flush()
			for len(line) > maxSlackMessage-8 {
				chunk, rest := utf8Prefix(line, maxSlackMessage-8)
				if inFence {
					chunks = append(chunks, "```\n"+chunk+"\n```")
				} else {
					chunks = append(chunks, chunk)
				}
				line = rest
			}
		}
		lineFence := strings.Count(line, "```")%2 != 0
		reserve := 0
		if inFence != lineFence {
			reserve = 4
		}
		if current.Len() > 0 && current.Len()+len(line)+reserve > maxSlackMessage {
			flush()
		}
		current.WriteString(line)
		if lineFence {
			inFence = !inFence
		}
	}
	if current.Len() > 0 {
		if inFence {
			current.WriteString("```\n")
		}
		chunks = append(chunks, current.String())
	}
	return chunks
}
func utf8Prefix(value string, limit int) (string, string) {
	end := 0
	for _, character := range value {
		size := utf8.RuneLen(character)
		if end+size > limit {
			break
		}
		end += size
	}
	return value[:end], value[end:]
}
func (b Bot) logf(format string, args ...any) {
	if b.Logf != nil {
		b.Logf(format, args...)
		return
	}
	log.Printf("slackbot: "+format, args...)
}
