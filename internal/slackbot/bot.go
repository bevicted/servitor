// Package slackbot implements the Slack command boundary.
package slackbot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
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
	Seen(context.Context, string) (bool, error)
}
type Responder interface {
	Reply(context.Context, Response) error
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
	Events                EventStore
	Defaults              command.CreateDefaults
	InventoryConfigMap    string
	InventoryConfigKey    string
	InventoryMaximumAge   time.Duration
	Lease                 time.Duration
	RetryIntervals        []time.Duration
	Responder             Responder
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
		b.handleDM(ctx, message)
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
		default:
			if firstToken(message.Text) == "extend" {
				b.extend(ctx, message, thread, true, reply)
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
		b.respondHelp(text, reply)
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
	case "extend":
		b.extend(ctx, message, thread, false, reply)
	default:
		reply(unknownText())
	}
	return claim()
}

func (b Bot) handleDM(ctx context.Context, message Message) {
	respond := func(text string) { b.respond(ctx, message.Channel, "", text) }
	switch firstToken(message.Text) {
	case "help":
		b.respondHelp(message.Text, respond)
	case "list":
		b.list(ctx, message.User, respond)
	case "create", "done", "destroy", "extend":
		respond(rejectedText("That command is available only in the configured channel."))
	default:
		respond(unknownText())
	}
}

func (b Bot) create(ctx context.Context, message Message, text string, respond func(string)) bool {
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
	if b.Client == nil || b.Namespace == "" {
		respond(rejectedText("Create is unavailable. Inspect the allocation CR status and private cluster logs."))
		return true
	}
	name := ownerClusterName(message.User)
	existing := &servitorv1alpha1.ServitorCluster{}
	err = b.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, existing)
	if err == nil {
		respond(existingAllocationNotice(existing, b.now()))
		return true
	}
	if !apierrors.IsNotFound(err) {
		b.logf("get existing allocation: %v", err)
		respond(rejectedText("Unable to check an existing allocation. No operation was started."))
		return false
	}
	// Delivery precedes CR creation. A controller can therefore never begin
	// planning an allocation the user was not told was accepted.
	if b.Responder == nil {
		b.logf("deliver create acceptance: responder is not configured")
		return false
	}
	if err := b.Responder.Reply(ctx, Response{Channel: message.Channel, ThreadTimestamp: message.Timestamp, Text: "Command accepted.\nPlanning..."}); err != nil {
		b.logf("deliver create acceptance: %v", err)
		return false
	}
	now := b.now()
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.Namespace}, Spec: servitorv1alpha1.ServitorClusterSpec{
		Slack:       servitorv1alpha1.SlackIdentity{OwnerID: message.User, ChannelID: message.Channel, ThreadTimestamp: message.Timestamp},
		UserOptions: userOptions,
		Lifecycle:   servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: int64(b.lease() / time.Second), RetrySeconds: seconds(b.RetryIntervals)},
	}}
	if err := b.Client.Create(ctx, cluster); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if getErr := b.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, existing); getErr == nil {
				respond(existingAllocationNotice(existing, now))
				return true
			}
		}
		b.logf("create allocation: %v", err)
		respond(rejectedText("Unable to record the create request. No operation was started."))
		return false
	}
	return true
}

func (b Bot) confirm(ctx context.Context, message Message, thread string) {
	cluster, err := b.ownerCluster(ctx, message.User)
	if err != nil || !ownsThread(cluster, message, thread, true) || cluster.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval {
		return
	}
	approval := "approved"
	if message.Text == "no" {
		approval = "rejected"
	}
	if err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) error {
		if !ownsThread(current, message, thread, true) || current.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval {
			return nil
		}
		current.Spec.Lifecycle.Approval = approval
		return nil
	}); err != nil {
		b.logf("record review decision: %v", err)
		return
	}
	response := "Plan approved.\nCreating... This may take 30m-90m."
	if approval == "rejected" {
		response = "Plan rejected.\nCleaning up..."
	}
	b.respond(ctx, message.Channel, thread, response)
}

func (b Bot) cleanup(ctx context.Context, message Message, thread string, acknowledge bool) {
	cluster, err := b.ownerCluster(ctx, message.User)
	if err != nil || !ownsThread(cluster, message, thread, message.ThreadTimestamp != "") {
		return
	}
	if err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) error {
		if !ownsThread(current, message, thread, message.ThreadTimestamp != "") {
			return nil
		}
		current.Spec.Lifecycle.CleanupRequested = true
		return nil
	}); err != nil {
		b.logf("request cleanup: %v", err)
		return
	}
	if acknowledge {
		b.respond(ctx, message.Channel, thread, "Command accepted.\nCleaning up...")
	}
}

func (b Bot) extend(ctx context.Context, message Message, thread string, requireThread bool, respond func(string)) {
	increment, err := command.ParseExtend(message.Text)
	if err != nil {
		respond(rejectedText(err.Error()))
		return
	}
	cluster, err := b.ownerCluster(ctx, message.User)
	if err != nil || !ownsThread(cluster, message, thread, requireThread) || cluster.Status.Phase != servitorv1alpha1.PhaseReady || cluster.Status.LeaseExpiresAt == nil || cluster.Status.LifecycleSnapshot == nil {
		return
	}
	target, err := command.ExtensionTarget(cluster.Status.LeaseExpiresAt.Time, increment, time.Duration(cluster.Status.LifecycleSnapshot.InitialLeaseSeconds)*time.Second)
	if err != nil {
		respond(rejectedText(err.Error()))
		return
	}
	if err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) error {
		if !ownsThread(current, message, thread, requireThread) || current.Status.Phase != servitorv1alpha1.PhaseReady || current.Status.LeaseExpiresAt == nil {
			return nil
		}
		if staleExtensionEvent(current.Spec.Lifecycle.ExtensionEventTimestamp, message.Timestamp) {
			return nil
		}
		current.Spec.Lifecycle.RequestedExpiry = &metav1.Time{Time: target}
		current.Spec.Lifecycle.ExtensionEventTimestamp = message.Timestamp
		return nil
	}); err != nil {
		b.logf("request lease extension: %v", err)
		respond(rejectedText("Unable to record the lease extension."))
	}
}

func (b Bot) list(ctx context.Context, user string, respond func(string)) {
	if b.Client == nil || b.Namespace == "" {
		respond(rejectedText("List is unavailable. Inspect the allocation CR status and private cluster logs."))
		return
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := b.Client.List(ctx, &clusters, client.InNamespace(b.Namespace)); err != nil {
		b.logf("list allocations: %v", err)
		respond(rejectedText("Unable to list clusters. No operation was started."))
		return
	}
	for _, text := range clusterListMessages(clusters.Items, user, b.now()) {
		respond(text)
	}
}

func (b Bot) ownerCluster(ctx context.Context, owner string) (*servitorv1alpha1.ServitorCluster, error) {
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := b.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: ownerClusterName(owner)}, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}
func (b Bot) updateIntent(ctx context.Context, name string, mutate func(*servitorv1alpha1.ServitorCluster) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := b.Client.Get(ctx, types.NamespacedName{Namespace: b.Namespace, Name: name}, current); err != nil {
			return err
		}
		if err := mutate(current); err != nil {
			return err
		}
		return b.Client.Update(ctx, current)
	})
}
func ownsThread(cluster *servitorv1alpha1.ServitorCluster, message Message, thread string, requireThread bool) bool {
	if cluster == nil || cluster.Spec.Slack.OwnerID != message.User || cluster.Spec.Slack.ChannelID != message.Channel {
		return false
	}
	return !requireThread || cluster.Spec.Slack.ThreadTimestamp == thread
}
func ownerClusterName(owner string) string {
	digest := sha256.Sum256([]byte(owner))
	return "slack-" + hex.EncodeToString(digest[:])[:24]
}

// staleExtensionEvent rejects a redelivery even after the bounded receipt
// cache evicts its ID. Slack message timestamps are monotonically increasing
// for a conversation and the latest accepted value is durable CR spec intent.
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
func (b Bot) respond(ctx context.Context, channel, thread, text string) {
	if b.Responder == nil {
		return
	}
	if err := b.Responder.Reply(ctx, Response{Channel: channel, ThreadTimestamp: thread, Text: text}); err != nil {
		b.logf("deliver Slack reply: %v", err)
	}
}
func (b Bot) respondHelp(text string, respond func(string)) {
	words := strings.Fields(text)
	if len(words) > 2 {
		respond(unknownText())
		return
	}
	topic := ""
	if len(words) == 2 {
		topic = words[1]
	}
	var messages []string
	switch topic {
	case "":
		messages = helpOverview()
	case "create":
		messages = createHelp(b.Defaults)
	case "done":
		messages = []string{"`done` releases your resources. Use it in your lifecycle thread, or as `@servitor done` in the configured channel."}
	case "extend":
		messages = []string{"`extend [N[h]]` extends your ready lease by the configured duration or by 1 through 24 whole hours. Use it in your lifecycle thread or as `@servitor extend` in the configured channel."}
	case "list":
		messages = []string{"`list` shows available cluster state. Use it in a DM or as `@servitor list` in the configured channel."}
	default:
		respond(unknownText())
		return
	}
	for _, message := range messages {
		for _, chunk := range boundedMessages(message) {
			respond(chunk)
		}
	}
}
func existingAllocationNotice(cluster *servitorv1alpha1.ServitorCluster, now time.Time) string {
	expiry := time.Time{}
	if cluster.Status.LeaseExpiresAt != nil {
		expiry = cluster.Status.LeaseExpiresAt.Time
	}
	return "You already have an allocation in state " + listStatusCell(cluster.Status.Phase) + ". Lease: " + lifecycle.FormatLeaseExpiry(expiry, now) + "."
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
func unknownText() string               { return "Command unknown.\n\n" + helpOverview()[0] }
func rejectedText(reason string) string { return "Command rejected.\n\n" + reason }
func helpOverview() []string {
	return []string{"Servitor provisions one temporary IBM Cloud cluster per Slack user.\n\nCommands\n```\nDM\n  help [command]          print help\n  list                    list clusters\n\nConfigured channel\n  @servitor help [command]  print help\n  @servitor create [safe options]  provision a new cluster\n  @servitor done            release your resources\n  @servitor extend [N[h]]   extend your lease\n  @servitor list            list clusters\n\nLifecycle thread\n  yes                       approve the cluster plan\n  no                        reject the cluster plan\n  done                      release your resources\n  extend [N[h]]             extend your lease\n```"}
}
func createHelp(defaults command.CreateDefaults) []string {
	return []string{"`create` starts planning from the configured channel root. Configured defaults: version " + safeHelpCell(defaults.Version) + ", target " + safeHelpCell(defaults.Target) + ", provider " + safeHelpCell(defaults.Provider) + ". `--provider` chooses infrastructure; version chooses Kubernetes or OpenShift. Cluster names are generated internally.\n\nUse `key=value`, `--key=value`, or `--key value`; forms can be mixed. A current common-option inventory also recognizes unique bare values, for example `create target=synthetic-target vpc-gen2 us-south-1 bx2.4x16 resource-group=\"Platform Team\"`. Unknown or colliding shorthand must use a key such as `flavor=value`; worker counts and uncommon Satellite values stay keyed. Use `roks` (also `openshift` or `default_openshift`) for the cloud default OpenShift stream, or `iks` (also `kubernetes`, `k8s`, or `default_kubernetes`) for Kubernetes. A compatible numeric stream can refine an alias: `create roks 4.17`. These reserved aliases require a key when used as resource names.\n\nReview the resolved stream and configuration in its thread, then reply with exact `yes` or `no` within five minutes.", "Safe create options\n```\nCommon\n  --target --provider --version --resource-group --worker-count\n\nVPC Gen 2\n  --zone --flavor --vpc-id --subnet-id --public-gateway-id\n\nClassic\n  --datacenter --machine-type --public-vlan-id --private-vlan-id\n\nSatellite\n  --satellite-zone --satellite-managed-from --satellite-location-id\n  --satellite-host-image --satellite-host-profile --satellite-ssh-key-id\n  --satellite-worker-instance-id --satellite-worker-operating-system\n```"}
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
