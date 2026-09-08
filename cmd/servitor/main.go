// Servitor is a read-only Slack Socket Mode front end for ICT state listing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bevicted/servitor/internal/admission"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/config"
	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/ict"
	"github.com/bevicted/servitor/internal/lifecycle"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
)

func main() {
	configPath := flag.String("config", "", "operator configuration YAML path")
	fixturePath := flag.String("socket-mode-fixture", "", "test-only Socket Mode fixture JSON path")
	fixtureOutputPath := flag.String("socket-mode-fixture-output", "", "test-only Socket Mode fixture result JSON path")
	fixtureClockPath := flag.String("socket-mode-fixture-clock", "", "test-only RFC3339 clock file for Socket Mode fixtures")
	flag.Parse()
	selectedConfigPath, err := config.ResolvePath(*configPath)
	if err != nil {
		fail(err)
	}
	operator, err := config.Load(selectedConfigPath)
	if err != nil {
		fail(err)
	}
	secrets, err := config.SecretsFromEnv()
	if err != nil {
		fail(err)
	}
	logs, err := os.OpenFile(filepath.Join(operator.Paths.Logs, "servitor.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fail(fmt.Errorf("open private log: %w", err))
	}
	defer logs.Close()
	logger := log.New(io.MultiWriter(logs, os.Stderr), "servitor: ", log.LstdFlags|log.LUTC)
	logger.Printf("startup config_path=%q", selectedConfigPath)
	logger.Printf("startup phase=configuration-loaded")

	events, err := state.OpenEventStore(operator.Paths.State)
	if err != nil {
		fail(err)
	}
	lifecycles, err := state.OpenLifecycleStore()
	if err != nil {
		fail(err)
	}
	admissionControl, err := admission.Open(operator.Paths.State)
	if err != nil {
		fail(err)
	}
	transport, err := newTransport(*fixturePath, *fixtureOutputPath, secrets)
	if err != nil {
		fail(err)
	}
	var clock lifecycle.Clock
	if *fixtureClockPath != "" {
		if *fixturePath == "" {
			fail(errors.New("-socket-mode-fixture-clock requires -socket-mode-fixture"))
		}
		clock = fixtureClock{path: *fixtureClockPath}
		if _, err := clock.(fixtureClock).read(); err != nil {
			fail(err)
		}
	}
	emergencyCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	socketCtx, cancelSocket := context.WithCancel(emergencyCtx)
	defer cancelSocket()
	logger.Printf("startup phase=slack-authentication")
	self, err := transport.SelfUserID(socketCtx)
	if err != nil {
		fail(err)
	}
	logger.Printf("startup phase=slack-authenticated self_id=%q", self)
	diagnosticLogs, err := diagnostics.Open(operator.Paths.Logs, operator.Logs.MaxSizeBytes)
	if err != nil {
		fail(err)
	}
	if err := diagnosticLogs.StartRetention(emergencyCtx, clock, operator.Logs.ResolvedRetention, time.Hour, func() []diagnostics.Reference {
		records := lifecycles.Records()
		references := make([]diagnostics.Reference, 0, len(records))
		for _, record := range records {
			references = append(references, diagnostics.Reference{ID: record.DiagnosticRef, Resolved: record.Status == "resolved", Updated: record.UpdatedAt})
		}
		return references
	}); err != nil {
		fail(err)
	}
	ictClient := ict.NewClient(operator.ICT.Path, logs)
	ictClient.Diagnostics = diagnosticLogs
	ictClient.Logf = logger.Printf
	cleanup := lifecycle.NewManager(ictClient, lifecycles, operator.Lifecycle.RetryIntervals, operator.Slack.MaintainerID)
	cleanup.Clock = clock
	cleanup.Lease = operator.Lifecycle.Lease
	cleanup.Diagnostics = diagnosticLogs
	cleanup.Logf = logger.Printf
	var cleanupActivity sync.WaitGroup
	var cleanupActivityMu sync.Mutex
	var cleanupDone []func()
	cleanup.OnCleanupActivity = func(active bool) {
		if active {
			cleanupActivity.Add(1)
			cleanupActivityMu.Lock()
			cleanupDone = append(cleanupDone, admissionControl.Activity("cleanup"))
			cleanupActivityMu.Unlock()
			return
		}
		cleanupActivity.Done()
		cleanupActivityMu.Lock()
		if len(cleanupDone) > 0 {
			done := cleanupDone[len(cleanupDone)-1]
			cleanupDone = cleanupDone[:len(cleanupDone)-1]
			cleanupActivityMu.Unlock()
			done()
			return
		}
		cleanupActivityMu.Unlock()
	}
	logger.Printf("startup phase=reconciliation")
	if err := cleanup.Reconcile(socketCtx, func(noticeCtx context.Context, notice lifecycle.Notice) error {
		return transport.Reply(noticeCtx, slackbot.Response{Channel: notice.Channel, ThreadTimestamp: notice.ThreadTimestamp, Text: notice.Text})
	}); err != nil {
		fail(err)
	}
	logger.Printf("startup phase=reconciliation-complete")
	creator := &lifecycle.Creator{ICT: &ictClient, Store: lifecycles, Destroyer: cleanup, ConfigPath: operator.ICT.ConfigPath, TerraformPath: operator.ICT.TerraformPath, ConfirmationTimeout: operator.Lifecycle.ConfirmationTimeout, Lease: operator.Lifecycle.Lease, Logf: logger.Printf, Defaults: command.CreateDefaults{Version: operator.Defaults.Version, Target: operator.Defaults.Target, Provider: operator.Defaults.Provider, ResourceGroup: operator.Defaults.ResourceGroup, Zone: operator.Defaults.Zone, VPCID: operator.Defaults.VPCID, OpenShiftFlavor: operator.Defaults.OpenShiftFlavor, KubernetesFlavor: operator.Defaults.KubernetesFlavor}, Diagnostics: diagnosticLogs, Clock: clock, Admission: admissionControl}
	var creatorActivityMu sync.Mutex
	creatorDone := make(map[string][]func())
	creator.OnActivity = func(kind string, active bool) {
		creatorActivityMu.Lock()
		if active {
			creatorDone[kind] = append(creatorDone[kind], admissionControl.Activity(kind))
			creatorActivityMu.Unlock()
			return
		}
		done := func() {}
		if callbacks := creatorDone[kind]; len(callbacks) > 0 {
			done = callbacks[len(callbacks)-1]
			creatorDone[kind] = callbacks[:len(callbacks)-1]
		}
		creatorActivityMu.Unlock()
		done()
	}
	gracefulComplete := make(chan struct{})
	bot := slackbot.Bot{
		ChannelID:    operator.Slack.ChannelID,
		SelfUserID:   self,
		MaintainerID: operator.Slack.MaintainerID,
		Admission:    admissionControl,
		Events:       events,
		ICT:          ictClient,
		Lifecycles:   lifecycles,
		Lifecycle:    cleanup,
		Creator:      creator,
		Defaults:     creator.Defaults,
		Responder:    transport,
		Logf:         logger.Printf,
	}
	bot.GracefulStop = func(request lifecycle.Request, drained <-chan struct{}) {
		select {
		case <-drained:
		case <-emergencyCtx.Done():
			return
		}
		finalCtx, cancelFinal := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelFinal()
		if err := transport.Reply(finalCtx, slackbot.Response{Channel: request.Channel, Text: "Servitor has stopped after draining active work."}); err != nil {
			logger.Printf("deliver graceful-stop final maintainer DM: %v", err)
		}
		if err := admissionControl.CompleteStop(); err != nil {
			logger.Printf("complete graceful stop: %v", err)
		}
		close(gracefulComplete)
		cancelSocket()
	}
	logger.Printf("ready; accepting Socket Mode events")
	runErr := transport.Run(socketCtx, bot.Handle)
	if emergencyCtx.Err() != nil {
		creator.Shutdown(func(noticeCtx context.Context, notice lifecycle.Notice) error {
			return transport.Reply(noticeCtx, slackbot.Response{Channel: notice.Channel, ThreadTimestamp: notice.ThreadTimestamp, Text: notice.Text})
		})
	} else if admissionControl.Status().Mode == admission.DrainingToStop {
		<-gracefulComplete
	} else {
		creator.Shutdown(func(noticeCtx context.Context, notice lifecycle.Notice) error {
			return transport.Reply(noticeCtx, slackbot.Response{Channel: notice.Channel, ThreadTimestamp: notice.ThreadTimestamp, Text: notice.Text})
		})
	}
	if err := waitForCleanup(runErr, &cleanupActivity); err != nil {
		fail(err)
	}
}

func waitForCleanup(runErr error, cleanupActivity *sync.WaitGroup) error {
	cleanupActivity.Wait()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

func newTransport(fixturePath, fixtureOutputPath string, secrets config.Secrets) (slackbot.Transport, error) {
	if fixturePath == "" {
		if fixtureOutputPath != "" {
			return nil, errors.New("-socket-mode-fixture-output requires -socket-mode-fixture")
		}
		return slackbot.NewSocketMode(secrets.BotToken, secrets.AppToken), nil
	}
	if fixtureOutputPath == "" {
		return nil, errors.New("-socket-mode-fixture-output is required with -socket-mode-fixture")
	}
	return slackbot.NewFixtureSocketMode(fixturePath, fixtureOutputPath)
}

type fixtureClock struct{ path string }

func (c fixtureClock) Now() time.Time {
	now, err := c.read()
	if err != nil {
		return time.Time{}
	}
	return now
}

func (c fixtureClock) After(delay time.Duration) <-chan time.Time {
	result := make(chan time.Time, 1)
	deadline := c.Now().Add(delay)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if now := c.Now(); !now.Before(deadline) {
				result <- now
				return
			}
		}
	}()
	return result
}

func (c fixtureClock) read() (time.Time, error) {
	contents, err := os.ReadFile(c.path)
	if err != nil {
		return time.Time{}, fmt.Errorf("read Socket Mode fixture clock: %w", err)
	}
	now, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(contents)))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse Socket Mode fixture clock: %w", err)
	}
	return now, nil
}

func fail(err error) {
	_, _ = os.Stderr.WriteString("servitor: " + err.Error() + "\n")
	os.Exit(1)
}
