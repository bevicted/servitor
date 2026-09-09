# Servitor

Servitor is a namespaced Kubernetes operator with a leader-elected Slack Socket Mode front end. It creates one `ServitorCluster` custom resource (CR) per Slack owner and reconciles temporary IBM Cloud clusters through Tekton PipelineRuns and ICT. There is no host-local Servitor runtime, workspace authority, lifecycle file, or migration path.

## Ownership and lifecycle

Slack validates authorized requests and writes only `spec.userOptions` and `spec.lifecycle` intent. The controller is the only writer of CR status. It snapshots configured defaults with the explicit safe user options once in `status.resolvedOptions`, including generated names and the pinned execution image. Changed deployment defaults never alter an existing allocation.

The CR owns the observed lifecycle:

- `spec.lifecycle` holds review approval, requested extension expiry, cleanup intent, and the immutable lease/retry snapshot.
- The controller records phase, review and lease deadlines, resolved options, non-secret recovery metadata, backend identity, operation identity, summaries, retry state, and diagnostic reason in `status`.
- Planning creates a disposable PipelineRun. The review is approval of the frozen configuration, not an exact saved Terraform plan. Approval starts a fresh ICT apply with `--auto-approve`; cloud drift can change Terraform actions between review and apply.
- Terraform state is stored only in the configured IBM Cloud Object Storage (COS) S3 backend. Planning metadata and Terraform plans are ephemeral task-local files. No PVC, artifact store, saved-plan handoff, or custom COS client is used.
- `done`, lease expiry, failed apply, rejected or expired review, and CR deletion use the cleanup finalizer. Apply and destroy never overlap. Failed destroy retries from persisted absolute deadlines; exhausted cleanup remains `Unresolved` with recovery context and finalizer retained.

Controller restart recovery is supported: persisted operations, status snapshots, and deadlines are observed rather than recreated. Tekton worker-loss recovery and management-cluster disaster recovery are not supported.

## Deploy

Build immutable operator and task images, then replace the digest placeholders in the deployment overlay:

```sh
make operator-image OPERATOR_IMAGE=registry.example/servitor-operator@sha256:...
make task-image TASK_IMAGE=registry.example/servitor-task@sha256:...
kubectl kustomize config/default
```

Copy `config.example.yaml` to the ConfigMap input used by `config/default`. It contains only non-secret deployment settings: namespace, Slack channel ID, safe defaults, lifecycle policy, ICT target ConfigMap, COS S3 identity, task image digest, and Secret names. `config/default/operator-references.env` supplies resource names. Do not put Slack, IBM Cloud, or COS HMAC values in configuration, CRs, status, CLI arguments, reports, or source control.

The manager reads its mounted configuration from `/etc/servitor/config/config.yaml`; `-config PATH` or `SERVITOR_CONFIG` can select another mounted path. The controller receives the Slack Secret only. Tekton execution receives COS HMAC and IBM credentials from namespace Secrets; the report step receives neither. The task service account has no CR or status write permissions.

Apply the rendered resources in the target namespace. They include the CRD, controller Role, empty-permission task Role, controller Deployment, ConfigMaps, Tekton Task/Pipeline, and a sample CR. The controller reads the selected report container's private Pod log after a PipelineRun completes and validates its bounded structured result before changing status.

## Slack interface

Enable Socket Mode with `connections:write`, `chat:write`, and the message-history scopes/events needed for the configured channel and DMs. Keep `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` in the referenced Kubernetes Secret.

```text
DM
  help [command]
  list

Configured channel
  @servitor help [command]
  @servitor create [safe flags]
  @servitor done
  @servitor extend [N[h]]
  @servitor list

Lifecycle thread
  yes | no
  done
  extend [N[h]]
```

`create` writes explicit safe flags to `spec.userOptions`; the controller overlays startup defaults and records the resolved result. Platform is derived from the numeric version: `4.*` selects OpenShift and `1.*` selects Kubernetes, so `--platform` is not a supported create flag. Cluster names are generated internally, so `--name` is not a supported create flag. Only the owner in the initiating thread can approve, reject, extend, or request cleanup. `destroy` remains a silent alias for `done`.

There are no maintainer `status`, `pause`, `unpause`, or `stop` commands, and no replacement command for them. Slack delivery is not exactly once: a controller crash after posting and before recording the receipt can duplicate a notification.

## Operations and diagnostics

Use opaque Kubernetes references when investigating a lifecycle: the namespaced `ServitorCluster` name/UID, `status.operation.id`, `status.operation.pipelineRunName`, the matching TaskRun, and the report container's private Pod log. Inspect private cluster logs with authorized cluster access. Do not expose or copy credentials, raw Terraform plans or state, task report internals, workspace paths, or host filesystem paths into Slack, CR status, tickets, or source control.

Excluded behavior is intentional: no local compatibility or allocation migration, no local filesystem/process supervision, no PVC or artifact store, no Tekton worker-loss recovery, no management-cluster disaster recovery, no exactly-once Slack guarantee, and no maintainer-command replacement.

## Sample CR

`config/samples/servitor_v1alpha1_servitorcluster.yaml` shows the schema. Create requests normally originate from Slack, but an authorized automation client can create a CR with immutable Slack identity, explicit `spec.userOptions`, and the required `spec.lifecycle` lease/retry snapshot. The controller adds the cleanup finalizer and writes all status fields.

## Development verification

```sh
gofmt -d $(find api cmd internal -name '*.go')
go test ./...
go test -race ./...
kubectl kustomize config/default
```

Live OpenShift, Tekton, COS, and Slack checks require the designated cluster namespace, COS key prefix, and credentials. Do not treat missing or pruned task logs as proof that a cloud operation did not run.
