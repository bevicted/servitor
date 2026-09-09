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

Copy `config.example.yaml` to the ConfigMap input used by `config/default`. It contains only non-secret deployment settings: namespace, Slack channel ID, safe defaults, lifecycle and private inventory refresh policy, ICT target ConfigMap, COS S3 identity, task image digest, and Secret names. `config/default/operator-references.env` supplies resource names. Do not put Slack, IBM Cloud, or COS HMAC values in configuration, CRs, status, CLI arguments, reports, or source control.

The manager reads its mounted configuration from `/etc/servitor/config/config.yaml`; `-config PATH` or `SERVITOR_CONFIG` can select another mounted path. The controller receives the Slack Secret only. Tekton execution receives COS HMAC and IBM credentials from namespace Secrets; the report step receives neither. The task service account has no CR or status write permissions.

## Private inventory export

`servitor-inventory` is a private internal Tekton Pipeline for the configured target's common create options. The leader starts discovery when a target has no snapshot, then refreshes each target hourly by default. It persists target run identity, deadlines, last-good catalog, and configuration revision in namespaced ConfigMaps, so a replacement leader adopts a stored run instead of creating a duplicate. Target changes or removal immediately invalidate matching data; stale results for an earlier revision are ignored.

The refresh policy defaults to `refresh_interval: 1h` and `maximum_age: 24h`. A failed, malformed, stale-revision, or oversized report retains the last-good catalog and schedules a bounded retry. Snapshot consumers report missing, expired, or unusable data rather than treating it as current. Refreshes are independent per target; they do not create allocations or make partial target failures global success.

The execute step reads the mounted non-secret target configuration and IBM credential, then emits a separately validated inventory report from a credential-free report step. It does not use COS, Terraform, ICT provisioning, Kubernetes writes, or allocation operation labels. The catalog includes configured provider names, version/default metadata, resource groups, VPC zones and flavors, Classic datacenters and machine types, and regional VPC profiles for supported Satellite host-profile matching. The mounted target config has version `1`, target names, `providers`, and configured service `endpoints`. Service bases are preserved, including `/global` and `/v1` prefixes. Inventory uses IAM `identity/token` and `identity/userinfo`, Resource Management `v2/resource_groups`, Container Service version/zone/flavor routes, and VPC `instance/profiles` pinned to API version `2026-08-04`. All discovery failures, malformed responses, pagination errors, and reports exceeding 512 KiB fail without publishing a partial catalog.

Apply the rendered resources in the target namespace. They include the CRD, controller Role, empty-permission task Role, controller Deployment, ConfigMaps, Tekton Task/Pipeline, and a sample CR. The controller reads the selected report container's private Pod log after a PipelineRun completes, validates its bounded structured result, atomically publishes a complete target snapshot, and removes terminal discovery runs without deleting allocation runs.

## Slack interface

Enable Socket Mode with `connections:write`, `chat:write`, and the message-history scopes/events needed for the configured channel and DMs. Keep `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` in the referenced Kubernetes Secret.

```text
DM
  help [command]
  list

Configured channel
  @servitor help [command]
  @servitor create [safe options]
  @servitor done
  @servitor extend [N[h]]
  @servitor list

Lifecycle thread
  yes | no
  done
  extend [N[h]]
```

`create` writes explicit safe options to `spec.userOptions`; omitted version uses the configured Servitor default. Use `key=value`, `--key=value`, or `--key value` in the same request. A current private common-option inventory also recognizes unique bare target, provider, resource group, location, and worker-shape values, for example `@servitor create target=synthetic-target vpc-gen2 us-south-1 bx2.4x16 resource-group="Platform Team"`. Matching is exact and case-sensitive in the selected target, provider, and location context. Unknown or colliding shorthand is rejected without creating a CR; correct it with a key such as `flavor=value`. Worker counts, IDs, and uncommon Satellite options remain keyed, and documentation never lists runtime catalogs. Use the cloud-default aliases `default_openshift`, `openshift`, or `roks` for OpenShift, and `default_kubernetes`, `kubernetes`, `k8s`, or `iks` for Kubernetes. A compatible numeric stream may refine a bare alias, for example `@servitor create roks 4.17`; the planning task resolves a bare alias from the cloud default marker. A resource named like a reserved alias requires a key. `--provider` selects infrastructure, while numeric streams derive platform: `4.*` selects OpenShift and `1.*` selects Kubernetes. Explicit keys identify an uncommon input but do not bypass configured-target/provider restrictions or planning validation: the planning task checks the current configured target and provider plus its fresh common-option catalog before ICT plans, and ICT remains authoritative for uncatalogued keyed inputs. `--platform` is not a supported create option. Cluster names are generated internally, so `--name` is not a supported create option. Only the owner in the initiating thread can approve, reject, extend, or request cleanup. Review instructions render the persisted UTC approval deadline; reply with exact `yes` or `no` before that deadline. Expired review decisions are not recorded. `destroy` remains a silent alias for `done`.

There are no maintainer `status`, `pause`, `unpause`, or `stop` commands, and no replacement command for them. Cleanup notices use the persisted initiating reason before completion, including when a notifier first observes a terminal phase. Scheduled destroy retries show their persisted retry number and UTC deadline. Slack delivery is not exactly once: a delivery claim prevents concurrent command/notifier duplicates and is released on a failed reply, but a crash during that non-transactional sequence can still duplicate or suppress a notice. Arbitrary Slack or controller outages can also outlast the observable cleanup grace.

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
