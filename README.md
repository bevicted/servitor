# Servitor

Servitor is a namespaced Kubernetes operator with a leader-elected Slack Socket Mode front end. It creates one `ServitorCluster` custom resource (CR) per initiating Slack lifecycle thread and reconciles temporary IBM Cloud clusters through Tekton PipelineRuns and ICT. There is no host-local Servitor runtime, workspace authority, lifecycle file, or migration path.

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

Copy `config.example.yaml` to the ConfigMap input used by `config/default`. It contains only non-secret deployment settings: namespace, Slack channel ID and per-user allocation cap, safe defaults, lifecycle and private inventory refresh policy, ICT target ConfigMap, COS S3 identity, task image digest, and Secret names. `slack.max_allocations_per_user` defaults to 3 when omitted or zero and is read only at operator startup; restart the operator after changing it. Lowering the cap leaves existing allocations usable and blocks only new creates until the owner's active count is below the cap. `config/default/operator-references.env` supplies resource names. Do not put Slack, IBM Cloud, or COS HMAC values in configuration, CRs, status, CLI arguments, reports, or source control.

The manager reads its mounted configuration from `/etc/servitor/config/config.yaml`; `-config PATH` or `SERVITOR_CONFIG` can select another mounted path. The controller receives the Slack Secret only. Tekton execution receives COS HMAC and IBM credentials from namespace Secrets; the report step receives neither. The task service account has no CR or status write permissions.

## Public kubeconfig publication

Phase 1 can store a complete public admin kubeconfig for a configured `auth.public_targets` target when the allocation is non-Satellite. Eligibility is frozen with the allocation, so later configuration changes cannot redirect publication. Before apply, the controller creates a UID-bound Secret and a dedicated publisher ServiceAccount, Role, and RoleBinding. That Role is limited to `get`, `update`, and `patch` on its one Secret; it cannot create, list, or access another Secret.

Task pods disable automatic ServiceAccount token mounting. The IBM/COS credential-bearing execute step writes the optional kubeconfig only to a memory-backed task volume. The credential-free publish step alone receives a short-lived projected Kubernetes token and atomically updates the allocation Secret after validating the self-contained kubeconfig-only artifact. The report step receives neither the auth volume nor a Kubernetes token. Publication failures and missing or malformed artifacts are recorded only as safe availability metadata and do not prevent a successful infrastructure apply from reaching Ready.

Delivery is opt-in. At create time, bare `auth`, `auth=true`, and `auth=false` are accepted; the default is no delivery. An eligible public opt-in queues the same owner-DM delivery as an owner-thread `auth` request. Private-only and Satellite opt-ins continue provisioning, reply `VPN-backed authentication is not implemented yet`, and do not queue delivery or auth publication. Cleanup removes the publisher binding and Secret before waiting for in-flight work, then removes the publisher Role and ServiceAccount, preventing a late publisher from recreating data.

## Public kubeconfig delivery

Only the persisted owner can send exact `auth` in the initiating lifecycle thread. Eligible public requests made before Ready are queued as one latest request. Once Ready, Servitor consumes the request in controller-owned status before opening the owner's DM and sharing the stored `kubeconfig.yaml` with Slack's external-upload API. Delivery never uses the lifecycle channel, and ordinary thread messages contain neither credentials nor download links.

A consumed request is never automatically retried: an upload failure, uncertain Slack outcome, receipt eviction, reconciliation, or restart does not replay it. Send a newer `auth` request to resend the unchanged stored file. Missing files remain unavailable; `auth` never invokes ICT, acquires credentials, renews them, or changes Ready. Cleanup and lease expiry cancel pending requests before a Secret read. Slack cannot transactionally recall an upload already accepted before cleanup.

## Private inventory export

`servitor-inventory` is a private internal Tekton Pipeline for the configured target's common create options. The leader starts discovery when a target has no snapshot, then refreshes each target hourly by default. It persists target run identity, deadlines, last-good catalog, and configuration revision in namespaced ConfigMaps, so a replacement leader adopts a stored run instead of creating a duplicate. Target changes or removal immediately invalidate matching data; stale results for an earlier revision are ignored.

The refresh policy defaults to `refresh_interval: 1h` and `maximum_age: 24h`. A failed, malformed, stale-revision, or oversized report retains the last-good catalog and schedules a bounded retry. Snapshot consumers report missing, expired, or unusable data rather than treating it as current. Refreshes are independent per target; they do not create allocations or make partial target failures global success.

The execute step reads the mounted non-secret ICT target configuration and IBM credential, then emits a separately validated inventory report from a credential-free report step. It does not use COS, Terraform, ICT provisioning, Kubernetes writes, or allocation operation labels. The catalog includes configured provider names, version/default metadata, resource groups, VPC zones and flavors, Classic datacenters and machine types, and default-region VPC profiles for supported Satellite host-profile matching. The mounted target config uses ICT v1 directly: target names, `providers`, `default_region`, and lower-snake-case `endpoints` keys. Service bases are preserved, including `/global` and `/v1` prefixes. Inventory uses IAM `identity/token` and `identity/userinfo`, Resource Management `v2/resource_groups`, Container Service version/zone/flavor routes, and VPC `instance/profiles` pinned to API version `2026-08-04`. All discovery failures, malformed responses, pagination errors, and reports exceeding 512 KiB fail without publishing a partial catalog.

Apply the rendered resources in the target namespace. They include the CRD, controller Role, empty-permission task Role, controller Deployment, ConfigMaps, Tekton Task/Pipeline, and a sample CR. The controller reads the selected report container's private Pod log after a PipelineRun completes, validates its bounded structured result, atomically publishes a complete target snapshot, and removes terminal discovery runs without deleting allocation runs.

## Slack interface

Enable Socket Mode with the `connections:write` app scope. Grant the bot `chat:write`, `files:write`, `im:write`, and the message-history scopes/events needed for the configured channel and DMs, then reinstall the app after changing scopes. Keep `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` in the referenced Kubernetes Secret.

```text
DM
  help [command]
  list
  refresh inventory  (configured maintainers only)

Configured channel
  @servitor help [command]
  @servitor create [safe options]
  @servitor done  (request cleanup for all of your allocations in this channel)
  @servitor list

Lifecycle thread
  yes | no
  done  (release this allocation)
  extend [N[h]]
  auth
```

`create` writes explicit safe options to `spec.userOptions`; omitted version uses the configured Servitor default. Each initiating channel/thread pair has a deterministic allocation identity. A repeated create in that same thread retains its allocation state, lease, and options and links to that lifecycle thread when Slack can resolve it. A new thread can create an independent allocation until the configured per-user active allocation cap is reached. Channel-only commands sent by DM redirect using the configured channel's Slack markup. Provisioning options use `key=value`; creation also accepts bare `auth`, `auth=true`, and `auth=false` as public-delivery-only options, defaulting to no delivery. An eligible public opt-in queues one owner-DM delivery after Ready. Private-only and Satellite opt-ins continue creating but reply `VPN-backed authentication is not implemented yet`; they do not queue delivery. A current private common-option inventory also recognizes unique bare target, provider, resource group, location, and worker-shape values, for example `@servitor create target=synthetic-target vpc-gen2 us-south-1 bx2.4x16 resource-group="Platform Team"`. Matching is exact and case-sensitive in the selected target, provider, and location context. Unknown or colliding shorthand is rejected without creating a CR; correct it with a key such as `flavor=value`. Worker counts, IDs, and uncommon Satellite options remain keyed, and documentation never lists runtime catalogs. Use the cloud-default aliases `default_openshift`, `openshift`, or `roks` for OpenShift, and `default_kubernetes`, `kubernetes`, `k8s`, or `iks` for Kubernetes. A compatible numeric stream may refine a bare alias, for example `@servitor create roks 4.17`; the planning task resolves a bare alias from the cloud default marker. A resource named like a reserved alias requires a key. `provider=value` selects infrastructure, while numeric streams derive platform: `4.*` selects OpenShift and `1.*` selects Kubernetes. Explicit keys identify an uncommon input but do not bypass configured-target/provider restrictions or planning validation: the planning task checks the current configured target and provider plus its fresh common-option catalog before ICT plans, and ICT remains authoritative for uncatalogued keyed inputs. `platform` is not a supported create option. Cluster names are generated internally, so `name` is not a supported create option. Only the owner in the initiating thread can approve, reject, extend, request public `auth`, or request cleanup for that allocation. `@servitor done` in the configured channel instead requests cleanup for all of the caller's allocations in that channel. Review instructions render the persisted UTC approval deadline; reply with exact `yes` or `no` before that deadline. Expired review decisions are not recorded. `destroy` remains a silent alias for `done`.

`list` returns `Your allocations:` and a `cluster`, `state`, `location`, and `expires` table containing only the caller's allocations, including cleanup-complete allocations until their existing notification grace ends. When the caller has no allocations, it returns the same header-only table. Cleanup in progress and cleanup complete are distinct states. Lease deadlines are persisted UTC timestamps with remaining time; an expired lease is labeled `expired` and an allocation without a deadline is labeled `never`.

Ready messages show how to extend or release one allocation: reply with `extend [N[h]]` or `done` in its lifecycle thread. Use `@servitor done` in the configured channel to request cleanup for all of your allocations there.

Optional `slack.maintainer_ids` holds exact Slack user IDs. Only those users can send the exact `refresh inventory` command in a DM; it starts or joins the normal private target refreshes and later receives a safe success, partial-failure, or failure summary. Empty `maintainer_ids` disables the command. The command never reveals target inventories or changes allocations. There are no maintainer `status`, `pause`, `unpause`, or `stop` commands.

Cleanup notices use the persisted initiating reason before completion, including when a notifier first observes a terminal phase. Scheduled destroy retries show their persisted retry number and UTC deadline. Slack delivery is not exactly once: a delivery claim prevents concurrent command/notifier duplicates and is released on a failed reply, but a crash during that non-transactional sequence can still duplicate or suppress a notice. Arbitrary Slack or controller outages can also outlast the observable cleanup grace.

## Operations and diagnostics

Use opaque Kubernetes references when investigating a lifecycle: the namespaced `ServitorCluster` name/UID, `status.operation.id`, `status.operation.pipelineRunName`, the matching TaskRun, and the report container's private Pod log. Inspect private cluster logs with authorized cluster access. Do not expose or copy credentials, raw Terraform plans or state, task report internals, workspace paths, or host filesystem paths into Slack, CR status, tickets, or source control.

Excluded behavior is intentional: no local compatibility or allocation migration, no local filesystem/process supervision, no PVC or artifact store, no Tekton worker-loss recovery, no management-cluster disaster recovery, no exactly-once Slack guarantee, and no maintainer admission, status, pause, unpause, or stop commands.

## Sample CR

`config/samples/servitor_v1alpha1_servitorcluster.yaml` shows the schema. Create requests normally originate from Slack, but an authorized automation client can create a CR with immutable Slack identity, explicit `spec.userOptions`, and the required `spec.lifecycle` lease/retry snapshot. The controller adds the cleanup finalizer and writes all status fields.

## Development verification

```sh
gofmt -d $(find api cmd internal -name '*.go')
make test
go test -race ./...
kubectl kustomize config/default
```

`make test` runs the unit suite and the RBAC-enforced controller runtime contract in `internal/controller/runtime_integration_test.go`. The integration test starts a local Kubernetes API server and etcd, then exercises the production scheme, cached and direct clients, publication, delivery, and cleanup using `config/rbac/role.yaml`. Its pinned envtest binaries are downloaded into `bin/` on the first run. Use `make test-unit` or `make test-integration` to run either suite separately.

Live OpenShift, Tekton, COS, and Slack checks require the designated cluster namespace, COS key prefix, and credentials. Do not treat missing or pruned task logs as proof that a cloud operation did not run.
