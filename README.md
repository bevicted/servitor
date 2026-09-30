# Servitor

Servitor provisions temporary IBM Cloud clusters from Slack.

It runs as a namespaced Kubernetes operator. Each Slack lifecycle thread maps to a `ServitorCluster` custom resource, and the operator uses Tekton and [ICT](https://github.com/bevicted/ict) to plan, create, and destroy the cluster. Terraform state is stored in IBM Cloud Object Storage (COS).

## How it works

1. A user requests a cluster in the configured Slack channel.
2. Servitor freezes the request and operator defaults, then runs an ICT plan.
3. Servitor posts a sanitized summary to the lifecycle thread.
4. The owner approves the request, or uses `approve=true` when creating it.
5. Servitor runs a fresh apply and manages the cluster until its lease expires or the owner requests cleanup.

Approval applies the frozen configuration, not a saved Terraform plan. Cloud drift can therefore change the actions between review and apply.

Servitor supports:

- OpenShift and Kubernetes clusters on IBM Cloud
- VPC Gen 2 and Classic infrastructure
- Multiple allocations per user, with a configurable limit
- Optional Headlamp installation for Kubernetes clusters
- Public kubeconfig or private VPN authentication bundles
- Automatic lease expiry, cleanup retries, and controller restart recovery
- Private inventory discovery for valid targets, locations, and worker shapes

New Satellite allocations are not supported.

## Slack commands

| Context | Command | Purpose |
| --- | --- | --- |
| DM | `help [command]` | Show help |
| DM | `list` | List your allocations |
| DM | `refresh inventory` | Refresh inventory; maintainers only |
| Configured channel | `@servitor create [options]` | Create an allocation |
| Configured channel | `@servitor list` | List your allocations |
| Configured channel | `@servitor done` | Clean up all your allocations in the channel |
| Lifecycle thread | `yes` / `no` | Approve or reject the request |
| Lifecycle thread | `extend [N[h]]` | Extend the lease |
| Lifecycle thread | `auth` | Send the stored auth bundle by DM |
| Lifecycle thread | `auth retry` | Retry unavailable private VPC auth acquisition |
| Lifecycle thread | `done` | Clean up this allocation |

Only the allocation owner can approve, extend, request credentials, or clean up an allocation from its lifecycle thread. `destroy` is an alias for `done`.

### Create examples

```text
@servitor create version=roks
@servitor create kubernetes headlamp
@servitor create version=iks worker-count=3 auth=true
@servitor create vpc-gen2 us-south-1 bx2.4x16 approve
```

Common options:

| Option | Values |
| --- | --- |
| `target` | Configured ICT target |
| `provider` | `vpc-gen2` or `classic` |
| `version` | Numeric stream, `roks`, `openshift`, `iks`, `kubernetes`, or `k8s` |
| `worker-count` | `1` to `100` |
| `auth` | Request auth delivery when ready |
| `approve` | Approve automatically after the summary is delivered |
| `headlamp` | Install the managed Headlamp add-on; Kubernetes only |
| `private-only` | Create only a private endpoint; VPC Gen 2 only |
| `zone`, `flavor` | VPC Gen 2 placement and worker shape |
| `datacenter`, `machine-type` | Classic placement and worker shape |
| `public-vlan-id`, `private-vlan-id` | Optional Classic VLAN IDs |

Boolean options accept either a bare name, such as `auth`, or `name=true|false`. Inventory-backed values can also be supplied in bare form when the match is unambiguous. Use `help create` and `help create-options` in Slack for the current grammar and configured defaults.

Cluster names, resource groups, network IDs, and platform selection are controlled or derived by Servitor; users cannot override them directly.

## Authentication delivery

Auth delivery is opt-in with `auth` or `auth=true` at creation time, or by sending `auth` in the lifecycle thread.

- Public allocations receive `kubeconfig.yaml`.
- Private-only allocations receive `kubeconfig.yaml` and `client.ovpn` together.
- Credentials are sent only to the owner's DM, never to the lifecycle channel.
- Cleanup removes the allocation's stored auth bundle.

`auth` resends an existing bundle; it does not reacquire credentials. `auth retry` is limited to an unexpired, Ready, private VPC allocation whose auth bundle is unavailable. It retries auth acquisition without reapplying the cluster.

## Deployment

### Requirements

- A Kubernetes or OpenShift namespace with Tekton Pipelines installed
- A Slack app using Socket Mode
- IBM Cloud credentials and a COS bucket for Terraform state
- An ICT target configuration
- An image registry for the operator and task images

### 1. Build images

```sh
make operator-image OPERATOR_IMAGE=registry.example/servitor-operator:dev
make task-image TASK_IMAGE=registry.example/servitor-task:dev
```

`make task-image` uses the ICT checkout at `../ict` by default. Override it with `ICT_SOURCE=/path/to/ict`.

Push both images, obtain their registry digests, and use digest-qualified references in the deployment overlay:

```text
registry.example/servitor-operator@sha256:...
registry.example/servitor-task@sha256:...
```

Pushes to `main` also publish `linux/amd64` images to:

- `ghcr.io/bevicted/servitor-operator`
- `ghcr.io/bevicted/servitor-task`

The workflow adds `latest` and `sha-<commit>` tags. Tags can move, so deploy the digests reported by the successful workflow. The task image uses the exact ICT commit in [`build/ict-revision`](build/ict-revision).

### 2. Configure the overlay

Start with [`config.example.yaml`](config.example.yaml), then update these files or replace them in your own Kustomize overlay:

| File | Contents |
| --- | --- |
| [`config/default/operator-config.yaml`](config/default/operator-config.yaml) | Namespace, Slack channel, defaults, lifecycle policy, network bindings, COS backend, and task image |
| [`config/default/ict-config.yaml`](config/default/ict-config.yaml) | Non-secret ICT targets |
| [`config/default/operator-references.env`](config/default/operator-references.env) | ConfigMap and Secret names |
| [`config/default/kustomization.yaml`](config/default/kustomization.yaml) | Operator image digest and rendered resources |

Create the referenced Secrets separately:

- Slack Secret: `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN`
- IBM Cloud credential Secret for ICT
- COS HMAC credential Secret for the Terraform S3 backend

Do not put credentials in configuration, custom resources, CLI arguments, reports, or source control.

The default Kustomization includes the sample CR in [`config/samples`](config/samples). Remove it from a production overlay unless you intend to create that resource.

Preview and apply the finished overlay:

```sh
kubectl kustomize config/default
kubectl apply -k config/default
```

### 3. Configure Slack

Enable Socket Mode and grant the app:

- `connections:write`
- `chat:write`
- `files:write`
- `im:write`
- The message-history scopes and events required for the configured channel and DMs

Reinstall the app after changing scopes.

## State, security, and recovery

- Slack writes authorized intent to the CR spec. The controller is the only status writer.
- Operator defaults, generated names, policy, and image references are frozen per allocation.
- Terraform state lives only in the configured COS S3 backend. Plans and planning metadata are ephemeral.
- Apply and destroy never run at the same time.
- Cleanup uses a finalizer. Failed destroys retry from persisted deadlines; exhausted cleanup remains visible as `Unresolved`.
- The controller can adopt persisted work after a restart.
- Task credentials are isolated from reporting and Slack delivery steps.
- Slack delivery is best effort, not exactly once.

Servitor does not recover from lost Tekton workers or a lost management cluster. Keep normal backups and operational controls for the Kubernetes cluster and COS backend.

For diagnostics, start with the `ServitorCluster` name and UID, `status.operation.id`, `status.operation.pipelineRunName`, its TaskRun, and the report container log. Never copy credentials, Terraform plans, Terraform state, or private task output into Slack or tickets.

## Development

```sh
make build
make test
# Optional additional race check:
go test -race ./...
```

`make test` runs unit tests and the RBAC-enforced controller integration test. Its envtest binaries are downloaded into `bin/` on first use.

Useful targets:

- `make test-unit`
- `make test-integration`
- `make manifests`
- `make operator-image`
- `make task-image`
