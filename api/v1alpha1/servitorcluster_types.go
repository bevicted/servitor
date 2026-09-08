package v1alpha1

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	PhasePending          = "Pending"
	PhasePlanning         = "Planning"
	PhaseAwaitingApproval = "AwaitingApproval"
	PhaseApplying         = "Applying"
	PhaseReady            = "Ready"
	PhaseCleanupPending   = "CleanupPending"
	PhaseCleanupComplete  = "CleanupComplete"
	PhaseUnresolved       = "Unresolved"
	CleanupFinalizer      = "servitor.bevicted.github.io/cleanup"
)

// SlackIdentity is immutable routing and authorization identity for an allocation.
type SlackIdentity struct {
	OwnerID         string `json:"ownerID"`
	ChannelID       string `json:"channelID"`
	ThreadTimestamp string `json:"threadTimestamp"`
}

// UserOptions are the explicit safe create inputs. Empty fields mean the user did not supply them.
type UserOptions struct {
	Target                         string   `json:"target,omitempty"`
	Provider                       string   `json:"provider,omitempty"`
	Platform                       string   `json:"platform,omitempty"`
	Version                        string   `json:"version,omitempty"`
	ResourceGroup                  string   `json:"resourceGroup,omitempty"`
	Zone                           string   `json:"zone,omitempty"`
	Flavor                         string   `json:"flavor,omitempty"`
	VPCID                          string   `json:"vpcID,omitempty"`
	Datacenter                     string   `json:"datacenter,omitempty"`
	MachineType                    string   `json:"machineType,omitempty"`
	PublicVLANID                   string   `json:"publicVLANID,omitempty"`
	PrivateVLANID                  string   `json:"privateVLANID,omitempty"`
	SubnetIDs                      []string `json:"subnetIDs,omitempty"`
	PublicGatewayIDs               []string `json:"publicGatewayIDs,omitempty"`
	SatelliteZones                 []string `json:"satelliteZones,omitempty"`
	SatelliteManagedFrom           string   `json:"satelliteManagedFrom,omitempty"`
	SatelliteLocationID            string   `json:"satelliteLocationID,omitempty"`
	SatelliteHostImage             string   `json:"satelliteHostImage,omitempty"`
	SatelliteHostProfile           string   `json:"satelliteHostProfile,omitempty"`
	SatelliteSSHKeyID              string   `json:"satelliteSSHKeyID,omitempty"`
	SatelliteWorkerInstanceIDs     []string `json:"satelliteWorkerInstanceIDs,omitempty"`
	SatelliteWorkerOperatingSystem string   `json:"satelliteWorkerOperatingSystem,omitempty"`
	Name                           string   `json:"name,omitempty"`
	WorkerCount                    int      `json:"workerCount,omitempty"`
}

// LifecyclePolicy is snapshotted when the CR is created.
type LifecyclePolicy struct {
	ConfirmationDeadline *metav1.Time `json:"confirmationDeadline,omitempty"`
	InitialLeaseSeconds  int64        `json:"initialLeaseSeconds"`
	RetrySeconds         []int64      `json:"retrySeconds"`
	Approval             string       `json:"approval,omitempty"`
	// RequestedExpiry is an absolute extension target. Its value is stable across
	// Slack redelivery, controller restarts, and optimistic-concurrency retries.
	RequestedExpiry  *metav1.Time `json:"requestedExpiry,omitempty"`
	CleanupRequested bool         `json:"cleanupRequested,omitempty"`
}

// ServitorClusterSpec is immutable after creation except lifecycle approval intent.
type ServitorClusterSpec struct {
	Slack       SlackIdentity   `json:"slack"`
	UserOptions UserOptions     `json:"userOptions,omitempty"`
	Lifecycle   LifecyclePolicy `json:"lifecycle"`
}

// ResolvedOptions are the once-frozen effective planning inputs.
type ResolvedOptions struct {
	UserOptions `json:",inline"`
	ClusterName string `json:"clusterName,omitempty"`
	Region      string `json:"region,omitempty"`
}

// BackendIdentity identifies remote Terraform state without credentials.
type BackendIdentity struct {
	Version                   int    `json:"version"`
	Bucket                    string `json:"bucket,omitempty"`
	Key                       string `json:"key"`
	Region                    string `json:"region,omitempty"`
	Endpoint                  string `json:"endpoint,omitempty"`
	SkipCredentialsValidation bool   `json:"skipCredentialsValidation"`
	SkipMetadataAPICheck      bool   `json:"skipMetadataAPICheck"`
	SkipRegionValidation      bool   `json:"skipRegionValidation"`
	SkipRequestingAccountID   bool   `json:"skipRequestingAccountID"`
	ForcePathStyle            bool   `json:"forcePathStyle,omitempty"`
	UseLockfile               bool   `json:"useLockfile,omitempty"`
}

// RecoveryMetadata contains only non-secret normalized values required for later operations.
type RecoveryMetadata struct {
	Version                          int               `json:"version"`
	Target                           string            `json:"target,omitempty"`
	Endpoints                        map[string]string `json:"endpoints,omitempty"`
	Values                           RecoveryValues    `json:"values"`
	SatelliteSSHPublicKeyFingerprint string            `json:"satelliteSSHPublicKeyFingerprint,omitempty"`
	TFVarsSHA256                     string            `json:"tfvarsSHA256,omitempty"`
}

// RecoveryValues are ICT's normalized non-secret Terraform inputs. They let a
// later fresh operation reconstruct its ephemeral tfvars without resolution.
type RecoveryValues struct {
	ClusterName                    string   `json:"cluster_name"`
	ResourceGroupName              string   `json:"resource_group_name"`
	Region                         string   `json:"region"`
	ClusterMode                    string   `json:"cluster_mode"`
	Platform                       string   `json:"platform"`
	KubeVersion                    string   `json:"kube_version"`
	WorkerCount                    int      `json:"worker_count"`
	Zone                           string   `json:"zone,omitempty"`
	Flavor                         string   `json:"flavor,omitempty"`
	VPCID                          string   `json:"vpc_id,omitempty"`
	SubnetIDs                      []string `json:"subnet_ids,omitempty"`
	PublicGatewayIDs               []string `json:"public_gateway_ids,omitempty"`
	Datacenter                     string   `json:"datacenter,omitempty"`
	MachineType                    string   `json:"machine_type,omitempty"`
	PublicVLANID                   string   `json:"public_vlan_id,omitempty"`
	PrivateVLANID                  string   `json:"private_vlan_id,omitempty"`
	SatelliteZones                 []string `json:"satellite_zones,omitempty"`
	SatelliteManagedFrom           string   `json:"satellite_managed_from,omitempty"`
	SatelliteLocationID            string   `json:"satellite_location_id,omitempty"`
	SatelliteHostImage             string   `json:"satellite_host_image,omitempty"`
	SatelliteHostProfile           string   `json:"satellite_host_profile,omitempty"`
	SatelliteSSHKeyID              string   `json:"satellite_ssh_key_id,omitempty"`
	SatelliteWorkerInstanceIDs     []string `json:"satellite_worker_instance_ids,omitempty"`
	SatelliteWorkerOperatingSystem string   `json:"satellite_worker_operating_system,omitempty"`
}

// OperationReference is persisted before a PipelineRun can be created.
type OperationReference struct {
	ID              string      `json:"id"`
	Kind            string      `json:"kind"`
	PipelineRunName string      `json:"pipelineRunName"`
	StartedAt       metav1.Time `json:"startedAt"`
	Dispatched      bool        `json:"dispatched,omitempty"`
	Adopted         bool        `json:"adopted,omitempty"`
}

// CleanupReason identifies the terminal transition that requested cleanup.
type CleanupReason string

const (
	CleanupReasonRejected       CleanupReason = "Rejected"
	CleanupReasonReviewExpired  CleanupReason = "ReviewExpired"
	CleanupReasonPlanningFailed CleanupReason = "PlanningFailed"
	CleanupReasonApplyFailed    CleanupReason = "ApplyFailed"
	CleanupReasonLeaseExpired   CleanupReason = "LeaseExpired"
	CleanupReasonExplicit       CleanupReason = "Explicit"
	CleanupReasonDeletion       CleanupReason = "Deletion"
)

// CleanupStatus is the persisted, restart-safe cleanup state machine.
type CleanupStatus struct {
	Reason          CleanupReason `json:"reason"`
	RequestedAt     metav1.Time   `json:"requestedAt"`
	RequiresDestroy bool          `json:"requiresDestroy"`
	RetryCount      int           `json:"retryCount,omitempty"`
	NextRetryAt     *metav1.Time  `json:"nextRetryAt,omitempty"`
	CompletedAt     *metav1.Time  `json:"completedAt,omitempty"`
}

// SummaryResource is deliberately bounded, sanitized review metadata.
type SummaryResource struct {
	Role    string   `json:"role,omitempty"`
	ID      string   `json:"id,omitempty"`
	Name    string   `json:"name,omitempty"`
	Actions []string `json:"actions,omitempty"`
	Reused  bool     `json:"reused,omitempty"`
}

// ReviewSummary is the controller-persisted sanitized planning review.
type ReviewSummary struct {
	Resources []SummaryResource `json:"resources,omitempty"`
}

// ReadySummary is the controller-persisted sanitized resource metadata after apply.
type ReadySummary struct {
	Resources []SummaryResource `json:"resources,omitempty"`
}

// ExtensionOutcome is the controller's typed disposition of an extension intent.
type ExtensionOutcome string

const (
	ExtensionOutcomeApplied  ExtensionOutcome = "Applied"
	ExtensionOutcomeInvalid  ExtensionOutcome = "Invalid"
	ExtensionOutcomeNotReady ExtensionOutcome = "NotReady"
	ExtensionOutcomeExpired  ExtensionOutcome = "Expired"
)

// LeaseExtensionStatus records an extension request and its immutable outcome.
// RequestedExpiry is sufficient request identity because an allocation's expiry
// can never move to the same absolute timestamp twice.
type LeaseExtensionStatus struct {
	RequestedExpiry metav1.Time      `json:"requestedExpiry"`
	PreviousExpiry  *metav1.Time     `json:"previousExpiry,omitempty"`
	NewExpiry       *metav1.Time     `json:"newExpiry,omitempty"`
	AddedSeconds    int64            `json:"addedSeconds,omitempty"`
	Outcome         ExtensionOutcome `json:"outcome"`
}

// ServitorClusterStatus is written exclusively by the controller.
type ServitorClusterStatus struct {
	Phase           string              `json:"phase,omitempty"`
	Conditions      []metav1.Condition  `json:"conditions,omitempty"`
	ResolvedOptions *ResolvedOptions    `json:"resolvedOptions,omitempty"`
	Backend         *BackendIdentity    `json:"backend,omitempty"`
	ExecutionImage  string              `json:"executionImage,omitempty"`
	Operation       *OperationReference `json:"operation,omitempty"`
	Review          *ReviewSummary      `json:"review,omitempty"`
	Recovery        *RecoveryMetadata   `json:"recovery,omitempty"`
	ReviewDeadline  *metav1.Time        `json:"reviewDeadline,omitempty"`
	// ReviewGeneration and ReviewApproval record the spec state that entered AwaitingApproval.
	// An approval must be written in a later generation.
	ReviewGeneration int64                 `json:"reviewGeneration,omitempty"`
	ReviewApproval   string                `json:"reviewApproval,omitempty"`
	Ready            *ReadySummary         `json:"ready,omitempty"`
	LeaseExpiresAt   *metav1.Time          `json:"leaseExpiresAt,omitempty"`
	LeaseExtension   *LeaseExtensionStatus `json:"leaseExtension,omitempty"`
	ApplyDispatched  bool                  `json:"applyDispatched,omitempty"`
	CleanupRequested bool                  `json:"cleanupRequested,omitempty"`
	Cleanup          *CleanupStatus        `json:"cleanup,omitempty"`
	Diagnostic       string                `json:"diagnostic,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,path=servitorclusters,shortName=svccluster
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
type ServitorCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ServitorClusterSpec   `json:"spec,omitempty"`
	Status            ServitorClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ServitorClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ServitorCluster `json:"items"`
}

func (s ServitorClusterSpec) Validate() error {
	if strings.TrimSpace(s.Slack.OwnerID) == "" || strings.TrimSpace(s.Slack.ChannelID) == "" || strings.TrimSpace(s.Slack.ThreadTimestamp) == "" {
		return fmt.Errorf("slack ownerID, channelID, and threadTimestamp are required")
	}
	if s.UserOptions.WorkerCount < 0 || s.UserOptions.WorkerCount > 100 {
		return fmt.Errorf("workerCount must be from 1 through 100 when supplied")
	}
	if s.Lifecycle.InitialLeaseSeconds < 3600 || s.Lifecycle.InitialLeaseSeconds > 86400 || s.Lifecycle.InitialLeaseSeconds%3600 != 0 {
		return fmt.Errorf("initialLeaseSeconds must be a whole number of hours from 1 through 24")
	}
	if len(s.Lifecycle.RetrySeconds) > 8 {
		return fmt.Errorf("retrySeconds has too many values")
	}
	for _, retry := range s.Lifecycle.RetrySeconds {
		if retry <= 0 || retry > 86400 {
			return fmt.Errorf("retrySeconds contains an invalid duration")
		}
	}
	if s.UserOptions.Provider != "" && s.UserOptions.Provider != "vpc-gen2" && s.UserOptions.Provider != "classic" && s.UserOptions.Provider != "satellite" {
		return fmt.Errorf("unsupported provider")
	}
	return nil
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &ServitorCluster{}, &ServitorClusterList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

func (in *ServitorCluster) DeepCopyInto(out *ServitorCluster) {
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = *in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}
func (in *ServitorCluster) DeepCopy() *ServitorCluster {
	if in == nil {
		return nil
	}
	out := new(ServitorCluster)
	in.DeepCopyInto(out)
	return out
}
func (in *ServitorCluster) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *ServitorClusterList) DeepCopyInto(out *ServitorClusterList) {
	*out = *in
	out.ListMeta = in.ListMeta
	out.Items = make([]ServitorCluster, len(in.Items))
	for i := range in.Items {
		in.Items[i].DeepCopyInto(&out.Items[i])
	}
}
func (in *ServitorClusterList) DeepCopy() *ServitorClusterList {
	if in == nil {
		return nil
	}
	out := new(ServitorClusterList)
	in.DeepCopyInto(out)
	return out
}
func (in *ServitorClusterList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *ServitorClusterSpec) DeepCopy() *ServitorClusterSpec {
	if in == nil {
		return nil
	}
	out := new(ServitorClusterSpec)
	*out = *in
	out.UserOptions.SubnetIDs = append([]string(nil), in.UserOptions.SubnetIDs...)
	out.UserOptions.PublicGatewayIDs = append([]string(nil), in.UserOptions.PublicGatewayIDs...)
	out.UserOptions.SatelliteZones = append([]string(nil), in.UserOptions.SatelliteZones...)
	out.UserOptions.SatelliteWorkerInstanceIDs = append([]string(nil), in.UserOptions.SatelliteWorkerInstanceIDs...)
	out.Lifecycle.RetrySeconds = append([]int64(nil), in.Lifecycle.RetrySeconds...)
	if in.Lifecycle.ConfirmationDeadline != nil {
		out.Lifecycle.ConfirmationDeadline = in.Lifecycle.ConfirmationDeadline.DeepCopy()
	}
	if in.Lifecycle.RequestedExpiry != nil {
		out.Lifecycle.RequestedExpiry = in.Lifecycle.RequestedExpiry.DeepCopy()
	}
	return out
}
func (in *ServitorClusterStatus) DeepCopy() *ServitorClusterStatus {
	if in == nil {
		return nil
	}
	out := new(ServitorClusterStatus)
	*out = *in
	out.Conditions = append([]metav1.Condition(nil), in.Conditions...)
	if in.ResolvedOptions != nil {
		v := *in.ResolvedOptions
		v.SubnetIDs = append([]string(nil), v.SubnetIDs...)
		v.PublicGatewayIDs = append([]string(nil), v.PublicGatewayIDs...)
		v.SatelliteZones = append([]string(nil), v.SatelliteZones...)
		v.SatelliteWorkerInstanceIDs = append([]string(nil), v.SatelliteWorkerInstanceIDs...)
		out.ResolvedOptions = &v
	}
	if in.Backend != nil {
		v := *in.Backend
		out.Backend = &v
	}
	if in.Operation != nil {
		v := *in.Operation
		out.Operation = &v
	}
	if in.Review != nil {
		v := *in.Review
		v.Resources = append([]SummaryResource(nil), v.Resources...)
		out.Review = &v
	}
	if in.Recovery != nil {
		v := *in.Recovery
		v.Endpoints = make(map[string]string, len(in.Recovery.Endpoints))
		for k, value := range in.Recovery.Endpoints {
			v.Endpoints[k] = value
		}
		v.Values.SubnetIDs = append([]string(nil), v.Values.SubnetIDs...)
		v.Values.PublicGatewayIDs = append([]string(nil), v.Values.PublicGatewayIDs...)
		v.Values.SatelliteZones = append([]string(nil), v.Values.SatelliteZones...)
		v.Values.SatelliteWorkerInstanceIDs = append([]string(nil), v.Values.SatelliteWorkerInstanceIDs...)
		out.Recovery = &v
	}
	if in.Ready != nil {
		v := *in.Ready
		v.Resources = append([]SummaryResource(nil), v.Resources...)
		out.Ready = &v
	}
	if in.ReviewDeadline != nil {
		out.ReviewDeadline = in.ReviewDeadline.DeepCopy()
	}
	if in.LeaseExpiresAt != nil {
		out.LeaseExpiresAt = in.LeaseExpiresAt.DeepCopy()
	}
	if in.LeaseExtension != nil {
		v := *in.LeaseExtension
		if in.LeaseExtension.PreviousExpiry != nil {
			v.PreviousExpiry = in.LeaseExtension.PreviousExpiry.DeepCopy()
		}
		if in.LeaseExtension.NewExpiry != nil {
			v.NewExpiry = in.LeaseExtension.NewExpiry.DeepCopy()
		}
		out.LeaseExtension = &v
	}
	if in.Cleanup != nil {
		v := *in.Cleanup
		if in.Cleanup.NextRetryAt != nil {
			v.NextRetryAt = in.Cleanup.NextRetryAt.DeepCopy()
		}
		if in.Cleanup.CompletedAt != nil {
			v.CompletedAt = in.Cleanup.CompletedAt.DeepCopy()
		}
		out.Cleanup = &v
	}
	return out
}
