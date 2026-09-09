// Package inventory discovers the bounded common-option catalog for one configured target.
package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	CatalogVersion   = 1
	MaxResponseBytes = 512 * 1024
	vpcAPIVersion    = "2026-08-04"
)

// Config is the mounted non-secret inventory target configuration.
type Config struct {
	Version int                     `yaml:"version"`
	Targets map[string]TargetConfig `yaml:"targets"`
}

// TargetConfig identifies the configured providers and their private service bases.
type TargetConfig struct {
	Providers []string          `yaml:"providers"`
	Regions   []string          `yaml:"regions,omitempty"`
	Endpoints map[string]string `yaml:"endpoints"`
}

// Catalog is the complete bounded common-option catalog for one target.
type Catalog struct {
	Version          int        `json:"version"`
	Target           string     `json:"target"`
	Providers        []string   `json:"providers"`
	Versions         []Version  `json:"versions"`
	ResourceGroups   []string   `json:"resourceGroups"`
	VPCLocations     []Location `json:"vpcLocations,omitempty"`
	ClassicLocations []Location `json:"classicLocations,omitempty"`
	SatelliteProfile []Profile  `json:"satelliteProfiles,omitempty"`
}

type Version struct {
	Name         string `json:"name"`
	Platform     string `json:"platform"`
	Default      bool   `json:"default"`
	Supported    bool   `json:"supported"`
	EndOfService string `json:"endOfService,omitempty"`
}

type Location struct {
	Name    string   `json:"name"`
	Flavors []string `json:"flavors"`
}

type Profile struct {
	Region string `json:"region"`
	Name   string `json:"name"`
}

// LoadConfig reads exactly one mounted inventory configuration document.
func LoadConfig(data []byte) (Config, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, errors.New("inventory configuration is invalid")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("inventory configuration must contain one document")
	}
	if config.Version != CatalogVersion || len(config.Targets) == 0 {
		return Config{}, errors.New("inventory configuration is incomplete")
	}
	for name, target := range config.Targets {
		if !safeTargetIdentity(name) || target.validate() != nil {
			return Config{}, errors.New("inventory configuration is incomplete")
		}
	}
	return config, nil
}

// Revision returns the stable configuration revision used to bind snapshots to a target.
func Revision(target TargetConfig) (string, error) {
	encoded, err := json.Marshal(target)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "v1-" + hex.EncodeToString(digest[:]), nil
}

func (t TargetConfig) validate() error {
	if len(t.Providers) == 0 || len(t.Endpoints) == 0 || len(t.Providers) > 3 || len(t.Regions) > 32 {
		return errors.New("incomplete target")
	}
	seen := map[string]bool{}
	for _, provider := range t.Providers {
		if (provider != "vpc-gen2" && provider != "classic" && provider != "satellite") || seen[provider] {
			return errors.New("invalid provider")
		}
		seen[provider] = true
	}
	for _, key := range []string{"IAM", "ContainerService", "ResourceManagement"} {
		if err := validBase(t.Endpoints[key]); err != nil {
			return err
		}
	}
	if seen["satellite"] && len(t.Regions) == 0 {
		return errors.New("satellite target is incomplete")
	}
	regions := map[string]bool{}
	for _, region := range t.Regions {
		if !safeName(region) || regions[region] {
			return errors.New("invalid region")
		}
		regions[region] = true
		if seen["satellite"] {
			if _, err := t.regionalVPCBase(region); err != nil {
				return errors.New("satellite target is incomplete")
			}
		}
	}
	return nil
}

func (t TargetConfig) regionalVPCBase(region string) (string, error) {
	base := t.Endpoints["VPC"]
	if strings.Contains(base, "{region}") {
		base = strings.ReplaceAll(base, "{region}", region)
	} else if len(t.Regions) != 1 {
		return "", errors.New("regional VPC endpoint is incomplete")
	}
	if err := validBase(base); err != nil {
		return "", err
	}
	return base, nil
}

func validBase(value string) error {
	base, err := url.Parse(value)
	if err != nil || base.Scheme == "" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "{}") {
		return errors.New("invalid endpoint")
	}
	return nil
}

// Discover reads all catalog pages through configured private service bases.
// Its errors deliberately exclude endpoints, credentials, and service response bodies.
func Discover(ctx context.Context, config Config, targetName, apiKey string, client *http.Client) (Catalog, error) {
	if apiKey == "" {
		return Catalog{}, errors.New("inventory credentials are unavailable")
	}
	target, ok := config.Targets[targetName]
	if !ok {
		return Catalog{}, errors.New("configured inventory target was not found")
	}
	if client == nil {
		client = http.DefaultClient
	}
	d := discovery{client: client, target: target}
	token, accountID, err := d.authenticate(ctx, apiKey)
	if err != nil {
		return Catalog{}, err
	}
	catalog := Catalog{Version: CatalogVersion, Target: targetName, Providers: append([]string(nil), target.Providers...)}
	if catalog.ResourceGroups, err = d.resourceGroups(ctx, token, accountID); err != nil {
		return Catalog{}, err
	}
	if catalog.Versions, err = d.versions(ctx, token); err != nil {
		return Catalog{}, err
	}
	for _, provider := range target.Providers {
		switch provider {
		case "vpc-gen2":
			if catalog.VPCLocations, err = d.vpcLocations(ctx, token); err != nil {
				return Catalog{}, err
			}
		case "classic":
			if catalog.ClassicLocations, err = d.classicLocations(ctx, token); err != nil {
				return Catalog{}, err
			}
		case "satellite":
			if catalog.SatelliteProfile, err = d.satelliteProfiles(ctx, token); err != nil {
				return Catalog{}, err
			}
		}
	}
	if err := catalog.Validate(); err != nil {
		return Catalog{}, errors.New("inventory catalog is incomplete")
	}
	return catalog, nil
}

type discovery struct {
	client *http.Client
	target TargetConfig
}

func (d discovery) authenticate(ctx context.Context, apiKey string) (string, string, error) {
	form := url.Values{"grant_type": {"urn:ibm:params:oauth:grant-type:apikey"}, "apikey": {apiKey}}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := d.request(ctx, "IAM", http.MethodPost, "identity/token", form, "", &token); err != nil || token.AccessToken == "" {
		return "", "", errors.New("inventory authentication failed")
	}
	var user struct {
		AccountID string `json:"account_id"`
		Account   struct {
			BSSAccount string `json:"bss_account"`
		} `json:"account"`
	}
	if err := d.request(ctx, "IAM", http.MethodGet, "identity/userinfo", nil, token.AccessToken, &user); err != nil {
		return "", "", errors.New("inventory account lookup failed")
	}
	account := user.AccountID
	if account == "" {
		account = user.Account.BSSAccount
	}
	if !safeName(account) {
		return "", "", errors.New("inventory account lookup failed")
	}
	return token.AccessToken, account, nil
}

func (d discovery) resourceGroups(ctx context.Context, token, account string) ([]string, error) {
	values := url.Values{"account_id": {account}, "limit": {"100"}}
	var all []string
	for {
		var response struct {
			Resources []struct{ ID, Name, State string } `json:"resources"`
			Next      struct {
				Href string `json:"href"`
			} `json:"next"`
		}
		if err := d.request(ctx, "ResourceManagement", http.MethodGet, "v2/resource_groups?"+values.Encode(), nil, token, &response); err != nil {
			return nil, errors.New("resource group discovery failed")
		}
		for _, resource := range response.Resources {
			if resource.State != "DELETED" && safeValue(resource.Name) {
				all = append(all, resource.Name)
			}
		}
		if response.Next.Href == "" {
			break
		}
		next, err := url.Parse(response.Next.Href)
		if err != nil || next.RawQuery == "" {
			return nil, errors.New("resource group pagination failed")
		}
		values, err = url.ParseQuery(next.RawQuery)
		if err != nil || values.Get("account_id") != account {
			return nil, errors.New("resource group pagination failed")
		}
		values.Set("limit", "100")
	}
	return uniqueSorted(all), nil
}

func (d discovery) versions(ctx context.Context, token string) ([]Version, error) {
	var response map[string][]struct {
		Major        int    `json:"major"`
		Minor        int    `json:"minor"`
		Default      bool   `json:"default"`
		EndOfService string `json:"end_of_service"`
	}
	if err := d.request(ctx, "ContainerService", http.MethodGet, "v1/versions", nil, token, &response); err != nil {
		return nil, errors.New("version discovery failed")
	}
	versions := []Version{}
	for platform, releases := range response {
		if platform != "openshift" && platform != "kubernetes" {
			continue
		}
		for _, release := range releases {
			if release.Major < 1 || release.Minor < 0 {
				return nil, errors.New("version discovery failed")
			}
			supported, err := supportedRelease(release.EndOfService)
			if err != nil {
				return nil, errors.New("version discovery failed")
			}
			name := fmt.Sprintf("%d.%d", release.Major, release.Minor)
			if platform == "openshift" {
				name += "_openshift"
			}
			versions = append(versions, Version{Name: name, Platform: platform, Default: release.Default, Supported: supported, EndOfService: release.EndOfService})
		}
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Name < versions[j].Name })
	return versions, nil
}

func supportedRelease(endOfService string) (bool, error) {
	if endOfService == "" {
		return true, nil
	}
	deadline, err := time.Parse("2006-01-02", endOfService)
	if err != nil {
		return false, err
	}
	return time.Now().Before(deadline.Add(24 * time.Hour)), nil
}

func (d discovery) vpcLocations(ctx context.Context, token string) ([]Location, error) {
	var zones []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := d.request(ctx, "ContainerService", http.MethodGet, "v2/vpc/getZones?provider=vpc-gen2&showFlavors=false", nil, token, &zones); err != nil {
		return nil, errors.New("VPC zone discovery failed")
	}
	locations := make([]Location, 0, len(zones))
	for _, zone := range zones {
		name := zone.Name
		if name == "" {
			name = zone.ID
		}
		if !safeName(name) {
			return nil, errors.New("VPC zone discovery failed")
		}
		var flavors []struct {
			Name   string `json:"name"`
			Flavor string `json:"flavor"`
		}
		if err := d.request(ctx, "ContainerService", http.MethodGet, "v2/getFlavors?"+url.Values{"provider": {"vpc-gen2"}, "zone": {name}}.Encode(), nil, token, &flavors); err != nil {
			return nil, errors.New("VPC flavor discovery failed")
		}
		values := make([]string, 0, len(flavors))
		for _, flavor := range flavors {
			value := flavor.Name
			if value == "" {
				value = flavor.Flavor
			}
			if !safeValue(value) {
				return nil, errors.New("VPC flavor discovery failed")
			}
			values = append(values, value)
		}
		locations = append(locations, Location{Name: name, Flavors: uniqueSorted(values)})
	}
	return sortedLocations(locations), nil
}

func (d discovery) classicLocations(ctx context.Context, token string) ([]Location, error) {
	var zones []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := d.request(ctx, "ContainerService", http.MethodGet, "v1/zones", nil, token, &zones); err != nil {
		return nil, errors.New("classic datacenter discovery failed")
	}
	locations := []Location{}
	for _, zone := range zones {
		name := zone.Name
		if name == "" {
			name = zone.ID
		}
		if zone.Type != "" && zone.Type != "classic" {
			continue
		}
		if !safeName(name) {
			return nil, errors.New("classic datacenter discovery failed")
		}
		var types []struct {
			Name        string `json:"name"`
			MachineType string `json:"machine_type"`
		}
		if err := d.request(ctx, "ContainerService", http.MethodGet, "v1/datacenters/"+url.PathEscape(name)+"/machine-types", nil, token, &types); err != nil {
			return nil, errors.New("classic machine type discovery failed")
		}
		values := make([]string, 0, len(types))
		for _, machine := range types {
			value := machine.Name
			if value == "" {
				value = machine.MachineType
			}
			if !safeValue(value) {
				return nil, errors.New("classic machine type discovery failed")
			}
			values = append(values, value)
		}
		locations = append(locations, Location{Name: name, Flavors: uniqueSorted(values)})
	}
	return sortedLocations(locations), nil
}

func (d discovery) satelliteProfiles(ctx context.Context, token string) ([]Profile, error) {
	profiles := []Profile{}
	for _, region := range d.target.Regions {
		base, err := d.target.regionalVPCBase(region)
		if err != nil {
			return nil, errors.New("Satellite host profile discovery failed")
		}
		values := url.Values{"version": {vpcAPIVersion}, "generation": {"2"}}
		for {
			var response struct {
				Profiles []struct {
					Name string `json:"name"`
				} `json:"profiles"`
				Next struct {
					Href string `json:"href"`
				} `json:"next"`
			}
			if err := d.requestBase(ctx, base, http.MethodGet, "instance/profiles?"+values.Encode(), nil, token, &response); err != nil {
				return nil, errors.New("Satellite host profile discovery failed")
			}
			for _, profile := range response.Profiles {
				if !safeValue(profile.Name) {
					return nil, errors.New("Satellite host profile discovery failed")
				}
				profiles = append(profiles, Profile{Region: region, Name: profile.Name})
			}
			if response.Next.Href == "" {
				break
			}
			next, err := url.Parse(response.Next.Href)
			if err != nil || next.RawQuery == "" {
				return nil, errors.New("Satellite host profile pagination failed")
			}
			values, err = url.ParseQuery(next.RawQuery)
			if err != nil || values.Get("version") != vpcAPIVersion || values.Get("generation") != "2" {
				return nil, errors.New("Satellite host profile pagination failed")
			}
		}
	}
	sort.Slice(profiles, func(i, j int) bool {
		if profiles[i].Region == profiles[j].Region {
			return profiles[i].Name < profiles[j].Name
		}
		return profiles[i].Region < profiles[j].Region
	})
	return profiles, nil
}

func (d discovery) request(ctx context.Context, service, method, relative string, form url.Values, token string, output any) error {
	return d.requestBase(ctx, d.target.Endpoints[service], method, relative, form, token, output)
}

func (d discovery) requestBase(ctx context.Context, baseValue, method, relative string, form url.Values, token string, output any) error {
	base, err := url.Parse(baseValue)
	if err != nil {
		return errors.New("invalid configured service endpoint")
	}
	relativeURL, err := url.Parse(relative)
	if err != nil || relativeURL.IsAbs() || strings.HasPrefix(relativeURL.Path, "/") {
		return errors.New("invalid inventory request")
	}
	requestURL := *base
	requestURL.Path = path.Join(base.Path, relativeURL.Path)
	requestURL.RawQuery = relativeURL.RawQuery
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), body)
	if err != nil {
		return errors.New("inventory request failed")
	}
	request.Header.Set("Accept", "application/json")
	if form != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	client := *d.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("inventory request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return errors.New("inventory service request failed")
	}
	bodyData, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil || len(bodyData) > MaxResponseBytes {
		return errors.New("inventory service response is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(bodyData))
	if err := decoder.Decode(output); err != nil {
		return errors.New("inventory service response is invalid")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("inventory service response is invalid")
	}
	return nil
}

// Validate rejects unsafe, malformed, and incomplete catalog data before report publication.
func (c Catalog) Validate() error {
	if c.Version != CatalogVersion || !safeName(c.Target) || len(c.Providers) == 0 || len(c.Providers) > 3 || len(c.Versions) == 0 || len(c.ResourceGroups) == 0 {
		return errors.New("catalog is incomplete")
	}
	providers := map[string]bool{}
	for _, provider := range c.Providers {
		if (provider != "vpc-gen2" && provider != "classic" && provider != "satellite") || providers[provider] {
			return errors.New("catalog has invalid providers")
		}
		providers[provider] = true
	}
	for _, version := range c.Versions {
		if !safeVersion(version.Name) || (version.Platform != "openshift" && version.Platform != "kubernetes") {
			return errors.New("catalog has invalid versions")
		}
	}
	for _, group := range c.ResourceGroups {
		if !safeValue(group) {
			return errors.New("catalog has invalid resource groups")
		}
	}
	if err := validateLocations(c.VPCLocations); err != nil || validateLocations(c.ClassicLocations) != nil {
		return errors.New("catalog has invalid locations")
	}
	for _, profile := range c.SatelliteProfile {
		if !safeName(profile.Region) || !safeValue(profile.Name) {
			return errors.New("catalog has invalid profiles")
		}
	}
	return nil
}

func validateLocations(locations []Location) error {
	for _, location := range locations {
		if !safeName(location.Name) {
			return errors.New("invalid location")
		}
		for _, flavor := range location.Flavors {
			if !safeValue(flavor) {
				return errors.New("invalid flavor")
			}
		}
	}
	return nil
}
func safeName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_') {
			return false
		}
	}
	return true
}

// safeTargetIdentity matches the target form used as a Kubernetes label and
// PipelineRun/report identity.
func safeTargetIdentity(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for index, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') || (char == '-' && (index == 0 || index == len(value)-1)) {
			return false
		}
	}
	return true
}
func safeValue(value string) bool {
	return len(value) > 0 && len(value) <= 256 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}
func safeVersion(value string) bool {
	return safeValue(value) && len(value) <= 64 && strings.Count(value, ".") >= 1
}
func uniqueSorted(values []string) []string {
	set := map[string]bool{}
	for _, value := range values {
		set[value] = true
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
func sortedLocations(locations []Location) []Location {
	sort.Slice(locations, func(i, j int) bool { return locations[i].Name < locations[j].Name })
	return locations
}

// RedactedError returns a stable diagnostic safe for task logs and controllers.
func RedactedError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("inventory discovery failed: %s", strings.Split(err.Error(), ":")[0])
}
