package pipelines

import (
	"strings"
	"testing"
)

func TestRegistryV2ManifestURL(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		path       string
		repository string
		tag        string
		want       string
	}{
		{
			name:       "host-only registry",
			host:       "registry.example.com",
			path:       "",
			repository: "rhcos-server",
			tag:        "4.18",
			want:       "https://registry.example.com/v2/rhcos-server/manifests/4.18",
		},
		{
			name:       "registry with single path component",
			host:       "registry.example.com",
			path:       "org",
			repository: "rhcos-server",
			tag:        "4.18",
			want:       "https://registry.example.com/v2/org/rhcos-server/manifests/4.18",
		},
		{
			name:       "registry with multi-level path",
			host:       "registry.example.com",
			path:       "org/team",
			repository: "rhcos-server",
			tag:        "4.18",
			want:       "https://registry.example.com/v2/org/team/rhcos-server/manifests/4.18",
		},
		{
			name:       "registry with port",
			host:       "registry.example.com:5000",
			path:       "",
			repository: "rhcos-server",
			tag:        "4.16",
			want:       "https://registry.example.com:5000/v2/rhcos-server/manifests/4.16",
		},
		{
			name:       "registry with port and path",
			host:       "registry.example.com:5000",
			path:       "myorg",
			repository: "rhcos-server",
			tag:        "4.17",
			want:       "https://registry.example.com:5000/v2/myorg/rhcos-server/manifests/4.17",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RegistryV2ManifestURL(tt.host, tt.path, tt.repository, tt.tag)
			if got != tt.want {
				t.Errorf("RegistryV2ManifestURL(%q, %q, %q, %q) =\n  %q\nwant:\n  %q",
					tt.host, tt.path, tt.repository, tt.tag, got, tt.want)
			}
		})
	}
}

func TestDownloadRHCOSTask(t *testing.T) {
	task := DownloadRHCOSTask("registry.access.redhat.com/ubi9:latest")

	name, ok := task["name"].(string)
	if !ok || name != "download-rhcos-images" {
		t.Errorf("task name = %v, want download-rhcos-images", task["name"])
	}

	taskSpec, ok := task["taskSpec"].(map[string]interface{})
	if !ok {
		t.Fatal("taskSpec missing or wrong type")
	}
	steps, ok := taskSpec["steps"].([]map[string]interface{})
	if !ok || len(steps) == 0 {
		t.Fatal("steps missing or empty")
	}

	image, ok := steps[0]["image"].(string)
	if !ok || image != "registry.access.redhat.com/ubi9:latest" {
		t.Errorf("step image = %q, want registry.access.redhat.com/ubi9:latest", image)
	}

	workspaces, ok := task["workspaces"].([]map[string]interface{})
	if !ok || len(workspaces) != 2 {
		t.Errorf("expected 2 workspaces, got %d", len(workspaces))
	}
}

func TestDownloadRHCOSScript_UsesRegistryHostPathSeparation(t *testing.T) {
	script := downloadRHCOSScript()

	if !strings.Contains(script, `REGISTRY_HOST=$(echo "$INTERMEDIATE_REGISTRY" | cut -d/ -f1)`) {
		t.Error("script does not extract REGISTRY_HOST from INTERMEDIATE_REGISTRY")
	}
	if !strings.Contains(script, `REGISTRY_PATH=$(echo "$INTERMEDIATE_REGISTRY" | cut -sd/ -f2-)`) {
		t.Error("script does not extract REGISTRY_PATH from INTERMEDIATE_REGISTRY")
	}
	if !strings.Contains(script, `"https://${REGISTRY_HOST}/v2/${REGISTRY_PATH:+${REGISTRY_PATH}/}rhcos-server/manifests/${RHCOS_VERSION}"`) {
		t.Error("script does not use correct V2 manifest URL with host/path separation")
	}
	if strings.Contains(script, `"https://${INTERMEDIATE_REGISTRY}/v2/rhcos-server/manifests/`) {
		t.Error("script still uses broken URL pattern with INTERMEDIATE_REGISTRY directly in V2 path")
	}
}

func TestBuildRHCOSServerTask(t *testing.T) {
	task := BuildRHCOSServerTask()

	name, ok := task["name"].(string)
	if !ok || name != "build-rhcos-server" {
		t.Errorf("task name = %v, want build-rhcos-server", task["name"])
	}

	runAfter, ok := task["runAfter"].([]string)
	if !ok || len(runAfter) != 1 || runAfter[0] != "download-rhcos-images" {
		t.Errorf("runAfter = %v, want [download-rhcos-images]", task["runAfter"])
	}

	taskSpec, ok := task["taskSpec"].(map[string]interface{})
	if !ok {
		t.Fatal("taskSpec missing or wrong type")
	}
	steps, ok := taskSpec["steps"].([]map[string]interface{})
	if !ok || len(steps) == 0 {
		t.Fatal("steps missing or empty")
	}

	image, ok := steps[0]["image"].(string)
	if !ok || image != "quay.io/buildah/stable:latest" {
		t.Errorf("step image = %q, want quay.io/buildah/stable:latest", image)
	}
}

func TestBuildRHCOSServerScript_UsesRegistryHostPathSeparation(t *testing.T) {
	script := buildRHCOSServerScript()

	if !strings.Contains(script, `REGISTRY_HOST=$(echo "$INTERMEDIATE_REGISTRY" | cut -d/ -f1)`) {
		t.Error("script does not extract REGISTRY_HOST")
	}
	if !strings.Contains(script, `REGISTRY_PATH=$(echo "$INTERMEDIATE_REGISTRY" | cut -sd/ -f2-)`) {
		t.Error("script does not extract REGISTRY_PATH")
	}
	if !strings.Contains(script, `"https://${REGISTRY_HOST}/v2/${REGISTRY_PATH:+${REGISTRY_PATH}/}rhcos-server/manifests/${RHCOS_VERSION}"`) {
		t.Error("script does not use correct V2 manifest URL")
	}
}

func TestRegistryExistsCheckSnippet_AuthLookup(t *testing.T) {
	snippet := registryExistsCheckSnippet()

	lookups := []string{
		"host",
		"'${INTERMEDIATE_REGISTRY}'",
		"'https://' + host",
		"'https://${INTERMEDIATE_REGISTRY}'",
	}
	for _, lookup := range lookups {
		if !strings.Contains(snippet, lookup) {
			t.Errorf("auth lookup snippet missing check for %s", lookup)
		}
	}
}

func TestDownloadAndBuildScripts_HaveConsistentRegistryCheck(t *testing.T) {
	downloadScript := downloadRHCOSScript()
	buildScript := buildRHCOSServerScript()

	urlPattern := `"https://${REGISTRY_HOST}/v2/${REGISTRY_PATH:+${REGISTRY_PATH}/}rhcos-server/manifests/${RHCOS_VERSION}"`
	if !strings.Contains(downloadScript, urlPattern) {
		t.Error("download script missing correct V2 URL pattern")
	}
	if !strings.Contains(buildScript, urlPattern) {
		t.Error("build script missing correct V2 URL pattern")
	}
}
