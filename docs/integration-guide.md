# Integration Guide for Mirror Operator

This guide shows how other Kubernetes operators, controllers, and tools can integrate with the mirror-operator to automate disconnected OpenShift workflows.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Integration Patterns](#integration-patterns)
- [Watching Resources](#watching-resources)
- [Creating Resources](#creating-resources)
- [Status and Health Checking](#status-and-health-checking)
- [Best Practices](#best-practices)
- [Examples](#examples)

---

## Overview

The mirror-operator provides CRDs that enable:

- **Content Collection**: Automate pulling of OCP releases, operators, and images
- **Bundle Management**: Create portable bundles for airgapped environments
- **Mirror Import**: Deploy bundles to disconnected registries
- **Cluster Bootstrapping**: Provision new clusters from mirrored content
- **Platform Orchestration**: Manage the complete connected/airgapped lifecycle

### Common Integration Use Cases

1. **GitOps Tools** (ArgoCD, Flux): Sync DisconnectedPlatform resources across environments
2. **Backup/DR Tools**: Trigger collections before backup windows
3. **Compliance Tools**: Monitor collection/import versions for compliance
4. **Automation Controllers** (Ansible Operator): Orchestrate multi-cluster deployments
5. **CI/CD Pipelines**: Trigger collections on release schedules

---

## Quick Start

### Prerequisites

1. Mirror-operator installed in your cluster:
   ```bash
   kubectl get crd | grep mirror.mathianasj.github.com
   ```

2. RBAC permissions to create/read mirror-operator CRDs:
   ```yaml
   apiVersion: rbac.authorization.k8s.io/v1
   kind: ClusterRole
   metadata:
     name: mirror-operator-integration
   rules:
   - apiGroups: ["mirror.mirror.mathianasj.github.com"]
     resources: ["disconnectedplatforms", "collectionpipelines", "mirrorimports", "clusterbootstraps"]
     verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
   - apiGroups: ["mirror.mirror.mathianasj.github.com"]
     resources: ["*/status"]
     verbs: ["get", "list", "watch"]
   ```

### Basic Integration Example (Go)

```go
package main

import (
    "context"
    "fmt"
    
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/client-go/dynamic"
    "k8s.io/client-go/tools/clientcmd"
)

func main() {
    // Load kubeconfig
    config, _ := clientcmd.BuildConfigFromFlags("", "/path/to/kubeconfig")
    client, _ := dynamic.NewForConfig(config)
    
    // Define GVR for DisconnectedPlatform
    gvr := schema.GroupVersionResource{
        Group:    "mirror.mirror.mathianasj.github.com",
        Version:  "v1",
        Resource: "disconnectedplatforms",
    }
    
    // Get DisconnectedPlatform
    dp, err := client.Resource(gvr).Get(context.TODO(), "my-platform", metav1.GetOptions{})
    if err != nil {
        panic(err)
    }
    
    // Read status
    phase, _, _ := unstructured.NestedString(dp.Object, "status", "phase")
    fmt.Printf("Platform phase: %s\n", phase)
}
```

---

## Integration Patterns

### Pattern 1: Event-Driven Collection Triggers

Trigger collections based on external events (new CVE, compliance requirement, scheduled release).

```yaml
# Your controller creates CollectionPipeline on demand
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: CollectionPipeline
metadata:
  name: cve-response-collection
  labels:
    triggered-by: security-scan
    cve-id: CVE-2026-12345
spec:
  triggerType: event
  imageSetConfig: |
    apiVersion: mirror.openshift.io/v1alpha2
    kind: ImageSetConfiguration
    mirror:
      platform:
        channels:
          - name: stable-4.17
            minVersion: 4.17.6  # Patched version
  storage:
    output:
      pvc: collection-storage
```

**When to use:**
- Security response automation
- Scheduled content updates
- Compliance-driven refreshes

### Pattern 2: Multi-Cluster Bundle Distribution

Collect once in a connected cluster, then distribute bundles to multiple airgapped sites.

```go
// Pseudo-code: Watch CollectionPipeline completion
func watchCollection(ctx context.Context, client dynamic.Interface) {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "collectionpipelines",
    }
    
    watch, _ := client.Resource(gvr).Watch(ctx, metav1.ListOptions{})
    
    for event := range watch.ResultChan() {
        obj := event.Object.(*unstructured.Unstructured)
        phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
        
        if phase == "Complete" {
            version, _, _ := unstructured.NestedString(obj.Object, "status", "version")
            // Trigger bundle distribution to airgapped sites
            distributeBundle(version)
        }
    }
}
```

**When to use:**
- Hub-and-spoke airgap architectures
- Multi-site DR scenarios
- Edge computing deployments

### Pattern 3: GitOps-Driven Platform Lifecycle

Store DisconnectedPlatform manifests in Git, let GitOps tools sync them.

```yaml
# Git repository: environments/prod/disconnected-platform.yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: prod-airgap
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  mode: airgapped
  airgapped:
    managementCluster: true
    mirrorRegistry: "registry.prod.airgap:5000"
    bootstrapEnabled: true
  architect:
    enabled: true
    route:
      tls:
        termination: edge
```

**When to use:**
- Declarative infrastructure management
- Multi-environment promotion (dev → test → prod)
- Audit trails via Git history

### Pattern 4: Automated Cluster Provisioning

Create ClusterBootstrap resources programmatically for IaC workflows.

```python
# Python example using kubernetes client
from kubernetes import client, config
from kubernetes.client.rest import ApiException

config.load_kube_config()
api = client.CustomObjectsApi()

# Create ClusterBootstrap
cluster_bootstrap = {
    "apiVersion": "mirror.mirror.mathianasj.github.com/v1",
    "kind": "ClusterBootstrap",
    "metadata": {"name": "edge-site-42"},
    "spec": {
        "version": "4.17.5",
        "platform": "vsphere",
        "installConfig": {"name": "edge-site-42-config"},
        "mirrorRegistry": "registry.airgap.local:5000/mirror",
        "pullSecret": {"name": "pull-secret"},
        "controlPlane": {"replicas": 3},
        "compute": {"replicas": 3}
    }
}

try:
    api.create_cluster_custom_object(
        group="mirror.mirror.mathianasj.github.com",
        version="v1",
        plural="clusterbootstraps",
        body=cluster_bootstrap
    )
    print(f"Created ClusterBootstrap: edge-site-42")
except ApiException as e:
    print(f"Error: {e}")
```

**When to use:**
- Infrastructure-as-Code platforms (Terraform, Pulumi)
- Self-service cluster provisioning portals
- Automated DR site standup

---

## Watching Resources

### Watch for Collection Completion

```go
import (
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
    "k8s.io/apimachinery/pkg/watch"
)

func monitorCollections(ctx context.Context, client dynamic.Interface) {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "collectionpipelines",
    }
    
    watcher, err := client.Resource(gvr).Watch(ctx, metav1.ListOptions{
        LabelSelector: "team=platform-engineering",
    })
    if err != nil {
        log.Fatal(err)
    }
    
    for event := range watcher.ResultChan() {
        switch event.Type {
        case watch.Added, watch.Modified:
            obj := event.Object.(*unstructured.Unstructured)
            phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
            name := obj.GetName()
            
            switch phase {
            case "Complete":
                log.Printf("Collection %s completed", name)
                onCollectionComplete(obj)
            case "Failed":
                log.Printf("Collection %s failed", name)
                onCollectionFailed(obj)
            }
        }
    }
}
```

### Watch for Import Readiness

```go
func waitForImportReady(ctx context.Context, client dynamic.Interface, importName string) error {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "mirrorimports",
    }
    
    watcher, _ := client.Resource(gvr).Watch(ctx, metav1.SingleObject(metav1.ObjectMeta{Name: importName}))
    
    for event := range watcher.ResultChan() {
        obj := event.Object.(*unstructured.Unstructured)
        conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
        
        for _, cond := range conditions {
            condMap := cond.(map[string]interface{})
            if condMap["type"] == "Ready" && condMap["status"] == "True" {
                return nil  // Import ready
            }
        }
    }
    return errors.New("watch ended without ready condition")
}
```

---

## Creating Resources

### Create DisconnectedPlatform from Template

```go
func createDisconnectedPlatform(client dynamic.Interface, mode string) error {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "disconnectedplatforms",
    }
    
    platform := &unstructured.Unstructured{
        Object: map[string]interface{}{
            "apiVersion": "mirror.mirror.mathianasj.github.com/v1",
            "kind": "DisconnectedPlatform",
            "metadata": map[string]interface{}{
                "name": "auto-generated-platform",
            },
            "spec": map[string]interface{}{
                "mode": mode,
            },
        },
    }
    
    if mode == "connected" {
        unstructured.SetNestedMap(platform.Object, map[string]interface{}{
            "collectionSchedule": "0 2 * * *",
            "mirrorRegistry": "quay.io/myorg/mirror",
            "artifactStorage": map[string]interface{}{
                "size": "500Gi",
            },
        }, "spec", "connected")
    }
    
    _, err := client.Resource(gvr).Create(context.TODO(), platform, metav1.CreateOptions{})
    return err
}
```

### Trigger On-Demand Collection

```go
func triggerCollection(client dynamic.Interface, imageSetConfig string) (string, error) {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "collectionpipelines",
    }
    
    collection := &unstructured.Unstructured{
        Object: map[string]interface{}{
            "apiVersion": "mirror.mirror.mathianasj.github.com/v1",
            "kind": "CollectionPipeline",
            "metadata": map[string]interface{}{
                "generateName": "manual-collection-",
            },
            "spec": map[string]interface{}{
                "triggerType": "manual",
                "imageSetConfig": imageSetConfig,
                "storage": map[string]interface{}{
                    "output": map[string]interface{}{
                        "pvc": "collection-storage",
                    },
                },
            },
        },
    }
    
    result, err := client.Resource(gvr).Create(context.TODO(), collection, metav1.CreateOptions{})
    if err != nil {
        return "", err
    }
    
    return result.GetName(), nil
}
```

---

## Status and Health Checking

### Check Platform Health

```go
func checkPlatformHealth(client dynamic.Interface, platformName string) (bool, error) {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "disconnectedplatforms",
    }
    
    obj, err := client.Resource(gvr).Get(context.TODO(), platformName, metav1.GetOptions{})
    if err != nil {
        return false, err
    }
    
    phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
    if phase != "Ready" {
        return false, fmt.Errorf("platform not ready: %s", phase)
    }
    
    components, _, _ := unstructured.NestedSlice(obj.Object, "status", "components")
    for _, comp := range components {
        compMap := comp.(map[string]interface{})
        if compMap["status"] != "Running" && compMap["status"] != "Succeeded" {
            return false, fmt.Errorf("component %s not healthy: %s", 
                compMap["name"], compMap["status"])
        }
    }
    
    return true, nil
}
```

### Get Latest Collection Version

```go
func getLatestCollectionVersion(client dynamic.Interface, platformName string) (string, error) {
    gvr := schema.GroupVersionResource{
        Group: "mirror.mirror.mathianasj.github.com",
        Version: "v1",
        Resource: "disconnectedplatforms",
    }
    
    obj, err := client.Resource(gvr).Get(context.TODO(), platformName, metav1.GetOptions{})
    if err != nil {
        return "", err
    }
    
    version, found, err := unstructured.NestedString(obj.Object, 
        "status", "lastCollection", "version")
    if !found || err != nil {
        return "", errors.New("no collection version found")
    }
    
    return version, nil
}
```

---

## Best Practices

### 1. Use Labels for Resource Discovery

```yaml
metadata:
  labels:
    app: my-controller
    environment: production
    managed-by: automation
```

Query with label selectors:
```go
listOptions := metav1.ListOptions{
    LabelSelector: "app=my-controller,environment=production",
}
```

### 2. Handle Finalizers Properly

Don't delete resources that have finalizers without understanding their cleanup:

```go
obj, _ := client.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
finalizers := obj.GetFinalizers()
if len(finalizers) > 0 {
    log.Printf("Resource has finalizers: %v - cleanup may be in progress", finalizers)
}
```

### 3. Use Status Conditions

Check conditions instead of just phase:

```go
func isConditionTrue(obj *unstructured.Unstructured, conditionType string) bool {
    conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
    for _, cond := range conditions {
        condMap := cond.(map[string]interface{})
        if condMap["type"] == conditionType && condMap["status"] == "True" {
            return true
        }
    }
    return false
}
```

### 4. Implement Retry Logic

```go
import "k8s.io/apimachinery/pkg/util/wait"

func createWithRetry(client dynamic.Interface, obj *unstructured.Unstructured) error {
    return wait.ExponentialBackoff(wait.Backoff{
        Steps:    5,
        Duration: 1 * time.Second,
        Factor:   2.0,
    }, func() (bool, error) {
        _, err := client.Resource(gvr).Create(context.TODO(), obj, metav1.CreateOptions{})
        if err == nil {
            return true, nil  // Success
        }
        if apierrors.IsAlreadyExists(err) {
            return true, nil  // Already exists is okay
        }
        return false, nil  // Retry
    })
}
```

### 5. Validate Before Creating

```go
func validateCollectionSpec(imageSetConfig string) error {
    // Parse YAML
    var config map[string]interface{}
    if err := yaml.Unmarshal([]byte(imageSetConfig), &config); err != nil {
        return fmt.Errorf("invalid imageSetConfig YAML: %w", err)
    }
    
    // Validate required fields
    if config["apiVersion"] != "mirror.openshift.io/v1alpha2" {
        return errors.New("imageSetConfig must be v1alpha2")
    }
    
    return nil
}
```

---

## Examples

### Example 1: Slack Notifier for Collection Events

```python
# Python controller that sends Slack notifications
import kopf
import requests
import os

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

@kopf.on.field('mirror.mirror.mathianasj.github.com', 'v1', 'collectionpipelines', 
               field='status.phase')
def collection_phase_changed(old, new, name, **kwargs):
    if new == "Complete":
        message = f":white_check_mark: Collection `{name}` completed successfully!"
        version = kwargs.get('status', {}).get('version', 'unknown')
        message += f"\nVersion: `{version}`"
    elif new == "Failed":
        message = f":x: Collection `{name}` failed!"
    else:
        return  # Don't notify for other phases
    
    requests.post(SLACK_WEBHOOK, json={"text": message})
```

### Example 2: Prometheus Metrics Exporter

```go
// Expose mirror-operator metrics
package main

import (
    "context"
    "time"
    
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
    collectionDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "mirror_collection_duration_seconds",
            Help: "Duration of collection operations",
        },
        []string{"name", "status"},
    )
    
    platformPhase = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mirror_platform_phase",
            Help: "Current platform phase (Ready=1, Error=0)",
        },
        []string{"name", "mode"},
    )
)

func init() {
    prometheus.MustRegister(collectionDuration, platformPhase)
}

func collectMetrics(client dynamic.Interface) {
    ticker := time.NewTicker(30 * time.Second)
    for range ticker.C {
        updatePlatformMetrics(client)
        updateCollectionMetrics(client)
    }
}
```

### Example 3: Ansible Integration

```yaml
# Ansible playbook
---
- name: Trigger Mirror Collection
  hosts: localhost
  tasks:
    - name: Create CollectionPipeline
      kubernetes.core.k8s:
        state: present
        definition:
          apiVersion: mirror.mirror.mathianasj.github.com/v1
          kind: CollectionPipeline
          metadata:
            name: "ansible-{{ ansible_date_time.epoch }}"
          spec:
            triggerType: manual
            imageSetConfig: "{{ lookup('file', 'imageset-config.yaml') }}"
            storage:
              output:
                pvc: collection-storage
    
    - name: Wait for Collection to Complete
      kubernetes.core.k8s_info:
        api_version: mirror.mirror.mathianasj.github.com/v1
        kind: CollectionPipeline
        name: "ansible-{{ ansible_date_time.epoch }}"
      register: collection
      until: collection.resources[0].status.phase == "Complete"
      retries: 60
      delay: 30
```

---

## Troubleshooting

### Common Integration Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Resource not found" | CRDs not installed | Run `kubectl get crd \| grep mirror` |
| "Forbidden" errors | Missing RBAC | Create ClusterRole with appropriate permissions |
| Watch disconnects | Long-running watches timeout | Implement reconnect logic with backoff |
| Status never updates | Controller not running | Check mirror-operator pod logs |
| Stale status | Caching issue | Force refresh with `resourceVersion: "0"` |

### Debug Commands

```bash
# Check if CRDs are installed
kubectl get crd | grep mirror.mathianasj.github.com

# Get all DisconnectedPlatforms
kubectl get disconnectedplatforms -A

# Watch CollectionPipeline status
kubectl get collectionpipelines -w

# Get detailed resource status
kubectl get disconnectedplatform my-platform -o jsonpath='{.status}' | jq

# Check operator logs
kubectl logs -n mirror-operator-system -l control-plane=controller-manager
```

---

## Additional Resources

- [CRD Reference](./crd-reference.md) - Complete API documentation
- [Mirror Operator README](../README.md) - Installation and setup
- [Reference Architecture](./reference-architecture-mapping.md) - System design

## Support

For integration support:
- File issues at: https://github.com/mathianasj/mirror-operator/issues
- Include: Kubernetes version, mirror-operator version, integration code snippet
