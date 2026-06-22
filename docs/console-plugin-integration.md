# OpenShift Console Plugin Integration

This document describes how to deploy the Airgap Architect as an OpenShift Console Plugin using the mirror-operator.

## Overview

The mirror-operator can deploy the Airgap Architect UI in three modes:

1. **Standalone Mode**: Separate frontend deployment with its own route (default)
2. **Console Plugin Mode**: Integrated into OpenShift Console as a dynamic plugin ⭐ NEW
3. **Hybrid Mode**: Both standalone AND console plugin enabled simultaneously ⭐ NEW

## Architecture

### Standalone Mode (Default)

```
┌─────────────────────────────────────────┐
│         OpenShift Cluster              │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │  Airgap Architect Frontend       │  │
│  │  (Vite/React on port 5173)       │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│  ┌──────────────▼───────────────────┐  │
│  │  Airgap Architect Backend        │  │
│  │  (Node.js API on port 4000)      │  │
│  └──────────────────────────────────┘  │
│                                         │
│  Routes:                                │
│  - airgap-architect.apps...            │
│  - airgap-architect-api.apps...        │
└─────────────────────────────────────────┘
```

### Console Plugin Mode

```
┌─────────────────────────────────────────┐
│         OpenShift Cluster              │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │  OpenShift Console               │  │
│  │  (loads dynamic plugins)         │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│  ┌──────────────▼───────────────────┐  │
│  │  Airgap Architect Plugin         │  │
│  │  (nginx serving static files)    │  │
│  │  Port: 9001                       │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│  ┌──────────────▼───────────────────┐  │
│  │  Airgap Architect Backend        │  │
│  │  (Node.js API on port 4000)      │  │
│  └──────────────────────────────────┘  │
│                                         │
│  ConsolePlugin CR:                      │
│  - airgap-architect-plugin             │
│                                         │
│  Route (backend only):                  │
│  - airgap-architect-api.apps...        │
└─────────────────────────────────────────┘
```

### Hybrid Mode (Both Enabled)

```
┌─────────────────────────────────────────┐
│         OpenShift Cluster              │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │  OpenShift Console               │  │
│  │  (loads dynamic plugins)         │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│  ┌──────────────▼───────────────────┐  │
│  │  Airgap Architect Plugin         │◄─┼─── Console access
│  │  (nginx serving static files)    │  │
│  │  Port: 9001                       │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│  ┌──────────────────────────────────┐  │
│  │  Airgap Architect Frontend       │◄─┼─── Route access
│  │  (Vite/React on port 5173)       │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│                 ▼                       │
│  ┌──────────────────────────────────┐  │
│  │  Airgap Architect Backend        │◄─┼─── Shared by both UIs
│  │  (Node.js API on port 4000)      │  │
│  └──────────────────────────────────┘  │
│                                         │
│  Routes:                                │
│  - airgap-architect.apps...            │
│  - airgap-architect-api.apps...        │
│                                         │
│  ConsolePlugin CR:                      │
│  - airgap-architect-plugin             │
└─────────────────────────────────────────┘

Two ways to access the same data:
1. Direct Route → Full standalone UI
2. Console → Embedded plugin UI
```

## Deployment

### Prerequisites

- OpenShift 4.10+ cluster
- mirror-operator deployed
- Container images available:
  - `quay.io/mirror-operator/openshift-airgap-architect-backend:latest`
  - `quay.io/mirror-operator/openshift-airgap-architect-console-plugin:latest`

### Standalone Mode (Default)

Deploy with just the standalone frontend:

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: my-platform
  namespace: mirror-operator-system
spec:
  mode: connected
  architect:
    enabled: true
    # frontendImage defaults to operator flag value
    # backendImage defaults to operator flag value
```

### Console Plugin Mode Only

Deploy only the console plugin (no standalone frontend):

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: my-platform
  namespace: mirror-operator-system
spec:
  mode: connected
  architect:
    enabled: true
    backendImage: "quay.io/mirror-operator/openshift-airgap-architect-backend:latest"
    consolePlugin:
      enabled: true
      image: "quay.io/mirror-operator/openshift-airgap-architect-console-plugin:latest"
```

### Hybrid Mode (Both Enabled)

Deploy both standalone frontend AND console plugin:

```yaml
apiVersion: mirror.mirror.mathianasj.github.com/v1
kind: DisconnectedPlatform
metadata:
  name: my-platform
  namespace: mirror-operator-system
spec:
  mode: connected
  architect:
    enabled: true
    frontendImage: "quay.io/mirror-operator/openshift-airgap-architect-frontend:latest"
    backendImage: "quay.io/mirror-operator/openshift-airgap-architect-backend:latest"
    consolePlugin:
      enabled: true
      image: "quay.io/mirror-operator/openshift-airgap-architect-console-plugin:latest"
```

This gives you **two ways to access the UI**:
- **Route**: `https://airgap-architect.apps.<cluster-domain>` → Standalone UI
- **Console**: Administrator → Airgap Architect → Embedded plugin UI

Both UIs share the same backend API and data.

Apply the configuration:

```bash
oc apply -f my-platform.yaml
```

The operator will deploy based on the mode:
- **Standalone**: Backend + Frontend + Routes
- **Console Plugin**: Backend + Plugin + ConsolePlugin CR
- **Hybrid**: Backend + Frontend + Plugin + Routes + ConsolePlugin CR

### Enable Plugin in OpenShift Console

After the operator creates the plugin, enable it in the Console operator:

```bash
oc patch console.operator.openshift.io cluster \
  --type='json' \
  -p='[{"op": "add", "path": "/spec/plugins/-", "value": "airgap-architect-plugin"}]'
```

Verify the plugin is registered:

```bash
oc get consoleplugin airgap-architect-plugin
```

### Access the Plugin

1. Open your OpenShift Console in a browser
2. **Force refresh** the page (Ctrl+Shift+R / Cmd+Shift+R)
3. Navigate to **Administrator** → **Airgap Architect**

The plugin will appear in the navigation menu under the Administrator perspective.

## Deployment Logic

The operator determines which components to deploy based on the configuration:

| frontendImage Set? | consolePlugin.enabled? | Components Deployed |
|-------------------|------------------------|---------------------|
| No (default) | No (default) | Backend + Frontend (standalone mode) |
| No | Yes | Backend + Plugin (console plugin only) |
| Yes | No | Backend + Frontend (standalone mode) |
| Yes | Yes | Backend + Frontend + Plugin (hybrid mode) |

**Key Points:**
- Backend is **always** deployed (both UIs need it)
- Frontend is deployed when `frontendImage` is set OR when `consolePlugin` is not enabled
- Plugin is deployed only when `consolePlugin.enabled = true`
- Setting both enables **hybrid mode** with dual access

## Configuration Options

### Using Custom Images

```yaml
architect:
  enabled: true
  backendImage: "quay.io/my-org/airgap-architect-backend:v1.2.3"
  consolePlugin:
    enabled: true
    image: "quay.io/my-org/airgap-architect-plugin:v1.2.3"
```

### Custom Backend Route

```yaml
architect:
  enabled: true
  route:
    host: "airgap-api.apps.example.com"
    tls:
      termination: edge
  consolePlugin:
    enabled: true
```

### Scaling

```yaml
architect:
  enabled: true
  replicas: 3  # Scales both backend and plugin
  consolePlugin:
    enabled: true
```

## Switching Between Modes

### From Standalone to Console Plugin

```yaml
architect:
  enabled: true
  # frontendImage is ignored when consolePlugin.enabled = true
  consolePlugin:
    enabled: true  # Add this
```

The operator will automatically:
- Delete standalone frontend Deployment, Service, and Route
- Deploy console plugin Deployment and Service
- Create ConsolePlugin CR

### From Console Plugin to Standalone

```yaml
architect:
  enabled: true
  frontendImage: "quay.io/mirror-operator/openshift-airgap-architect-frontend:latest"
  consolePlugin:
    enabled: false  # Or remove this section
```

The operator will automatically:
- Delete console plugin Deployment, Service, and ConsolePlugin CR
- Deploy standalone frontend Deployment, Service, and Route

## Resources Created

### Console Plugin Mode

| Resource | Name | Namespace | Purpose |
|----------|------|-----------|---------|
| Deployment | `airgap-architect-plugin` | `openshift-airgap-architect` | Serves plugin static files |
| Service | `airgap-architect-plugin` | `openshift-airgap-architect` | ClusterIP on port 9001 |
| ConsolePlugin | `airgap-architect-plugin` | (cluster-scoped) | Registers plugin with Console |
| Deployment | `airgap-architect-backend` | `openshift-airgap-architect` | Backend API |
| Service | `airgap-architect-backend` | `openshift-airgap-architect` | ClusterIP on port 4000 |
| Route | `airgap-architect-api` | `openshift-airgap-architect` | Backend API route |

### Standalone Mode

| Resource | Name | Namespace | Purpose |
|----------|------|-----------|---------|
| Deployment | `airgap-architect-frontend` | `openshift-airgap-architect` | Standalone frontend UI |
| Service | `airgap-architect-frontend` | `openshift-airgap-architect` | ClusterIP on port 5173 |
| Route | `airgap-architect` | `openshift-airgap-architect` | Frontend route |
| Deployment | `airgap-architect-backend` | `openshift-airgap-architect` | Backend API |
| Service | `airgap-architect-backend` | `openshift-airgap-architect` | ClusterIP on port 4000 |
| Route | `airgap-architect-api` | `openshift-airgap-architect` | Backend API route |

## Troubleshooting

### Plugin Not Showing in Console

1. **Check if ConsolePlugin CR exists:**
   ```bash
   oc get consoleplugin airgap-architect-plugin
   ```

2. **Check if plugin is enabled in Console operator:**
   ```bash
   oc get console.operator.openshift.io cluster -o jsonpath='{.spec.plugins}' | jq
   ```
   Should include `"airgap-architect-plugin"`.

3. **Check plugin pod is running:**
   ```bash
   oc get pods -n openshift-airgap-architect -l app.kubernetes.io/component=console-plugin
   ```

4. **Check plugin service:**
   ```bash
   oc get svc airgap-architect-plugin -n openshift-airgap-architect
   ```

5. **Test plugin manifest:**
   ```bash
   oc exec -n openshift-airgap-architect deployment/airgap-architect-plugin -- curl http://localhost:9001/plugin-manifest.json
   ```

### Plugin Crashes or Won't Start

1. **Check pod logs:**
   ```bash
   oc logs -n openshift-airgap-architect deployment/airgap-architect-plugin
   ```

2. **Check pod events:**
   ```bash
   oc describe pod -n openshift-airgap-architect -l app.kubernetes.io/component=console-plugin
   ```

3. **Verify image pull:**
   ```bash
   oc get pods -n openshift-airgap-architect -l app.kubernetes.io/component=console-plugin -o jsonpath='{.items[0].status.containerStatuses[0].state}'
   ```

### Backend Not Reachable from Plugin

1. **Check backend service:**
   ```bash
   oc get svc airgap-architect-backend -n openshift-airgap-architect
   ```

2. **Test backend from plugin pod:**
   ```bash
   POD=$(oc get pod -n openshift-airgap-architect -l app.kubernetes.io/component=console-plugin -o name | head -1)
   oc exec -n openshift-airgap-architect $POD -- curl http://airgap-architect-backend:4000/api/health
   ```

3. **Check backend logs:**
   ```bash
   oc logs -n openshift-airgap-architect deployment/airgap-architect-backend
   ```

### Clear Console Cache

If the plugin appears but looks broken:

1. **Hard refresh** browser: Ctrl+Shift+R (Windows/Linux) or Cmd+Shift+R (Mac)
2. **Clear browser cache** for the OpenShift Console domain
3. **Restart Console pods** (forces Console to reload plugins):
   ```bash
   oc delete pods -n openshift-console -l app=console
   ```

## Operator Configuration

The operator supports configuring default images via command-line flags:

```bash
--architect-backend-image=quay.io/mirror-operator/openshift-airgap-architect-backend:latest
--architect-frontend-image=quay.io/mirror-operator/openshift-airgap-architect-frontend:latest
--architect-console-plugin-image=quay.io/mirror-operator/openshift-airgap-architect-console-plugin:latest
```

These defaults are used when the corresponding `image` fields are not specified in the CR.

## Next Steps

After deploying the console plugin:

1. Navigate to **Administrator** → **Airgap Architect** in OpenShift Console
2. Use the wizard to create ImageSetConfigurations
3. Configure collection schedules
4. Monitor mirror bundles

For more information on using the Airgap Architect UI, see:
- [openshift-airgap-architect README](https://github.com/mathianasj/openshift-airgap-architect)
- [Console Plugin DEPLOYMENT.md](https://github.com/mathianasj/openshift-airgap-architect/blob/main/console-plugin/DEPLOYMENT.md)
