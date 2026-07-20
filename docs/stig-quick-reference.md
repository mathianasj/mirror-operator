# STIG/FIPS Quick Reference Guide

## Quick Start - STIG Systems

```bash
# Standard auto-detection (recommended)
./import-airgap-architect.sh start

# The script will automatically:
# ✓ Detect STIG environment
# ✓ Request sudo when needed
# ✓ Stop fapolicyd temporarily
# ✓ Install tools to /usr/local/bin
# ✓ Configure fapolicyd trust
# ✓ Fix podman user namespaces
# ✓ Restart fapolicyd
```

## What to Expect on STIG Systems

### During Installation

You will see messages like:

```
=== DISA STIG / FIPS Mode Detected ===
Applying compliance-friendly configuration...

STIG mode: Set umask to 0022 for installation (was 0077)
STIG mode: Increasing max_user_namespaces for podman (currently 0)...
✓ Set user.max_user_namespaces=62372
STIG mode: Stopping fapolicyd temporarily...
✓ fapolicyd stopped

Installing CLI tools to /usr/local/bin...
[sudo] password for user:  # <-- Enter your password here
```

### What Gets Installed

Tools installed to `/usr/local/bin` (requires sudo):
- `oc`
- `kubectl`
- `openshift-install`
- `oc-mirror`
- `mirror-registry`

### After Installation

```
=== Restoring STIG Environment ===
STIG mode: Adding tools to fapolicyd trust list...
  Added to fapolicyd trust: /usr/local/bin/oc
  Added to fapolicyd trust: /usr/local/bin/oc-mirror
  Added to fapolicyd trust: /usr/local/bin/openshift-install
  Added to fapolicyd trust: /usr/local/bin/mirror-registry
✓ fapolicyd trust database updated
STIG mode: Restarting fapolicyd...
✓ fapolicyd restarted
```

## Common Issues & Solutions

### 1. "fapolicyd blocks execution"

**Symptom**: Tools fail to run with permission denied

**Solution**:
```bash
# Add tool to trust manually
sudo fapolicyd-cli --file add /usr/local/bin/oc-mirror
sudo fapolicyd-cli --update
sudo systemctl restart fapolicyd.service
```

### 2. "Podman fails with user namespace error"

**Symptom**: `podman: cannot set up namespace using newuidmap: Invalid argument`

**Solution**:
```bash
# Check current setting
cat /proc/sys/user/max_user_namespaces

# Fix if < 62372
sudo sysctl -w user.max_user_namespaces=62372

# Make persistent
echo "user.max_user_namespaces = 62372" | sudo tee /etc/sysctl.d/99-user-namespaces.conf
```

### 3. "Permission denied creating files"

**Symptom**: Cannot write to directories

**Solution**:
```bash
# Check umask
umask  # Should be 0022 during installation

# Temporarily fix
umask 0022
./import-airgap-architect.sh start

# Restore STIG umask after
umask 0077
```

### 4. "SELinux denials"

**Symptom**: AVC denials in audit log

**Solution**:
```bash
# Check denials
sudo ausearch -m avc -ts recent

# Fix binary contexts
sudo chcon -t bin_t /usr/local/bin/oc-mirror

# Fix data directory contexts
sudo chcon -Rt container_file_t /path/to/bundle/data
```

## Manual STIG Preparation (if auto-detection fails)

If the script doesn't detect STIG mode automatically:

```bash
# 1. Stop fapolicyd
sudo systemctl stop fapolicyd.service

# 2. Fix user namespaces
sudo sysctl -w user.max_user_namespaces=62372

# 3. Set umask
umask 0022

# 4. Force STIG mode
STIG_MODE=true ./import-airgap-architect.sh start

# 5. After installation, add tools to fapolicyd trust
sudo fapolicyd-cli --file add /usr/local/bin/oc
sudo fapolicyd-cli --file add /usr/local/bin/oc-mirror
sudo fapolicyd-cli --file add /usr/local/bin/openshift-install
sudo fapolicyd-cli --file add /usr/local/bin/mirror-registry
sudo fapolicyd-cli --update

# 6. Restart fapolicyd
sudo systemctl start fapolicyd.service

# 7. Restore umask
umask 0077
```

## Verification Commands

### Check if STIG mode was detected
```bash
# Run with dry-run to see detection
STIG_MODE=auto ./import-airgap-architect.sh start 2>&1 | grep -i stig
```

### Verify tools are installed
```bash
ls -la /usr/local/bin/{oc,oc-mirror,openshift-install,mirror-registry}
```

### Verify fapolicyd trust
```bash
sudo fapolicyd-cli --list | grep -E "(oc|mirror-registry)"
```

### Verify user namespaces
```bash
cat /proc/sys/user/max_user_namespaces  # Should be >= 62372
```

### Verify podman works
```bash
podman run --rm registry.access.redhat.com/ubi9/ubi:latest echo "Podman OK"
```

## Environment Variable Overrides

### Force STIG mode ON
```bash
STIG_MODE=true ./import-airgap-architect.sh start
```

### Force STIG mode OFF
```bash
STIG_MODE=false ./import-airgap-architect.sh start
```

### Custom CLI install directory
```bash
# Install to /opt instead of /usr/local/bin
CLI_INSTALL_DIR=/opt/ocp-tools ./import-airgap-architect.sh start
```

### Skip registry installation (STIG systems may have existing registry)
```bash
MIRROR_REGISTRY_INSTALL=false ./import-airgap-architect.sh start
```

### Non-interactive mode with existing registry
```bash
MIRROR_REGISTRY_EXISTING=true \
EXISTING_REGISTRY_URL=registry.example.com:8443 \
EXISTING_REGISTRY_USERNAME=admin \
EXISTING_REGISTRY_PASSWORD=mypassword \
./import-airgap-architect.sh start
```

## STIG Compliance Checklist

Before running the import script on a STIG system:

- [ ] Have sudo access
- [ ] Know the sudo password
- [ ] fapolicyd service is installed/running
- [ ] `/usr/local/bin` exists or can be created
- [ ] Sufficient disk space in `/usr/local` (at least 500MB)
- [ ] Podman is installed
- [ ] SELinux is enforcing (check with `getenforce`)

After running the import script:

- [ ] Tools are in `/usr/local/bin`
- [ ] Tools are in fapolicyd trust list
- [ ] `max_user_namespaces >= 62372`
- [ ] Podman can run containers
- [ ] Mirror-registry is running (if installed)
- [ ] fapolicyd service is running

## Security Notes

### What the script modifies (temporarily):
- **umask**: Changed to `0022` during installation, restored after
- **fapolicyd**: Stopped during installation, restarted after

### What the script modifies (permanently):
- **user.max_user_namespaces**: Increased to `62372` (required for podman)
- **/usr/local/bin**: CLI tools installed with `root:root` ownership
- **fapolicyd trust**: Tools added to allowlist

### What the script does NOT modify:
- System-wide STIG policies
- SELinux booleans or policies
- Firewall rules
- SSH configurations
- Audit rules
- Other STIG controls

## Getting Help

If you encounter issues:

1. Check the [full STIG/FIPS documentation](./stig-fips-compliance.md)
2. Review audit logs: `sudo ausearch -m avc -ts recent`
3. Check fapolicyd logs: `sudo journalctl -u fapolicyd.service`
4. Verify SELinux: `sudo sestatus -v`
5. File an issue with logs from: `./import-airgap-architect.sh start 2>&1 | tee install.log`
