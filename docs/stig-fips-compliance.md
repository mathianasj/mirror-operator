# DISA STIG / FIPS Compliance for Import Script

## Overview

The `import-airgap-architect.sh` script now supports DISA STIG and FIPS-compliant systems, automatically detecting and adapting to hardened environments while maintaining backward compatibility with standard systems.

## STIG/FIPS Challenges

DISA STIG-hardened RHEL systems implement several security controls that can break standard installation workflows:

### 1. **fapolicyd (File Access Policy Daemon)**
- **Issue**: Blocks execution of binaries that are not RPMs
- **Solution**: Temporarily stop fapolicyd during installation, install tools to `/usr/local/bin`, add to fapolicyd trust list, then restart
- **Commands used**:
  ```bash
  sudo systemctl stop fapolicyd.service
  sudo fapolicyd-cli --file add /usr/local/bin/oc-mirror
  sudo fapolicyd-cli --update
  sudo systemctl start fapolicyd.service
  ```

### 2. **Umask 0077**
- **Issue**: STIG sets umask to `0077` (no group/other permissions), causing issues with file sharing
- **Solution**: Temporarily set umask to `0022` during installation, restore afterward
- **Commands used**:
  ```bash
  umask 0022  # During installation
  umask 0077  # Restore after
  ```

### 3. **max_user_namespaces = 0**
- **Issue**: STIG sets `user.max_user_namespaces=0` which breaks rootless podman
- **Solution**: Increase to `62372` (required for podman)
- **Commands used**:
  ```bash
  sudo sysctl -w user.max_user_namespaces=62372
  echo "user.max_user_namespaces = 62372" | sudo tee /etc/sysctl.d/99-user-namespaces.conf
  ```

### 4. **/home mounted noexec**
- **Issue**: Cannot execute binaries from user home directory
- **Solution**: Install CLI tools to `/usr/local/bin` instead of `~/.local/bin`

### 5. **SELinux Enforcing Mode**
- **Issue**: Binaries need proper security contexts
- **Solution**: Set `bin_t` context on installed executables
- **Commands used**:
  ```bash
  sudo chcon -t bin_t /usr/local/bin/oc-mirror
  sudo chcon -Rt container_file_t /path/to/data
  ```

## Auto-Detection

The script automatically detects STIG mode by checking multiple indicators:

```bash
detect_stig_mode() {
    # Checks for:
    # 1. /home mounted with noexec
    # 2. umask = 0077
    # 3. fapolicyd installed
    # 4. max_user_namespaces = 0 or < 1000
    
    # If 2+ indicators present, STIG mode is enabled
}
```

## Environment Variables

### STIG_MODE
- `auto` (default): Automatically detect STIG environment
- `true`: Force STIG-compliant mode
- `false`: Disable STIG mode

```bash
# Force STIG mode
STIG_MODE=true ./import-airgap-architect.sh start

# Disable STIG mode (not recommended on hardened systems)
STIG_MODE=false ./import-airgap-architect.sh start
```

### CLI_INSTALL_DIR
- Standard systems: `~/.local/bin` (default)
- STIG systems: `/usr/local/bin` (default, requires sudo)

```bash
# Custom installation directory
CLI_INSTALL_DIR=/opt/ocp-tools ./import-airgap-architect.sh start
```

## STIG Mode Workflow

When STIG mode is detected or enabled:

### 1. Preparation Phase
```
prepare_stig_environment()
├── set_install_umask()          # Save original umask, set to 0022
├── fix_user_namespaces()        # Increase max_user_namespaces to 62372
└── disable_fapolicyd()          # Stop fapolicyd service
```

### 2. Installation Phase
```
install_cli_tools()
├── Extract binaries to temp directory
├── Copy to /usr/local/bin with sudo
├── Set ownership to root:root
├── Set permissions to 0755
├── Set SELinux context (bin_t)
└── configure_fapolicyd_trust()  # Add to fapolicyd allowlist
```

### 3. Cleanup Phase
```
cleanup_stig_environment()
├── configure_fapolicyd_trust()  # Update fapolicyd database
├── enable_fapolicyd()           # Restart fapolicyd service
└── restore_umask()              # Restore original umask
```

## Tools Installed to /usr/local/bin

In STIG mode, the following tools are installed to `/usr/local/bin` and added to fapolicyd trust:

1. `oc` - OpenShift CLI
2. `kubectl` - Kubernetes CLI
3. `openshift-install` - OpenShift installer
4. `oc-mirror` - Mirror tool for disconnected environments
5. `mirror-registry` - Quay registry installer

## Permissions Required

STIG mode requires sudo access for:

- Creating `/usr/local/bin` directory (if it doesn't exist)
- Copying binaries to `/usr/local/bin`
- Setting file ownership to `root:root`
- Setting SELinux contexts
- Stopping/starting fapolicyd service
- Adding files to fapolicyd trust
- Modifying sysctl settings for user namespaces

The script will prompt for sudo password when needed.

## Testing STIG Mode

### On a Standard System
```bash
# Force STIG mode for testing
STIG_MODE=true ./import-airgap-architect.sh start
```

### On a STIG-Hardened System
```bash
# Auto-detection (recommended)
./import-airgap-architect.sh start

# Check what was detected
grep "STIG mode" /var/log/messages
```

## Troubleshooting

### fapolicyd Blocks Execution
```bash
# Check fapolicyd status
sudo systemctl status fapolicyd.service

# Check if tool is trusted
sudo fapolicyd-cli --list | grep oc-mirror

# Manually add to trust
sudo fapolicyd-cli --file add /usr/local/bin/oc-mirror
sudo fapolicyd-cli --update
```

### Podman Fails with User Namespace Error
```bash
# Check current setting
cat /proc/sys/user/max_user_namespaces

# Should be >= 62372 for podman
# If not, run:
sudo sysctl -w user.max_user_namespaces=62372
```

### Permission Denied Errors
```bash
# Check umask
umask

# Should be 0022 during installation
# STIG default is 0077

# Temporarily set for installation
umask 0022
./import-airgap-architect.sh start
umask 0077  # Restore
```

### SELinux Denials
```bash
# Check for denials
sudo ausearch -m avc -ts recent

# Set correct context on binaries
sudo chcon -t bin_t /usr/local/bin/oc-mirror

# Set correct context on data directory
sudo chcon -Rt container_file_t /path/to/data
```

## Compatibility Matrix

| Environment | Auto-Detect | CLI Install Dir | Requires Sudo | fapolicyd Handling |
|-------------|-------------|-----------------|---------------|-------------------|
| Standard RHEL | No | `~/.local/bin` | No | Not applicable |
| STIG RHEL | Yes | `/usr/local/bin` | Yes | Stop/trust/start |
| FIPS RHEL | Yes | `/usr/local/bin` | Yes | Stop/trust/start |
| Custom (noexec /home) | Yes | `/usr/local/bin` | Yes | If installed |

## References

- [OpenShift Disconnected Installation Cheat Sheet](https://developers.redhat.com/articles/2024/05/28/openshift-disconnected-installation-cheat-sheet)
- [DISA STIG for RHEL 8](https://www.stigviewer.com/stig/red_hat_enterprise_linux_8/)
- [Red Hat OpenShift STIG Documentation](https://docs.openshift.com/container-platform/4.15/security/compliance/index.html)
- [fapolicyd Documentation](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/8/html/security_hardening/assembly_blocking-and-allowing-applications-using-fapolicyd_security-hardening)

## Known Issues

1. **Mirror-registry installation may take longer in STIG mode** due to additional security checks
2. **Some container images may need additional SELinux labels** - use `chcon -Rt container_file_t` on data directories
3. **Rootless podman may still have issues** if additional STIG controls are applied beyond the ones handled by this script

## Future Enhancements

- [ ] Add support for alternative registries (Harbor, Artifactory)
- [ ] Implement fapolicyd rules file for permanent trust configuration
- [ ] Add compliance reporting/verification mode
- [ ] Support for running entirely in temporary filesystem (when STIG prevents persistent storage)
