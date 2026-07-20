#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_DIR="${BUNDLE_DIR:-${SCRIPT_DIR}}"

FRONTEND_IMAGE_TAR="${BUNDLE_DIR}/airgap-architect-frontend.tar.gz"
BACKEND_IMAGE_TAR="${BUNDLE_DIR}/airgap-architect-backend.tar.gz"

FRONTEND_IMAGE="localhost/openshift-airgap-architect-frontend:latest"
BACKEND_IMAGE="localhost/openshift-airgap-architect-backend:latest"

DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/data}"

# CLI tools configuration
CLI_TOOLS_DIR="${BUNDLE_DIR}/cli-tools"

# STIG/FIPS compliance settings
STIG_MODE="${STIG_MODE:-auto}"  # "auto" | "true" | "false"
STIG_DETECTED=false
FAPOLICYD_WAS_ACTIVE=false
ORIGINAL_UMASK=""

# Detect if /home is mounted noexec (DISA STIG FIPS mode)
detect_home_noexec() {
    if mount | grep -E "^.* on ${HOME%/*} " | grep -q noexec; then
        return 0  # noexec detected
    fi
    return 1  # no noexec
}

# Detect STIG-hardened system
detect_stig_mode() {
    local indicators=0

    # Check for noexec on /home
    if detect_home_noexec; then
        ((indicators++))
    fi

    # Check umask (STIG sets to 0077)
    local current_umask=$(umask)
    if [ "${current_umask}" = "0077" ]; then
        ((indicators++))
    fi

    # Check if fapolicyd is installed
    if command -v fapolicyd &>/dev/null || systemctl list-unit-files | grep -q fapolicyd; then
        ((indicators++))
    fi

    # Check max_user_namespaces (STIG sets to 0)
    if [ -f /proc/sys/user/max_user_namespaces ]; then
        local max_ns=$(cat /proc/sys/user/max_user_namespaces 2>/dev/null || echo "0")
        if [ "${max_ns}" -eq 0 ] || [ "${max_ns}" -lt 1000 ]; then
            ((indicators++))
        fi
    fi

    # If 2 or more indicators, likely STIG system
    if [ ${indicators} -ge 2 ]; then
        return 0
    fi
    return 1
}

# Apply STIG mode detection
if [ "${STIG_MODE}" = "auto" ]; then
    if detect_stig_mode; then
        STIG_DETECTED=true
        STIG_MODE="true"
    else
        STIG_MODE="false"
    fi
elif [ "${STIG_MODE}" = "true" ]; then
    STIG_DETECTED=true
fi

# Skip ~/.local/bin if /home is noexec or STIG mode
if detect_home_noexec || [ "${STIG_DETECTED}" = true ]; then
    # In STIG mode, prefer /usr/local/bin (requires sudo but works with fapolicyd)
    CLI_INSTALL_DIR="${CLI_INSTALL_DIR:-/usr/local/bin}"
    NOEXEC_DETECTED=true
else
    CLI_INSTALL_DIR="${CLI_INSTALL_DIR:-${HOME}/.local/bin}"
    NOEXEC_DETECTED=false
fi

# Mirror-registry configuration
MIRROR_REGISTRY_INSTALL="${MIRROR_REGISTRY_INSTALL:-prompt}"  # "prompt" | "true" | "false"
MIRROR_REGISTRY_DATA_PATH="${MIRROR_REGISTRY_DATA_PATH:-}"
MIRROR_REGISTRY_HOSTNAME="${MIRROR_REGISTRY_HOSTNAME:-}"
MIRROR_REGISTRY_PORT="${MIRROR_REGISTRY_PORT:-}"
MIRROR_REGISTRY_INIT_PASSWORD="${MIRROR_REGISTRY_INIT_PASSWORD:-}"
MIRROR_REGISTRY_SSL_CERT="${MIRROR_REGISTRY_SSL_CERT:-}"  # Path to SSL certificate file
MIRROR_REGISTRY_SSL_KEY="${MIRROR_REGISTRY_SSL_KEY:-}"    # Path to SSL key file
MIRROR_REGISTRY_EXISTING="${MIRROR_REGISTRY_EXISTING:-false}"
EXISTING_REGISTRY_URL="${EXISTING_REGISTRY_URL:-}"
EXISTING_REGISTRY_USERNAME="${EXISTING_REGISTRY_USERNAME:-}"
EXISTING_REGISTRY_PASSWORD="${EXISTING_REGISTRY_PASSWORD:-}"

usage() {
    cat <<EOF
Import and run Airgap Architect containers for airgapped environments.

Usage:
    $0 [command] [options]

Commands:
    start                   Start Airgap Architect containers (imports images if needed) [default]
    stop                    Stop Airgap Architect containers
    restart                 Restart Airgap Architect containers
    status                  Show status of Airgap Architect containers
    logs                    Show logs from containers
    mirror                  Mirror images from archives to registry using oc-mirror
    clean                   Remove containers and images (preserves data)
    uninstall-mirror-registry  Uninstall mirror-registry (Quay) from this host

Options:
    --bundle-dir DIR    Directory containing image tarballs (default: script directory)
    --data-dir DIR      Directory for persistent data (default: ~/.local/share/airgap-architect)
    --help              Show this help message

Environment Variables:
    BUNDLE_DIR                    Same as --bundle-dir
    DATA_DIR                      Same as --data-dir
    CLI_INSTALL_DIR               Directory for CLI tools (default: ~/.local/bin, /usr/local/bin in STIG mode)
    STIG_MODE                     "auto" (detect), "true" (force), "false" (disable) [default: auto]
    MIRROR_REGISTRY_INSTALL       "prompt" (interactive), "true" (auto-install), "false" (skip) [default: prompt]
    MIRROR_REGISTRY_HOSTNAME      Hostname for Quay (prompted if not set)
    MIRROR_REGISTRY_PORT          Port for Quay (prompted if not set, default: 8443)
    MIRROR_REGISTRY_DATA_PATH     Storage path for Quay data (prompted if not set, default: /opt/quay)
    MIRROR_REGISTRY_INIT_PASSWORD Initial admin password (prompted if not set, auto-generated if empty)
    MIRROR_REGISTRY_SSL_CERT      Path to custom SSL certificate file (optional, prompted if not set)
    MIRROR_REGISTRY_SSL_KEY       Path to custom SSL key file (optional, prompted if not set)
    MIRROR_REGISTRY_EXISTING      Use existing registry instead of installing (default: false)
    EXISTING_REGISTRY_URL         URL of existing registry (e.g., registry.example.com:8443)
    EXISTING_REGISTRY_USERNAME    Username for existing registry
    EXISTING_REGISTRY_PASSWORD    Password for existing registry
    MOCK_MODE                     Set to 'true' to enable mock mode (default: false)
    FEEDBACK_MODE                 Feedback mode: 'github' or 'issue-tracker' (default: github)
    APP_REPO                      GitHub repository for feedback (default: bstrauss84/openshift-airgap-architect)
    APP_BRANCH                    Branch for feedback (default: main)
    VITE_ALLOWED_HOSTS            Comma-separated hostnames for the UI (default: empty)

Examples:
    # Interactive mode (prompts for registry configuration)
    $0 start

    # Auto-install mirror-registry with custom config
    MIRROR_REGISTRY_INSTALL=true \\
    MIRROR_REGISTRY_HOSTNAME=bastion.lab.local \\
    MIRROR_REGISTRY_PORT=8443 \\
    MIRROR_REGISTRY_INIT_PASSWORD=SecurePass123 \\
    $0 start

    # Install with custom SSL certificates
    MIRROR_REGISTRY_INSTALL=true \\
    MIRROR_REGISTRY_HOSTNAME=registry.example.com \\
    MIRROR_REGISTRY_SSL_CERT=/path/to/cert.pem \\
    MIRROR_REGISTRY_SSL_KEY=/path/to/key.pem \\
    $0 start

    # Use existing registry (non-interactive)
    MIRROR_REGISTRY_EXISTING=true \\
    EXISTING_REGISTRY_URL=registry.example.com:8443 \\
    EXISTING_REGISTRY_USERNAME=admin \\
    EXISTING_REGISTRY_PASSWORD=mypassword \\
    $0 start

    # Skip registry setup entirely
    MIRROR_REGISTRY_INSTALL=false $0 start

    # Install CLI tools to custom directory
    CLI_INSTALL_DIR=/opt/ocp-tools $0 start

    # DISA STIG / FIPS mode (auto-detected)
    # The script automatically detects STIG-hardened systems and:
    #   - Installs CLI tools to /usr/local/bin (requires sudo)
    #   - Temporarily stops/starts fapolicyd
    #   - Adds tools to fapolicyd trust list
    #   - Fixes max_user_namespaces for podman
    #   - Adjusts umask during installation
    # To force STIG mode on or off:
    STIG_MODE=true $0 start
    STIG_MODE=false $0 start

    # Use custom data directory
    DATA_DIR=/mnt/large-drive/architect $0 start

    # View logs
    $0 logs

    # Stop and clean up
    $0 stop
    $0 clean

After starting, the Airgap Architect UI will be available at:
    http://localhost:5173

The backend API will be available at:
    http://localhost:4000

EOF
}

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

# STIG/FIPS compliance functions

# Global variable for sudo keepalive process
SUDO_KEEPER_PID=""

# Cleanup function on exit
cleanup_sudo() {
    stop_sudo_keepalive
}

# Register cleanup on exit
trap cleanup_sudo EXIT

# Prompt user to confirm sudo operation
# Usage: confirm_sudo "description" "what it changes"
confirm_sudo() {
    local description="$1"
    local what_changes="$2"

    log ""
    log "Sudo operation required:"
    log "  What: ${description}"
    log "  Changes: ${what_changes}"
    echo -n "Proceed with sudo? [Y/n]: "
    read -r response

    case "${response}" in
        n|N|no|No|NO)
            log "Skipped by user"
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

# Wrapper for sudo - just uses regular sudo
# Usage: run_sudo <command> [args...]
run_sudo() {
    sudo "$@"
}

# Execute multiple commands under a single sudo session
# Usage: run_sudo_batch <<'EOF'
#   command1
#   command2
#   command3
# EOF
run_sudo_batch() {
    sudo bash
}

# Request and cache sudo credentials upfront in STIG mode
request_sudo_access() {
    if [ "${STIG_DETECTED}" != "true" ]; then
        return 0
    fi

    log ""
    log "=== Sudo Access Required ==="
    log "STIG mode requires sudo for the following operations:"
    log "  • Installing CLI tools to /usr/local/bin"
    log "  • Managing fapolicyd service"
    log "  • Configuring user namespaces for podman"
    log "  • Setting SELinux contexts"
    log ""

    # Check if sudo caching is disabled (STIG requirement)
    local timeout_setting=$(sudo grep -E "^Defaults.*timestamp_timeout" /etc/sudoers /etc/sudoers.d/* 2>/dev/null | grep "timestamp_timeout=0" || true)

    if [ -n "${timeout_setting}" ]; then
        log "⚠ DETECTED: Sudo credential caching is DISABLED (DISA STIG requirement)"
        log "  Setting: timestamp_timeout=0"
        log ""
        log "This means you will be prompted for your password for EACH sudo operation."
        log "This is a security requirement and cannot be bypassed."
        log ""
        log "Estimated number of password prompts: 10-15"
        log ""
        read -p "Continue with multiple password prompts? [Y/n]: " continue_choice

        if [ "${continue_choice}" = "n" ] || [ "${continue_choice}" = "N" ]; then
            log "Installation cancelled by user"
            exit 0
        fi

        log ""
        log "Note: Each sudo operation will prompt for your password."
    else
        log "You will be prompted for your sudo password when needed."
        log "Your password will be cached for subsequent operations."
        log ""
    fi

    # Validate sudo access
    if ! sudo -v; then
        error "Failed to obtain sudo access"
        return 1
    fi

    log "✓ Sudo access granted"
    log ""

    # Only start keepalive if caching is enabled
    if [ -z "${timeout_setting}" ]; then
        # Start a background process to keep sudo alive
        (
            while true; do
                sleep 30
                sudo -n -v 2>/dev/null || exit
            done
        ) &
        SUDO_KEEPER_PID=$!
        sleep 1
    else
        log "Note: Sudo credential caching is disabled - keepalive not started"
        log ""
    fi

    return 0
}

# Stop the sudo keepalive process
stop_sudo_keepalive() {
    if [ -n "${SUDO_KEEPER_PID}" ]; then
        kill "${SUDO_KEEPER_PID}" 2>/dev/null || true
        wait "${SUDO_KEEPER_PID}" 2>/dev/null || true
        SUDO_KEEPER_PID=""
    fi
}

# Temporarily stop fapolicyd if running
disable_fapolicyd() {
    if [ "${STIG_DETECTED}" != "true" ]; then
        return 0
    fi

    if systemctl is-active --quiet fapolicyd.service 2>/dev/null; then
        if confirm_sudo "Stop fapolicyd service" "Temporarily disables file execution policy (will be restarted later)"; then
            if run_sudo systemctl stop fapolicyd.service; then
                FAPOLICYD_WAS_ACTIVE=true
                log "✓ fapolicyd stopped"
            else
                error "Failed to stop fapolicyd - installation may fail"
                return 1
            fi
        else
            log "WARNING: fapolicyd is still running - installation may fail"
        fi
    fi
    return 0
}

# Re-enable fapolicyd if it was active
enable_fapolicyd() {
    if [ "${FAPOLICYD_WAS_ACTIVE}" = true ]; then
        log "STIG mode: Restarting fapolicyd service..."
        run_sudo systemctl start fapolicyd.service
        log "✓ fapolicyd restarted"
    fi
}

# Add tools to fapolicyd trust list
configure_fapolicyd_trust() {
    if [ "${STIG_DETECTED}" != "true" ]; then
        return 0
    fi

    if ! command -v fapolicyd-cli &>/dev/null; then
        log "fapolicyd not installed, skipping trust configuration"
        return 0
    fi

    log "STIG mode: Adding tools to fapolicyd trust list..."

    local tools_to_trust=(
        "/usr/local/bin/oc"
        "/usr/local/bin/oc-mirror"
        "/usr/local/bin/openshift-install"
    )

    if confirm_sudo "Add CLI tools to fapolicyd trust" "Allows oc, oc-mirror, openshift-install to execute from /usr/local/bin"; then
        log "  Adding tools to fapolicyd trust..."
        # Batch all fapolicyd operations in one sudo session
        sudo bash <<'EOFAP'
            for tool in /usr/local/bin/oc /usr/local/bin/oc-mirror /usr/local/bin/openshift-install; do
                if [ -f "${tool}" ]; then
                    if fapolicyd-cli --file add "${tool}" 2>&1; then
                        echo "  ✓ Added: ${tool}"
                    else
                        echo "  Warning: Could not add ${tool} (may already exist)"
                    fi
                fi
            done
            echo "  Updating fapolicyd database..."
            if fapolicyd-cli --update 2>&1; then
                echo "  ✓ Database updated"
            else
                echo "  Warning: Could not update database (fapolicyd may not be running)"
            fi
EOFAP
        log "✓ fapolicyd trust configuration complete"
    fi

    return 0
}

# Set temporary umask for file creation
set_install_umask() {
    if [ "${STIG_DETECTED}" = true ]; then
        ORIGINAL_UMASK=$(umask)
        # Set umask to 0022 for installation (allows group/other read)
        umask 0022
        log "STIG mode: Set umask to 0022 for installation (was ${ORIGINAL_UMASK})"
    fi
}

# Restore original umask
restore_umask() {
    if [ -n "${ORIGINAL_UMASK}" ]; then
        umask "${ORIGINAL_UMASK}"
        log "STIG mode: Restored umask to ${ORIGINAL_UMASK}"
    fi
}

# Fix max_user_namespaces for podman (STIG sets to 0)
fix_user_namespaces() {
    if [ "${STIG_DETECTED}" != "true" ]; then
        return 0
    fi

    if [ ! -f /proc/sys/user/max_user_namespaces ]; then
        return 0
    fi

    local max_ns=$(cat /proc/sys/user/max_user_namespaces 2>/dev/null || echo "0")
    if [ "${max_ns}" -lt 62372 ]; then
        if confirm_sudo "Set user.max_user_namespaces=62372" "Increases kernel limit from ${max_ns} to 62372 (required for rootless podman)"; then
            if run_sudo sysctl -w user.max_user_namespaces=62372 2>/dev/null; then
                log "✓ Set user.max_user_namespaces=62372"
            else
                error "Failed to set max_user_namespaces - podman may fail"
                return 1
            fi

            # Make it persistent
            if [ ! -f /etc/sysctl.d/99-user-namespaces.conf ]; then
                if confirm_sudo "Create /etc/sysctl.d/99-user-namespaces.conf" "Makes user namespace setting persistent across reboots"; then
                    echo "user.max_user_namespaces = 62372" | run_sudo tee /etc/sysctl.d/99-user-namespaces.conf >/dev/null
                    log "✓ Persisted max_user_namespaces configuration"
                fi
            fi
        else
            log "WARNING: user namespaces not increased - podman may fail"
        fi
    fi
    return 0
}

# Run all STIG preparation steps
prepare_stig_environment() {
    if [ "${STIG_DETECTED}" != "true" ]; then
        return 0
    fi

    log ""
    log "=== DISA STIG / FIPS Mode Detected ==="
    log "Applying compliance-friendly configuration..."
    log ""

    # Request sudo access upfront (single password prompt)
    request_sudo_access || return 1

    set_install_umask
    fix_user_namespaces
    disable_fapolicyd

    return 0
}

# Cleanup STIG environment changes
cleanup_stig_environment() {
    if [ "${STIG_DETECTED}" != "true" ]; then
        return 0
    fi

    log ""
    log "=== Restoring STIG Environment ==="

    configure_fapolicyd_trust
    enable_fapolicyd
    restore_umask
    stop_sudo_keepalive

    log "✓ STIG environment restored"
    log ""
}

check_podman() {
    if ! command -v podman &>/dev/null; then
        error "podman is not installed or not in PATH"
        error "Install podman: https://podman.io/getting-started/installation"
        exit 1
    fi
}

check_cli_tools() {
    if [ -d "${CLI_TOOLS_DIR}" ]; then
        log "CLI tools found in bundle"
        return 0
    else
        log "No CLI tools found in bundle (optional)"
        return 1
    fi
}

install_cli_tools() {
    if ! check_cli_tools; then
        return 0
    fi

    # In STIG mode, inform user about the requirement
    if [ "${STIG_DETECTED}" = "true" ]; then
        log "STIG/FIPS mode detected - CLI tools will be installed to ${CLI_INSTALL_DIR}"
        log "Note: Installation to /usr/local/bin requires sudo for fapolicyd trust"
    fi

    log "Installing CLI tools to ${CLI_INSTALL_DIR}..."

    # Create install directory if it doesn't exist
    local needs_sudo=false
    if [ ! -d "${CLI_INSTALL_DIR}" ]; then
        # Check if we need sudo to create the directory
        local parent_dir=$(dirname "${CLI_INSTALL_DIR}")
        if [ ! -w "${parent_dir}" ] 2>/dev/null; then
            needs_sudo=true
            if confirm_sudo "Create directory ${CLI_INSTALL_DIR}" "Creates system directory for CLI tools"; then
                run_sudo mkdir -p "${CLI_INSTALL_DIR}"
            else
                error "Cannot proceed without ${CLI_INSTALL_DIR}"
                return 1
            fi
        else
            mkdir -p "${CLI_INSTALL_DIR}"
        fi
    elif [ ! -w "${CLI_INSTALL_DIR}" ]; then
        needs_sudo=true
    fi

    local temp_dir=$(mktemp -d)
    local overwrite_all=false

    # Function to check and prompt for overwriting existing tool
    check_existing_tool() {
        local tool_name=$1
        if command -v "${tool_name}" &>/dev/null && [ "${overwrite_all}" = false ]; then
            local existing_path=$(which "${tool_name}")
            log "Found existing '${tool_name}' in PATH: ${existing_path}"
            echo ""
            echo "Options:"
            echo "  [o] Overwrite with bundled version"
            echo "  [s] Skip installation of ${tool_name}"
            echo "  [a] Overwrite all (don't ask again)"
            echo "  [c] Cancel installation"
            echo ""
            read -p "Choice [o/s/a/c]: " choice
            case "${choice}" in
                o|O) return 0 ;;
                s|S) return 1 ;;
                a|A) overwrite_all=true; return 0 ;;
                c|C) log "Installation cancelled by user"; return 2 ;;
                *) log "Invalid choice, skipping ${tool_name}"; return 1 ;;
            esac
        fi
        return 0
    }

    # Extract and install oc and kubectl
    if [ -f "${CLI_TOOLS_DIR}/openshift-client-linux.tar.gz" ]; then
        check_existing_tool "oc"
        local oc_choice=$?
        if [ ${oc_choice} -eq 2 ]; then
            rm -rf "$temp_dir"
            exit 0
        fi

        if [ ${oc_choice} -eq 0 ]; then
            tar -xzf "${CLI_TOOLS_DIR}/openshift-client-linux.tar.gz" -C "$temp_dir"
            # Install oc and kubectl together with a single sudo session
            if [ -f "$temp_dir/oc" ] || [ -f "$temp_dir/kubectl" ]; then
                if [ "${needs_sudo}" = true ]; then
                    if confirm_sudo "Install oc and kubectl to ${CLI_INSTALL_DIR}" "Copies binaries, sets ownership, applies SELinux labels"; then
                        # Batch all sudo commands together
                        sudo bash <<EOSUDO
                            set -e
                            if [ -f "$temp_dir/oc" ]; then
                                cp "$temp_dir/oc" "${CLI_INSTALL_DIR}/oc"
                                chmod +x "${CLI_INSTALL_DIR}/oc"
                                if [ "${STIG_DETECTED}" = true ]; then
                                    chown root:root "${CLI_INSTALL_DIR}/oc"
                                    chcon -t bin_t "${CLI_INSTALL_DIR}/oc" 2>/dev/null || true
                                fi
                            fi
                            if [ -f "$temp_dir/kubectl" ]; then
                                cp "$temp_dir/kubectl" "${CLI_INSTALL_DIR}/kubectl"
                                chmod +x "${CLI_INSTALL_DIR}/kubectl"
                                if [ "${STIG_DETECTED}" = true ]; then
                                    chown root:root "${CLI_INSTALL_DIR}/kubectl"
                                    chcon -t bin_t "${CLI_INSTALL_DIR}/kubectl" 2>/dev/null || true
                                fi
                            fi
EOSUDO
                        [ -f "$temp_dir/oc" ] && log "✓ Installed oc: $(${CLI_INSTALL_DIR}/oc version --client 2>/dev/null || echo 'version check failed')"
                        [ -f "$temp_dir/kubectl" ] && log "✓ Installed kubectl"
                    fi
                else
                    [ -f "$temp_dir/oc" ] && cp "$temp_dir/oc" "${CLI_INSTALL_DIR}/oc" && chmod +x "${CLI_INSTALL_DIR}/oc"
                    [ -f "$temp_dir/kubectl" ] && cp "$temp_dir/kubectl" "${CLI_INSTALL_DIR}/kubectl" && chmod +x "${CLI_INSTALL_DIR}/kubectl"
                    [ -f "$temp_dir/oc" ] && log "✓ Installed oc: $(${CLI_INSTALL_DIR}/oc version --client 2>/dev/null || echo 'version check failed')"
                    [ -f "$temp_dir/kubectl" ] && log "✓ Installed kubectl"
                fi
            fi
            rm -f "$temp_dir/oc" "$temp_dir/kubectl"
        fi
    fi

    # Extract openshift-install and oc-mirror
    local has_openshift_install=false
    local has_oc_mirror=false

    if [ -f "${CLI_TOOLS_DIR}/openshift-install-linux.tar.gz" ]; then
        check_existing_tool "openshift-install"
        local install_choice=$?
        if [ ${install_choice} -eq 2 ]; then
            rm -rf "$temp_dir"
            exit 0
        fi
        if [ ${install_choice} -eq 0 ]; then
            tar -xzf "${CLI_TOOLS_DIR}/openshift-install-linux.tar.gz" -C "$temp_dir"
            [ -f "$temp_dir/openshift-install" ] && has_openshift_install=true
        fi
    fi

    if [ -f "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" ]; then
        check_existing_tool "oc-mirror"
        local mirror_choice=$?
        if [ ${mirror_choice} -eq 2 ]; then
            rm -rf "$temp_dir"
            exit 0
        fi
        if [ ${mirror_choice} -eq 0 ]; then
            # Try different extraction methods
            if tar -xzf "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" -C "$temp_dir" 2>/dev/null; then
                : # Success with gzip
            elif tar -xf "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" -C "$temp_dir" 2>/dev/null; then
                log "Note: oc-mirror.tar.gz was not gzipped"
            elif file "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" 2>/dev/null | grep -q "executable"; then
                cp "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" "$temp_dir/oc-mirror"
                log "Note: oc-mirror.tar.gz was the binary itself, not an archive"
            else
                log "WARNING: Could not extract oc-mirror.tar.gz, skipping"
            fi
            [ -f "$temp_dir/oc-mirror" ] && has_oc_mirror=true
        fi
    fi

    # Install openshift-install and oc-mirror together with single sudo
    if [ "${has_openshift_install}" = true ] || [ "${has_oc_mirror}" = true ]; then
        if [ "${needs_sudo}" = true ]; then
            # Batch both installations in one sudo session
            sudo bash <<EOSUDO2
                set -e
                if [ -f "$temp_dir/openshift-install" ]; then
                    cp "$temp_dir/openshift-install" "${CLI_INSTALL_DIR}/openshift-install"
                    chmod +x "${CLI_INSTALL_DIR}/openshift-install"
                    if [ "${STIG_DETECTED}" = true ]; then
                        chown root:root "${CLI_INSTALL_DIR}/openshift-install"
                        chcon -t bin_t "${CLI_INSTALL_DIR}/openshift-install" 2>/dev/null || true
                    fi
                fi
                if [ -f "$temp_dir/oc-mirror" ]; then
                    cp "$temp_dir/oc-mirror" "${CLI_INSTALL_DIR}/oc-mirror"
                    chmod +x "${CLI_INSTALL_DIR}/oc-mirror"
                    if [ "${STIG_DETECTED}" = true ]; then
                        chown root:root "${CLI_INSTALL_DIR}/oc-mirror"
                        chcon -t bin_t "${CLI_INSTALL_DIR}/oc-mirror" 2>/dev/null || true
                    fi
                fi
EOSUDO2
            [ "${has_openshift_install}" = true ] && log "Installed openshift-install: $(${CLI_INSTALL_DIR}/openshift-install version 2>/dev/null | head -1 || echo 'version check failed')"
            [ "${has_oc_mirror}" = true ] && log "Installed oc-mirror"
        else
            if [ "${has_openshift_install}" = true ]; then
                cp "$temp_dir/openshift-install" "${CLI_INSTALL_DIR}/openshift-install"
                chmod +x "${CLI_INSTALL_DIR}/openshift-install"
                log "Installed openshift-install: $(${CLI_INSTALL_DIR}/openshift-install version 2>/dev/null | head -1 || echo 'version check failed')"
            fi
            if [ "${has_oc_mirror}" = true ]; then
                cp "$temp_dir/oc-mirror" "${CLI_INSTALL_DIR}/oc-mirror"
                chmod +x "${CLI_INSTALL_DIR}/oc-mirror"
                log "Installed oc-mirror"
            fi
        fi
        rm -f "$temp_dir/openshift-install" "$temp_dir/oc-mirror"
    fi

    rm -rf "$temp_dir"

    # Configure fapolicyd trust in STIG mode
    if [ "${STIG_DETECTED}" = true ]; then
        configure_fapolicyd_trust
    fi

    # Check if CLI_INSTALL_DIR is in PATH
    if ! echo "$PATH" | grep -q "${CLI_INSTALL_DIR}"; then
        log ""
        log "WARNING: ${CLI_INSTALL_DIR} is not in your PATH"
        echo ""
        echo "To use the CLI tools, add ${CLI_INSTALL_DIR} to your PATH:"
        echo ""

        # Detect shell
        if [ -n "${BASH_VERSION}" ] && [ -f "${HOME}/.bashrc" ]; then
            read -p "Add to ~/.bashrc? [y/N]: " add_path
            if [ "${add_path}" = "y" ] || [ "${add_path}" = "Y" ]; then
                echo "" >> "${HOME}/.bashrc"
                echo "# Added by airgap-architect import script" >> "${HOME}/.bashrc"
                echo "export PATH=\"${CLI_INSTALL_DIR}:\$PATH\"" >> "${HOME}/.bashrc"
                log "Added to ~/.bashrc - run 'source ~/.bashrc' or restart your shell"
            fi
        elif [ -n "${ZSH_VERSION}" ] && [ -f "${HOME}/.zshrc" ]; then
            read -p "Add to ~/.zshrc? [y/N]: " add_path
            if [ "${add_path}" = "y" ] || [ "${add_path}" = "Y" ]; then
                echo "" >> "${HOME}/.zshrc"
                echo "# Added by airgap-architect import script" >> "${HOME}/.zshrc"
                echo "export PATH=\"${CLI_INSTALL_DIR}:\$PATH\"" >> "${HOME}/.zshrc"
                log "Added to ~/.zshrc - run 'source ~/.zshrc' or restart your shell"
            fi
        else
            echo "  export PATH=\"${CLI_INSTALL_DIR}:\$PATH\""
            echo ""
        fi
    fi

    log "CLI tools installation complete"
}

configure_fapolicyd_trust_for_script_dir() {
    # Add fapolicyd trust for CLI tools in script directory (STIG mode)
    if [ "${STIG_DETECTED}" != true ]; then
        return 0
    fi

    if ! command -v fapolicyd-cli &>/dev/null; then
        log "  fapolicyd not installed, skipping script directory trust configuration"
        return 0
    fi

    # Check if fapolicyd is actually running
    if ! systemctl is-active --quiet fapolicyd 2>/dev/null && [ ! -e /run/fapolicyd/fapolicyd.fifo ]; then
        log "  fapolicyd is not running, skipping trust configuration"
        return 0
    fi

    local tools_in_script=()
    for tool in oc kubectl openshift-install oc-mirror; do
        if [ -f "${SCRIPT_DIR}/${tool}" ] && [ -x "${SCRIPT_DIR}/${tool}" ]; then
            tools_in_script+=("${SCRIPT_DIR}/${tool}")
        fi
    done

    if [ ${#tools_in_script[@]} -eq 0 ]; then
        log "  No CLI tools found in script directory to trust"
        return 0
    fi

    if confirm_sudo "Add script directory CLI tools to fapolicyd trust" "Allows tools in ${SCRIPT_DIR} to execute"; then
        log "  Adding script directory tools to fapolicyd trust..."
        # Use sudo to add each tool
        for tool in "${tools_in_script[@]}"; do
            if sudo fapolicyd-cli --file add "${tool}" 2>&1 | grep -q -E "added|already"; then
                log "  ✓ Added: ${tool}"
            else
                log "  Warning: Could not add ${tool}"
            fi
        done

        # Update fapolicyd database
        if sudo fapolicyd-cli --update 2>&1; then
            log "  ✓ fapolicyd database updated"
        else
            log "  Warning: Could not update database"
        fi
    fi

    return 0
}

create_cli_symlinks() {
    log "Copying CLI tools to script directory for container access..."

    local created_links=false

    # Copy tools to SCRIPT_DIR instead of symlinking (symlinks don't work in containers)
    for tool in oc kubectl openshift-install oc-mirror; do
        local tool_path=""
        local dest_path="${SCRIPT_DIR}/${tool}"

        # Remove existing file or symlink if it exists
        if [ -f "${dest_path}" ] || [ -L "${dest_path}" ]; then
            rm -f "${dest_path}"
        fi

        # Check if tool exists in CLI_INSTALL_DIR (if it was installed)
        if [ -f "${CLI_INSTALL_DIR}/${tool}" ] && [ -x "${CLI_INSTALL_DIR}/${tool}" ]; then
            tool_path="${CLI_INSTALL_DIR}/${tool}"
        # Otherwise check if it's in PATH
        elif command -v "${tool}" &>/dev/null; then
            tool_path=$(which "${tool}" 2>/dev/null || true)
        fi

        # Copy the tool if we found it (instead of symlinking)
        if [ -n "${tool_path}" ]; then
            if cp "${tool_path}" "${dest_path}" 2>/dev/null && chmod +x "${dest_path}"; then
                log "  Copied: ${tool} -> ${dest_path}"
                created_links=true
            else
                log "  WARNING: Could not copy ${tool}"
            fi
        fi
    done

    # If no tools were found in PATH or CLI_INSTALL_DIR, check if they exist in cli-tools bundle
    # and extract them directly to SCRIPT_DIR for use
    if [ "${created_links}" = false ] && [ -d "${CLI_TOOLS_DIR}" ]; then
        log "  No installed CLI tools found - extracting from bundle to ${SCRIPT_DIR}"

        # Try to create temp directory - use SCRIPT_DIR if mktemp fails (DISA STIG mode)
        local temp_dir
        if ! temp_dir=$(mktemp -d 2>/dev/null); then
            temp_dir="${SCRIPT_DIR}/.tmp-extract-$$"
            mkdir -p "${temp_dir}"
            log "  Using ${temp_dir} for extraction (mktemp unavailable)"
        fi

        # Extract oc and kubectl
        if [ -f "${CLI_TOOLS_DIR}/openshift-client-linux.tar.gz" ]; then
            tar -xzf "${CLI_TOOLS_DIR}/openshift-client-linux.tar.gz" -C "$temp_dir" 2>/dev/null || true
            if [ -f "$temp_dir/oc" ]; then
                cp "$temp_dir/oc" "${SCRIPT_DIR}/oc"
                chmod +x "${SCRIPT_DIR}/oc"
                log "  Extracted oc to ${SCRIPT_DIR}"
                created_links=true
            fi
            if [ -f "$temp_dir/kubectl" ]; then
                cp "$temp_dir/kubectl" "${SCRIPT_DIR}/kubectl"
                chmod +x "${SCRIPT_DIR}/kubectl"
                log "  Extracted kubectl to ${SCRIPT_DIR}"
                created_links=true
            fi
            rm -f "$temp_dir/oc" "$temp_dir/kubectl"
        fi

        # Extract openshift-install
        if [ -f "${CLI_TOOLS_DIR}/openshift-install-linux.tar.gz" ]; then
            tar -xzf "${CLI_TOOLS_DIR}/openshift-install-linux.tar.gz" -C "$temp_dir" 2>/dev/null || true
            if [ -f "$temp_dir/openshift-install" ]; then
                cp "$temp_dir/openshift-install" "${SCRIPT_DIR}/openshift-install"
                chmod +x "${SCRIPT_DIR}/openshift-install"
                log "  Extracted openshift-install to ${SCRIPT_DIR}"
                created_links=true
            fi
            rm -f "$temp_dir/openshift-install"
        fi

        # Extract oc-mirror
        if [ -f "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" ]; then
            # Try different extraction methods
            if tar -xzf "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" -C "$temp_dir" 2>/dev/null; then
                : # Success with gzip
            elif tar -xf "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" -C "$temp_dir" 2>/dev/null; then
                : # Success without gzip
            elif file "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" 2>/dev/null | grep -q "executable"; then
                # It's actually just the binary, not a tar at all
                cp "${CLI_TOOLS_DIR}/oc-mirror.tar.gz" "$temp_dir/oc-mirror"
            fi

            if [ -f "$temp_dir/oc-mirror" ]; then
                cp "$temp_dir/oc-mirror" "${SCRIPT_DIR}/oc-mirror"
                chmod +x "${SCRIPT_DIR}/oc-mirror"
                log "  Extracted oc-mirror to ${SCRIPT_DIR}"
                created_links=true
            fi
            rm -f "$temp_dir/oc-mirror"
        fi

        rm -rf "$temp_dir"
    fi

    if [ "${created_links}" = false ]; then
        log "  No CLI tools found in PATH, install directory, or bundle"
    fi

    # Configure fapolicyd trust for script directory tools in STIG mode
    configure_fapolicyd_trust_for_script_dir
}

update_container_config() {
    # Regenerate container-specific config with container paths for IDMS/ITMS/CA
    if [ ! -f "${DATA_DIR}/mirror-registry-config.json" ]; then
        log "No mirror registry config found, skipping container config update"
        return 0
    fi

    log "Updating container-specific config with latest paths..."

    # Start with a copy of the host config
    cp "${DATA_DIR}/mirror-registry-config.json" "${DATA_DIR}/mirror-registry-config-container.json"

    # Update CA cert path to container path if CA exists
    if [ -f "${DATA_DIR}/mirror-registry-ca.pem" ]; then
        sed -i.bak "s|\"caCertPath\": *\"[^\"]*\"|\"caCertPath\": \"/data/data/mirror-registry-ca.pem\"|g" \
            "${DATA_DIR}/mirror-registry-config-container.json"
        rm -f "${DATA_DIR}/mirror-registry-config-container.json.bak"
    fi

    # Add or update IDMS path if file exists
    if [ -f "${DATA_DIR}/imageDigestMirrorSet.yaml" ]; then
        if grep -q '"idmsPath"' "${DATA_DIR}/mirror-registry-config-container.json"; then
            sed -i.bak "s|\"idmsPath\": *\"[^\"]*\"|\"idmsPath\": \"/data/data/imageDigestMirrorSet.yaml\"|g" \
                "${DATA_DIR}/mirror-registry-config-container.json"
        else
            sed -i.bak 's|}$|,\n  "idmsPath": "/data/data/imageDigestMirrorSet.yaml"\n}|' \
                "${DATA_DIR}/mirror-registry-config-container.json"
        fi
        rm -f "${DATA_DIR}/mirror-registry-config-container.json.bak"
        log "  Added IDMS path to container config"
    fi

    # Add or update ITMS path if file exists
    if [ -f "${DATA_DIR}/imageTagMirrorSet.yaml" ]; then
        if grep -q '"itmsPath"' "${DATA_DIR}/mirror-registry-config-container.json"; then
            sed -i.bak "s|\"itmsPath\": *\"[^\"]*\"|\"itmsPath\": \"/data/data/imageTagMirrorSet.yaml\"|g" \
                "${DATA_DIR}/mirror-registry-config-container.json"
        else
            sed -i.bak 's|}$|,\n  "itmsPath": "/data/data/imageTagMirrorSet.yaml"\n}|' \
                "${DATA_DIR}/mirror-registry-config-container.json"
        fi
        rm -f "${DATA_DIR}/mirror-registry-config-container.json.bak"
        log "  Added ITMS path to container config"
    fi

    chmod 644 "${DATA_DIR}/mirror-registry-config-container.json"
    log "✓ Container config updated"
}

check_images_exist() {
    local frontend_exists=false
    local backend_exists=false

    if podman images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${FRONTEND_IMAGE}\$"; then
        frontend_exists=true
    fi

    if podman images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${BACKEND_IMAGE}\$"; then
        backend_exists=true
    fi

    if [ "$frontend_exists" = true ] && [ "$backend_exists" = true ]; then
        return 0
    else
        return 1
    fi
}

import_images() {
    log "Importing Airgap Architect images from tarballs..."

    if [ ! -f "${FRONTEND_IMAGE_TAR}" ]; then
        error "Frontend image tarball not found: ${FRONTEND_IMAGE_TAR}"
        exit 1
    fi

    if [ ! -f "${BACKEND_IMAGE_TAR}" ]; then
        error "Backend image tarball not found: ${BACKEND_IMAGE_TAR}"
        exit 1
    fi

    log "Importing frontend image..."
    LOADED_FRONTEND=$(podman load -i "${FRONTEND_IMAGE_TAR}" | grep -oP 'Loaded image: \K.*' | head -1)
    if [ -n "$LOADED_FRONTEND" ] && [ "$LOADED_FRONTEND" != "$FRONTEND_IMAGE" ]; then
        log "Tagging $LOADED_FRONTEND as $FRONTEND_IMAGE"
        podman tag "$LOADED_FRONTEND" "$FRONTEND_IMAGE"
    fi

    log "Importing backend image..."
    LOADED_BACKEND=$(podman load -i "${BACKEND_IMAGE_TAR}" | grep -oP 'Loaded image: \K.*' | head -1)
    if [ -n "$LOADED_BACKEND" ] && [ "$LOADED_BACKEND" != "$BACKEND_IMAGE" ]; then
        log "Tagging $LOADED_BACKEND as $BACKEND_IMAGE"
        podman tag "$LOADED_BACKEND" "$BACKEND_IMAGE"
    fi

    log "Images imported and tagged successfully"
}

load_existing_registry_config() {
    # Check both collection folder and user's home config directory
    local config_file=""
    local config_dir="${HOME}/.config/airgap-architect"

    # Try user's home config first (persists even if collection folder deleted)
    if [ -f "${config_dir}/mirror-registry-config.json" ]; then
        config_file="${config_dir}/mirror-registry-config.json"
        log "Found existing registry configuration in user config: ${config_file}"

        # Copy to DATA_DIR if not present there (backend needs it)
        if [ ! -f "${DATA_DIR}/mirror-registry-config.json" ]; then
            mkdir -p "${DATA_DIR}"
            cp "${config_file}" "${DATA_DIR}/mirror-registry-config.json"
            chmod 644 "${DATA_DIR}/mirror-registry-config.json"  # Readable by container
            log "Copied registry config to collection folder for backend access"
        fi

        # Also copy CA certificate if it exists
        if [ -f "${config_dir}/mirror-registry-ca.pem" ] && [ ! -f "${DATA_DIR}/mirror-registry-ca.pem" ]; then
            cp "${config_dir}/mirror-registry-ca.pem" "${DATA_DIR}/mirror-registry-ca.pem"
            chmod 644 "${DATA_DIR}/mirror-registry-ca.pem"  # Readable by container
            log "Copied CA certificate to collection folder for backend access"
        fi

        # Create container-specific config with container paths
        # Container mounts ${SCRIPT_DIR} as /data, so ${DATA_DIR} becomes /data/data
        cp "${DATA_DIR}/mirror-registry-config.json" "${DATA_DIR}/mirror-registry-config-container.json"

        # Update CA cert path to container path if CA exists
        if [ -f "${DATA_DIR}/mirror-registry-ca.pem" ]; then
            sed -i.bak "s|\"caCertPath\": *\"[^\"]*\"|\"caCertPath\": \"/data/data/mirror-registry-ca.pem\"|g" \
                "${DATA_DIR}/mirror-registry-config-container.json"
            rm -f "${DATA_DIR}/mirror-registry-config-container.json.bak"
        fi

        # Add IDMS path if file exists
        if [ -f "${DATA_DIR}/imageDigestMirrorSet.yaml" ]; then
            # Check if idmsPath already exists in the JSON
            if grep -q '"idmsPath"' "${DATA_DIR}/mirror-registry-config-container.json"; then
                # Update existing idmsPath
                sed -i.bak "s|\"idmsPath\": *\"[^\"]*\"|\"idmsPath\": \"/data/data/imageDigestMirrorSet.yaml\"|g" \
                    "${DATA_DIR}/mirror-registry-config-container.json"
            else
                # Add idmsPath before the closing brace
                sed -i.bak 's|}$|,\n  "idmsPath": "/data/data/imageDigestMirrorSet.yaml"\n}|' \
                    "${DATA_DIR}/mirror-registry-config-container.json"
            fi
            rm -f "${DATA_DIR}/mirror-registry-config-container.json.bak"
            log "Added IDMS path to container config"
        fi

        # Add ITMS path if file exists
        if [ -f "${DATA_DIR}/imageTagMirrorSet.yaml" ]; then
            # Check if itmsPath already exists in the JSON
            if grep -q '"itmsPath"' "${DATA_DIR}/mirror-registry-config-container.json"; then
                # Update existing itmsPath
                sed -i.bak "s|\"itmsPath\": *\"[^\"]*\"|\"itmsPath\": \"/data/data/imageTagMirrorSet.yaml\"|g" \
                    "${DATA_DIR}/mirror-registry-config-container.json"
            else
                # Add itmsPath before the closing brace
                sed -i.bak 's|}$|,\n  "itmsPath": "/data/data/imageTagMirrorSet.yaml"\n}|' \
                    "${DATA_DIR}/mirror-registry-config-container.json"
            fi
            rm -f "${DATA_DIR}/mirror-registry-config-container.json.bak"
            log "Added ITMS path to container config"
        fi

        chmod 644 "${DATA_DIR}/mirror-registry-config-container.json"
        log "Created container-specific config with container paths"
    elif [ -f "${DATA_DIR}/mirror-registry-config.json" ]; then
        config_file="${DATA_DIR}/mirror-registry-config.json"
        log "Found existing registry configuration in collection folder: ${config_file}"

        # Ensure it's readable by container
        chmod 644 "${DATA_DIR}/mirror-registry-config.json" 2>/dev/null || true
    else
        return 1
    fi

    # Parse JSON config using grep/sed (avoid jq dependency)
    MIRROR_REGISTRY_HOSTNAME=$(grep -o '"hostname": *"[^"]*"' "${config_file}" | sed 's/.*: *"\([^"]*\)".*/\1/')
    MIRROR_REGISTRY_PORT=$(grep -o '"port": *[0-9]*' "${config_file}" | sed 's/.*: *\([0-9]*\).*/\1/')
    local registry_type=$(grep -o '"type": *"[^"]*"' "${config_file}" | sed 's/.*: *"\([^"]*\)".*/\1/')

    if [ -n "${MIRROR_REGISTRY_HOSTNAME}" ] && [ -n "${MIRROR_REGISTRY_PORT}" ]; then
        if [ "${registry_type}" = "external" ]; then
            log "Loaded external registry config: ${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}"
            MIRROR_REGISTRY_EXISTING="true"
            MIRROR_REGISTRY_INSTALL="false"
        else
            log "Loaded mirror-registry config: ${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}"
            MIRROR_REGISTRY_INSTALL="false"  # Don't reinstall
        fi
        return 0
    fi

    return 1
}

check_mirror_registry() {
    # Check if mirror-registry is already running

    # Check system-level service
    if command -v systemctl &>/dev/null && systemctl is-active --quiet quay-pod.service 2>/dev/null; then
        log "Mirror-registry is already running (system service: quay-pod)"
        return 0
    fi

    # Check user-level service (more common with newer mirror-registry)
    if command -v systemctl &>/dev/null && systemctl --user is-active --quiet quay-app.service 2>/dev/null; then
        log "Mirror-registry is already running (user service: quay-app)"
        return 0
    fi

    # Check if podman containers exist
    if podman ps -a --format '{{.Names}}' 2>/dev/null | grep -qE '^(quay|quay-app|quay-postgres|quay-redis)$'; then
        log "Mirror-registry containers exist"
        return 0
    fi

    return 1
}

prompt_for_registry_config() {
    if [ "${MIRROR_REGISTRY_INSTALL}" != "prompt" ]; then
        return 0
    fi

    log ""
    log "=== Container Registry Configuration ==="
    log ""
    log "This bundle requires a container registry to store mirrored images."
    log ""
    echo "Options:"
    echo "  [i] Install mirror-registry (Quay) on this host"
    echo "  [e] Use an existing registry"
    echo "  [s] Skip registry setup (configure manually later)"
    echo ""
    read -p "Choice [i/e/s]: " registry_choice

    case "${registry_choice}" in
        i|I)
            log "--- Installing Mirror-Registry ---"
            log ""

            # Prompt for configuration
            read -p "Hostname [$(hostname -f)]: " input_hostname
            MIRROR_REGISTRY_HOSTNAME="${input_hostname:-$(hostname -f)}"

            read -p "Port [8443]: " input_port
            MIRROR_REGISTRY_PORT="${input_port:-8443}"

            read -p "Data storage path [/opt/quay]: " input_path
            MIRROR_REGISTRY_DATA_PATH="${input_path:-/opt/quay}"

            # Validate and create data path if needed
            if [ ! -d "${MIRROR_REGISTRY_DATA_PATH}" ]; then
                log "Data path does not exist: ${MIRROR_REGISTRY_DATA_PATH}"
                read -p "Create directory? [Y/n]: " create_dir
                if [ "${create_dir}" != "n" ] && [ "${create_dir}" != "N" ]; then
                    # Check if we need sudo
                    local parent_dir=$(dirname "${MIRROR_REGISTRY_DATA_PATH}")
                    if [ ! -w "${parent_dir}" ]; then
                        log "Creating ${MIRROR_REGISTRY_DATA_PATH} (requires sudo)..."
                        if ! sudo mkdir -p "${MIRROR_REGISTRY_DATA_PATH}"; then
                            error "Failed to create directory"
                            exit 1
                        fi
                        # Set ownership to current user so mirror-registry can write to it
                        log "Setting ownership to $(id -un):$(id -gn)..."
                        sudo chown "$(id -un):$(id -gn)" "${MIRROR_REGISTRY_DATA_PATH}"
                        sudo chmod 755 "${MIRROR_REGISTRY_DATA_PATH}"
                    else
                        # No sudo needed
                        if ! mkdir -p "${MIRROR_REGISTRY_DATA_PATH}"; then
                            error "Failed to create directory"
                            exit 1
                        fi
                        chmod 755 "${MIRROR_REGISTRY_DATA_PATH}"
                    fi
                    log "✓ Created ${MIRROR_REGISTRY_DATA_PATH}"
                else
                    error "Data path must exist before proceeding"
                    exit 1
                fi
            else
                # Directory exists, check if writable
                if [ ! -w "${MIRROR_REGISTRY_DATA_PATH}" ]; then
                    error "Data path exists but is not writable: ${MIRROR_REGISTRY_DATA_PATH}"
                    log "Current ownership: $(ls -ld "${MIRROR_REGISTRY_DATA_PATH}")"
                    read -p "Fix permissions with sudo? [Y/n]: " fix_perms
                    if [ "${fix_perms}" != "n" ] && [ "${fix_perms}" != "N" ]; then
                        sudo chown "$(id -un):$(id -gn)" "${MIRROR_REGISTRY_DATA_PATH}"
                        sudo chmod 755 "${MIRROR_REGISTRY_DATA_PATH}"
                        log "✓ Fixed permissions on ${MIRROR_REGISTRY_DATA_PATH}"
                    else
                        error "Cannot proceed without write access to data path"
                        exit 1
                    fi
                fi
            fi

            read -s -p "Initial admin password (leave empty to auto-generate): " input_password
            echo ""
            MIRROR_REGISTRY_INIT_PASSWORD="${input_password}"

            # Ask about SSL certificates
            echo ""
            read -p "Provide custom SSL certificate? (y/N): " use_custom_ssl
            if [ "${use_custom_ssl}" = "y" ] || [ "${use_custom_ssl}" = "Y" ]; then
                read -p "Path to SSL certificate file: " MIRROR_REGISTRY_SSL_CERT
                read -p "Path to SSL key file: " MIRROR_REGISTRY_SSL_KEY

                # Validate certificate files exist
                if [ ! -f "${MIRROR_REGISTRY_SSL_CERT}" ]; then
                    error "Certificate file not found: ${MIRROR_REGISTRY_SSL_CERT}"
                    exit 1
                fi
                if [ ! -f "${MIRROR_REGISTRY_SSL_KEY}" ]; then
                    error "Key file not found: ${MIRROR_REGISTRY_SSL_KEY}"
                    exit 1
                fi
                log "Will use custom SSL certificate"
            else
                log "Will use self-signed certificate (auto-generated)"
            fi

            MIRROR_REGISTRY_INSTALL="true"
            ;;

        e|E)
            log "--- Configure Existing Registry ---"
            log ""

            local retry_count=0
            local max_retries=3

            while [ ${retry_count} -lt ${max_retries} ]; do
                read -p "Registry URL (hostname:port): " EXISTING_REGISTRY_URL
                read -p "Username: " EXISTING_REGISTRY_USERNAME
                read -s -p "Password: " EXISTING_REGISTRY_PASSWORD
                echo ""

                log "Testing connection..."
                if podman login --tls-verify=false "${EXISTING_REGISTRY_URL}" \
                    -u "${EXISTING_REGISTRY_USERNAME}" \
                    -p "${EXISTING_REGISTRY_PASSWORD}" &>/dev/null; then
                    log "✓ Successfully authenticated to ${EXISTING_REGISTRY_URL}"
                    MIRROR_REGISTRY_EXISTING="true"
                    MIRROR_REGISTRY_INSTALL="false"
                    return 0
                else
                    retry_count=$((retry_count + 1))
                    if [ ${retry_count} -lt ${max_retries} ]; then
                        error "Authentication failed. Please try again (${retry_count}/${max_retries})"
                    else
                        error "Authentication failed after ${max_retries} attempts"
                        log "Exiting..."
                        exit 1
                    fi
                fi
            done
            ;;

        s|S)
            log ""
            log "⚠ WARNING: No registry configured."
            log "You will need to configure a container registry manually before importing images."
            log ""
            read -p "Continue anyway? [y/N]: " confirm
            if [ "${confirm}" != "y" ] && [ "${confirm}" != "Y" ]; then
                log "Exiting..."
                exit 0
            fi
            MIRROR_REGISTRY_INSTALL="false"
            ;;

        *)
            error "Invalid choice"
            exit 1
            ;;
    esac
}

install_mirror_registry() {
    if [ "${MIRROR_REGISTRY_INSTALL}" != "true" ]; then
        return 0
    fi

    if ! check_cli_tools; then
        log "CLI tools not found, skipping mirror-registry installation"
        return 0
    fi

    if [ ! -f "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" ]; then
        log "mirror-registry.tar.gz not found, skipping installation"
        MIRROR_REGISTRY_INSTALL="false"  # Set to false so we don't wait for it
        return 0
    fi

    if check_mirror_registry; then
        log "Mirror-registry already installed, skipping"
        return 0
    fi

    log ""
    log "=== Installing Mirror-Registry ==="
    log ""

    # Extract mirror-registry installer
    # Use a persistent directory instead of temp to preserve for uninstall
    local install_dir="${DATA_DIR}/mirror-registry-install"
    mkdir -p "${install_dir}"
    log "Extracting mirror-registry to ${install_dir}..."

    # Try different extraction methods
    if tar -xzf "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" -C "$install_dir" 2>/dev/null; then
        log "Extracted mirror-registry (gzipped tar)"
    elif tar -xf "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" -C "$install_dir" 2>/dev/null; then
        log "Extracted mirror-registry (uncompressed tar)"
    elif file "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" 2>/dev/null | grep -q "executable"; then
        # It's the binary itself
        cp "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" "$install_dir/mirror-registry"
        log "mirror-registry.tar.gz was the binary itself"
    else
        error "Failed to extract mirror-registry.tar.gz"
        rm -rf "$install_dir"
        return 1
    fi

    # Use prompted or environment-configured values
    MIRROR_REGISTRY_HOSTNAME="${MIRROR_REGISTRY_HOSTNAME:-$(hostname -f)}"
    MIRROR_REGISTRY_PORT="${MIRROR_REGISTRY_PORT:-8443}"
    MIRROR_REGISTRY_DATA_PATH="${MIRROR_REGISTRY_DATA_PATH:-/opt/quay}"

    # Generate init password if not provided
    if [ -z "${MIRROR_REGISTRY_INIT_PASSWORD}" ]; then
        MIRROR_REGISTRY_INIT_PASSWORD=$(openssl rand -base64 12 2>/dev/null || cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
        log "Generated random init password for Quay"
    fi

    log "Running mirror-registry installer..."
    log "  Hostname: ${MIRROR_REGISTRY_HOSTNAME}"
    log "  Port: ${MIRROR_REGISTRY_PORT}"
    log "  Data path: ${MIRROR_REGISTRY_DATA_PATH}"

    # Prepare SSL certificate arguments
    local ssl_cert_arg=""
    local ssl_key_arg=""
    local using_custom_ssl=false

    if [ -n "${MIRROR_REGISTRY_SSL_CERT}" ] && [ -f "${MIRROR_REGISTRY_SSL_CERT}" ] && \
       [ -n "${MIRROR_REGISTRY_SSL_KEY}" ] && [ -f "${MIRROR_REGISTRY_SSL_KEY}" ]; then
        ssl_cert_arg="${MIRROR_REGISTRY_SSL_CERT}"
        ssl_key_arg="${MIRROR_REGISTRY_SSL_KEY}"
        using_custom_ssl=true
        log "  SSL: Custom certificate"
    else
        ssl_cert_arg=""
        ssl_key_arg=""
        log "  SSL: Self-signed (auto-generated)"
    fi

    cd "$install_dir"

    # Make mirror-registry executable if it exists
    if [ -f "./mirror-registry" ]; then
        chmod +x ./mirror-registry

        # Set SELinux context if SELinux is enabled (DISA STIG mode)
        if command -v chcon &>/dev/null && [ -f "./mirror-registry" ]; then
            log "Setting SELinux context for mirror-registry..."
            if [ "${STIG_DETECTED}" = true ]; then
                sudo chcon -t bin_t ./mirror-registry 2>/dev/null || \
                    log "Warning: Could not set SELinux context (may need sudo or SELinux is disabled)"
            else
                chcon -t bin_t ./mirror-registry 2>/dev/null || \
                    log "Warning: Could not set SELinux context (may need sudo or SELinux is disabled)"
            fi
        fi

        # In STIG mode, add mirror-registry to fapolicyd trust (in install dir)
        if [ "${STIG_DETECTED}" = true ]; then
            local full_path="$(cd "$(dirname "./mirror-registry")" && pwd)/$(basename "./mirror-registry")"
            if command -v fapolicyd-cli &>/dev/null; then
                if confirm_sudo "Add mirror-registry to fapolicyd trust" "Allows ${full_path} to execute"; then
                    run_sudo fapolicyd-cli --file add "${full_path}" 2>/dev/null || log "Warning: Could not add to fapolicyd trust"
                    run_sudo fapolicyd-cli --update 2>/dev/null || log "Warning: Could not update fapolicyd database"
                    log "✓ Added mirror-registry to fapolicyd trust"
                fi
            fi
        fi

        MIRROR_REGISTRY_CMD="./mirror-registry"
    else
        error "mirror-registry binary not found after extraction"
        cd -
        rm -rf "$install_dir"
        return 1
    fi

    # In STIG mode, temporarily set umask for mirror-registry installation
    local old_umask=""
    if [ "${STIG_DETECTED}" = true ]; then
        old_umask=$(umask)
        umask 0022
    fi

    if ! ${MIRROR_REGISTRY_CMD} install \
        --quayHostname "${MIRROR_REGISTRY_HOSTNAME}" \
        --quayRoot "${MIRROR_REGISTRY_DATA_PATH}" \
        --initPassword "${MIRROR_REGISTRY_INIT_PASSWORD}" \
        --sslCert "${ssl_cert_arg}" \
        --sslKey "${ssl_key_arg}"; then
        error "Mirror-registry installation failed"
        [ -n "${old_umask}" ] && umask "${old_umask}"
        cd -
        # Don't remove install_dir on failure - may need it for debugging
        log "Mirror-registry files preserved at: ${install_dir}"
        return 1
    fi

    # Restore umask if changed
    [ -n "${old_umask}" ] && umask "${old_umask}"

    cd -
    # Keep the install_dir - don't remove it! We need it for uninstall
    log "Mirror-registry installation files preserved at: ${install_dir}"

    log ""
    log "✓ Mirror-registry installed successfully"
    log "  Admin user: init"
    log "  Admin password: ${MIRROR_REGISTRY_INIT_PASSWORD}"
    log "  URL: https://${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}"
    log ""

    # Define config directory in user's home (persists even if collection folder deleted)
    local config_dir="${HOME}/.config/airgap-architect"
    mkdir -p "${config_dir}"

    # Save credentials and connection details to both locations
    # 1. Collection folder (DATA_DIR) - accessible to backend container
    # 2. User's home config - persists even if collection folder deleted
    mkdir -p "${DATA_DIR}"

    # Save credentials in text format to BOTH locations
    for target_dir in "${DATA_DIR}" "${config_dir}"; do
        cat > "${target_dir}/mirror-registry-credentials.txt" <<EOF
Mirror Registry Credentials
Generated: $(date)

URL: https://${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}
Username: init
Password: ${MIRROR_REGISTRY_INIT_PASSWORD}

Podman login command:
podman login --tls-verify=false ${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT} -u init -p ${MIRROR_REGISTRY_INIT_PASSWORD}
EOF
        chmod 600 "${target_dir}/mirror-registry-credentials.txt"
    done

    # Create JSON file for Airgap Architect to consume in BOTH locations
    for target_dir in "${DATA_DIR}" "${config_dir}"; do
        cat > "${target_dir}/mirror-registry-config.json" <<EOF
{
  "url": "https://${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}",
  "hostname": "${MIRROR_REGISTRY_HOSTNAME}",
  "port": ${MIRROR_REGISTRY_PORT},
  "username": "init",
  "password": "${MIRROR_REGISTRY_INIT_PASSWORD}",
  "tlsVerify": false,
  "installedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "dataPath": "${MIRROR_REGISTRY_DATA_PATH}",
  "installDir": "${install_dir}",
  "type": "quay"
}
EOF
        # DATA_DIR needs 644 for container access, config_dir can be 600 for security
        if [ "${target_dir}" = "${DATA_DIR}" ]; then
            chmod 644 "${target_dir}/mirror-registry-config.json"
        else
            chmod 600 "${target_dir}/mirror-registry-config.json"
        fi
    done

    # Copy CA certificate if it exists (mirror-registry generates it)
    local ca_cert_path=""
    local tls_verify="false"
    local ssl_type="self-signed"

    if [ "${using_custom_ssl}" = true ]; then
        ssl_type="custom"
        tls_verify="true"
        # Copy custom certificate for reference to BOTH locations
        if [ -f "${MIRROR_REGISTRY_SSL_CERT}" ]; then
            cp "${MIRROR_REGISTRY_SSL_CERT}" "${DATA_DIR}/mirror-registry-cert.pem"
            cp "${MIRROR_REGISTRY_SSL_CERT}" "${config_dir}/mirror-registry-cert.pem"
            ca_cert_path="${DATA_DIR}/mirror-registry-cert.pem"
        fi
    elif [ -f "${MIRROR_REGISTRY_DATA_PATH}/quay-rootCA/rootCA.pem" ]; then
        cp "${MIRROR_REGISTRY_DATA_PATH}/quay-rootCA/rootCA.pem" "${DATA_DIR}/mirror-registry-ca.pem"
        cp "${MIRROR_REGISTRY_DATA_PATH}/quay-rootCA/rootCA.pem" "${config_dir}/mirror-registry-ca.pem"
        ca_cert_path="${DATA_DIR}/mirror-registry-ca.pem"
        log "CA certificate copied to ${DATA_DIR}/mirror-registry-ca.pem"
        log "CA certificate also saved to ${config_dir}/mirror-registry-ca.pem"
    fi

    # Write JSON config with CA path if available to BOTH locations
    if [ -n "${ca_cert_path}" ]; then
        for target_dir in "${DATA_DIR}" "${config_dir}"; do
            local target_ca_path="${target_dir}/mirror-registry-ca.pem"
            if [ "${using_custom_ssl}" = true ]; then
                target_ca_path="${target_dir}/mirror-registry-cert.pem"
            fi
            cat > "${target_dir}/mirror-registry-config.json" <<EOF
{
  "url": "https://${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}",
  "hostname": "${MIRROR_REGISTRY_HOSTNAME}",
  "port": ${MIRROR_REGISTRY_PORT},
  "username": "init",
  "password": "${MIRROR_REGISTRY_INIT_PASSWORD}",
  "tlsVerify": ${tls_verify},
  "caCertPath": "${target_ca_path}",
  "sslType": "${ssl_type}",
  "installedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "dataPath": "${MIRROR_REGISTRY_DATA_PATH}",
  "installDir": "${install_dir}",
  "type": "quay"
}
EOF
            # DATA_DIR needs 644 for container access, config_dir can be 600 for security
            if [ "${target_dir}" = "${DATA_DIR}" ]; then
                chmod 644 "${target_dir}/mirror-registry-config.json"
            else
                chmod 600 "${target_dir}/mirror-registry-config.json"
            fi
        done
    fi

    log "Registry configuration saved to multiple locations:"
    log "  Collection folder:"
    log "    - Credentials: ${DATA_DIR}/mirror-registry-credentials.txt"
    log "    - JSON config: ${DATA_DIR}/mirror-registry-config.json"
    if [ -f "${DATA_DIR}/mirror-registry-ca.pem" ]; then
        log "    - CA cert: ${DATA_DIR}/mirror-registry-ca.pem"
    fi
    log "  User config (persists if collection folder deleted):"
    log "    - Credentials: ${config_dir}/mirror-registry-credentials.txt"
    log "    - JSON config: ${config_dir}/mirror-registry-config.json"
    if [ -f "${config_dir}/mirror-registry-ca.pem" ]; then
        log "    - CA cert: ${config_dir}/mirror-registry-ca.pem"
    fi

    return 0
}

configure_existing_registry() {
    if [ "${MIRROR_REGISTRY_EXISTING}" != "true" ]; then
        return 0
    fi

    log ""
    log "=== Configuring Existing Registry Connection ==="
    log ""

    # Test connection with podman login (already done in prompt, but verify)
    if ! podman login --tls-verify=false "${EXISTING_REGISTRY_URL}" \
        -u "${EXISTING_REGISTRY_USERNAME}" \
        -p "${EXISTING_REGISTRY_PASSWORD}" &>/dev/null; then
        error "Failed to authenticate to existing registry"
        return 1
    fi

    log "✓ Connected to existing registry: ${EXISTING_REGISTRY_URL}"

    # Define config directory in user's home (persists even if collection folder deleted)
    local config_dir="${HOME}/.config/airgap-architect"
    mkdir -p "${config_dir}"
    mkdir -p "${DATA_DIR}"

    # Save reference for documentation to BOTH locations
    for target_dir in "${DATA_DIR}" "${config_dir}"; do
        cat > "${target_dir}/mirror-registry-external.txt" <<EOF
External Mirror Registry Configuration
Connected: $(date)

Registry URL: ${EXISTING_REGISTRY_URL}
Username: ${EXISTING_REGISTRY_USERNAME}

Note: Credentials are stored in podman auth.json
EOF
        chmod 600 "${target_dir}/mirror-registry-external.txt"
    done

    # Create JSON file for Airgap Architect to consume
    # Parse hostname and port from URL
    REGISTRY_HOST=$(echo "${EXISTING_REGISTRY_URL}" | cut -d: -f1)
    REGISTRY_PORT=$(echo "${EXISTING_REGISTRY_URL}" | cut -d: -f2)
    if [ "${REGISTRY_PORT}" = "${REGISTRY_HOST}" ]; then
        REGISTRY_PORT="443"  # Default HTTPS port
    fi

    # Save to BOTH locations
    for target_dir in "${DATA_DIR}" "${config_dir}"; do
        cat > "${target_dir}/mirror-registry-config.json" <<EOF
{
  "url": "https://${EXISTING_REGISTRY_URL}",
  "hostname": "${REGISTRY_HOST}",
  "port": ${REGISTRY_PORT},
  "username": "${EXISTING_REGISTRY_USERNAME}",
  "tlsVerify": false,
  "connectedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "type": "external",
  "note": "Credentials stored in podman auth.json"
}
EOF
        chmod 600 "${target_dir}/mirror-registry-config.json"
    done

    log "Registry configuration saved to multiple locations:"
    log "  Collection folder:"
    log "    - Details: ${DATA_DIR}/mirror-registry-external.txt"
    log "    - JSON config: ${DATA_DIR}/mirror-registry-config.json"
    log "  User config (persists if collection folder deleted):"
    log "    - Details: ${config_dir}/mirror-registry-external.txt"
    log "    - JSON config: ${config_dir}/mirror-registry-config.json"

    return 0
}

wait_for_mirror_registry() {
    if [ "${MIRROR_REGISTRY_INSTALL}" != "true" ] && [ "${MIRROR_REGISTRY_EXISTING}" != "true" ]; then
        return 0
    fi

    # Only wait if we just installed (not for existing registry)
    if [ "${MIRROR_REGISTRY_INSTALL}" != "true" ]; then
        return 0
    fi

    log ""
    log "Waiting for mirror-registry to be ready..."
    log "This may take 5-10 minutes for initial startup..."
    local max_wait=600  # 10 minutes
    local waited=0

    while [ $waited -lt $max_wait ]; do
        if curl -k -s "https://${MIRROR_REGISTRY_HOSTNAME}:${MIRROR_REGISTRY_PORT}/health/instance" >/dev/null 2>&1; then
            echo ""
            log "✓ Mirror-registry is ready (took ${waited} seconds)"
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
        # Show progress every minute
        if [ $((waited % 60)) -eq 0 ]; then
            echo ""
            log "  Still waiting... ${waited}/${max_wait} seconds elapsed"
        else
            echo -n "."
        fi
    done

    echo ""
    error "Mirror-registry did not become ready within ${max_wait} seconds (${max_wait}/60 minutes)"
    log "You can check the status with: podman ps -a | grep quay"
    log "View logs with: podman logs <container-name>"
    return 1
}

start_containers() {
    log "Starting Airgap Architect containers..."

    # Prepare STIG environment if needed
    prepare_stig_environment

    # Install CLI tools first
    install_cli_tools

    # Create symlinks to CLI tools in script directory
    create_cli_symlinks

    # Try to load existing registry configuration from data directory
    if load_existing_registry_config; then
        log "Using existing registry configuration - skipping prompts"
    else
        # Prompt for registry configuration if needed
        prompt_for_registry_config
    fi

    # Either install mirror-registry or configure existing registry
    if [ "${MIRROR_REGISTRY_INSTALL}" = "true" ]; then
        install_mirror_registry
    elif [ "${MIRROR_REGISTRY_EXISTING}" = "true" ]; then
        configure_existing_registry
    fi

    # Wait for registry to be ready
    wait_for_mirror_registry

    # Check if images exist, import if not
    if ! check_images_exist; then
        log "Images not found, importing from tarballs..."
        import_images
    else
        log "Images already imported, skipping import"
    fi

    mkdir -p "${DATA_DIR}"

    # Ensure directories we own have proper permissions for container to write
    # Only change directories owned by current user, ignore errors for others
    log "Setting directory permissions for container access..."
    find "${SCRIPT_DIR}" -type d -user "$(id -u)" -exec chmod 777 {} \; 2>/dev/null || true

    # Set SELinux context so container can write to script directory
    if command -v chcon &>/dev/null; then
        log "Setting SELinux context on ${SCRIPT_DIR} for container access..."
        chcon -Rt container_file_t "${SCRIPT_DIR}" 2>/dev/null || log "Warning: chcon failed (SELinux may not be enabled)"
    fi

    log "Mounting script directory as backend /data: ${SCRIPT_DIR}"

    # Ensure imageset-config.yaml is readable by container
    if [ -f "${SCRIPT_DIR}/imageset-config.yaml" ]; then
        chmod 644 "${SCRIPT_DIR}/imageset-config.yaml"
        log "Set imageset-config.yaml permissions for container access"
    fi

    # Regenerate container-specific config with latest IDMS/ITMS paths
    update_container_config

    log "Starting backend container..."
    podman run -d \
        --name airgap-architect-backend \
        --replace \
        --user "$(id -u):$(id -g)" \
        -e PORT=4000 \
        -e DATA_DIR=/data \
        -e MIRROR_REGISTRY_CONFIG=/data/data/mirror-registry-config-container.json \
        -e IMAGESET_CONFIG=/data/imageset-config.yaml \
        -e IDMS_PATH=/data/data/imageDigestMirrorSet.yaml \
        -e ITMS_PATH=/data/data/imageTagMirrorSet.yaml \
        -e MOCK_MODE="${MOCK_MODE:-false}" \
        -e FEEDBACK_MODE="${FEEDBACK_MODE:-github}" \
        -e APP_REPO="${APP_REPO:-bstrauss84/openshift-airgap-architect}" \
        -e APP_BRANCH="${APP_BRANCH:-main}" \
        -v "${SCRIPT_DIR}:/data:z" \
        -p 127.0.0.1:4000:4000 \
        --stop-timeout 3 \
        "${BACKEND_IMAGE}"

    log "Waiting for backend to be ready..."
    sleep 2

    log "Starting frontend container..."
    podman run -d \
        --name airgap-architect-frontend \
        --replace \
        -e VITE_API_BASE=http://localhost:4000 \
        -e VITE_ALLOWED_HOSTS="${VITE_ALLOWED_HOSTS:-}" \
        -p 127.0.0.1:5173:5173 \
        --stop-timeout 3 \
        "${FRONTEND_IMAGE}"

    log "Containers started successfully"
    log ""
    log "Airgap Architect is now running:"
    log "  Frontend UI: http://localhost:5173"
    log "  Backend API: http://localhost:4000"
    log ""
    log "To view logs: $0 logs"
    log "To stop:      $0 stop"

    # Cleanup STIG environment
    cleanup_stig_environment
}

stop_containers() {
    log "Stopping Airgap Architect containers..."

    if podman ps -a --format '{{.Names}}' | grep -q '^airgap-architect-frontend$'; then
        log "Stopping frontend..."
        podman stop airgap-architect-frontend || true
        podman rm airgap-architect-frontend || true
    fi

    if podman ps -a --format '{{.Names}}' | grep -q '^airgap-architect-backend$'; then
        log "Stopping backend..."
        podman stop airgap-architect-backend || true
        podman rm airgap-architect-backend || true
    fi

    log "Containers stopped"
}

restart_containers() {
    stop_containers
    start_containers
}

show_status() {
    log "Airgap Architect container status:"
    echo ""

    if podman ps --format '{{.Names}}' | grep -q '^airgap-architect-backend$'; then
        echo "✓ Backend:  Running"
        podman ps --filter name=airgap-architect-backend --format "  - Container: {{.Names}} ({{.Status}})"
        echo "  - API:       http://localhost:4000"
    else
        echo "✗ Backend:  Not running"
    fi

    echo ""

    if podman ps --format '{{.Names}}' | grep -q '^airgap-architect-frontend$'; then
        echo "✓ Frontend: Running"
        podman ps --filter name=airgap-architect-frontend --format "  - Container: {{.Names}} ({{.Status}})"
        echo "  - UI:        http://localhost:5173"
    else
        echo "✗ Frontend: Not running"
    fi

    echo ""
}

show_logs() {
    local container="${1:-}"

    if [ -z "${container}" ]; then
        log "Showing logs from all containers..."
        echo ""
        echo "=== Backend logs ==="
        podman logs --tail 50 airgap-architect-backend 2>&1 || echo "Backend not running"
        echo ""
        echo "=== Frontend logs ==="
        podman logs --tail 50 airgap-architect-frontend 2>&1 || echo "Frontend not running"
    else
        podman logs -f "airgap-architect-${container}"
    fi
}

mirror_to_registry() {
    log "Mirroring images from archives to registry..."

    # Check if oc-mirror is available
    if ! command -v oc-mirror &>/dev/null; then
        error "oc-mirror is not installed or not in PATH"
        error "Run '$0 start' first to install CLI tools"
        exit 1
    fi

    # Check if archives directory exists
    if [ ! -d "${SCRIPT_DIR}/archives" ]; then
        error "Archives directory not found: ${SCRIPT_DIR}/archives"
        error "This bundle may not contain mirrored images"
        exit 1
    fi

    # Load registry configuration
    local registry_config=""
    local config_dir="${HOME}/.config/airgap-architect"

    if [ -f "${SCRIPT_DIR}/data/mirror-registry-config.json" ]; then
        registry_config="${SCRIPT_DIR}/data/mirror-registry-config.json"
    elif [ -f "${config_dir}/mirror-registry-config.json" ]; then
        registry_config="${config_dir}/mirror-registry-config.json"
    else
        error "No registry configuration found"
        error "Run '$0 start' first to configure a registry"
        exit 1
    fi

    # Parse registry configuration
    local registry_hostname=$(grep -o '"hostname": *"[^"]*"' "${registry_config}" | sed 's/.*: *"\([^"]*\)".*/\1/')
    local registry_port=$(grep -o '"port": *[0-9]*' "${registry_config}" | sed 's/.*: *\([0-9]*\).*/\1/')
    local registry_url="${registry_hostname}:${registry_port}"
    local ca_cert_path=$(grep -o '"caCertPath": *"[^"]*"' "${registry_config}" | sed 's/.*: *"\([^"]*\)".*/\1/')

    log "Using registry: ${registry_url}"

    # Prepare oc-mirror arguments for TLS and authentication
    local oc_mirror_args=""

    # Check for CA certificate - prefer data directory first
    local ca_cert=""
    if [ -f "${SCRIPT_DIR}/data/mirror-registry-ca.pem" ]; then
        ca_cert="${SCRIPT_DIR}/data/mirror-registry-ca.pem"
    elif [ -f "${config_dir}/mirror-registry-ca.pem" ]; then
        ca_cert="${config_dir}/mirror-registry-ca.pem"
    elif [ -n "$ca_cert_path" ] && [ -f "$ca_cert_path" ]; then
        ca_cert="$ca_cert_path"
    fi

    if [ -z "$ca_cert" ]; then
        error "No CA certificate found for registry"
        error "Expected CA certificate at: ${SCRIPT_DIR}/data/mirror-registry-ca.pem"
        error "Cannot proceed without proper TLS verification"
        exit 1
    fi

    log "Using CA certificate for TLS verification: ${ca_cert}"

    # Set SSL_CERT_FILE environment variable for oc-mirror to use the CA cert
    export SSL_CERT_FILE="${ca_cert}"

    # Get credentials from registry config and login to podman
    local registry_username=$(grep -o '"username": *"[^"]*"' "${registry_config}" | sed 's/.*: *"\([^"]*\)".*/\1/')
    local registry_password=$(grep -o '"password": *"[^"]*"' "${registry_config}" | sed 's/.*: *"\([^"]*\)".*/\1/')

    if [ -z "$registry_username" ] || [ -z "$registry_password" ]; then
        error "Registry credentials not found in ${registry_config}"
        error "Cannot authenticate to registry"
        exit 1
    fi

    log "Authenticating to registry..."
    if ! echo "${registry_password}" | podman login --tls-verify=false "${registry_url}" -u "${registry_username}" --password-stdin 2>/dev/null; then
        error "Failed to authenticate to registry ${registry_url}"
        error "Username: ${registry_username}"
        exit 1
    fi

    # Find podman auth file for oc-mirror
    local auth_file=""
    if [ -f "${XDG_RUNTIME_DIR}/containers/auth.json" ]; then
        auth_file="${XDG_RUNTIME_DIR}/containers/auth.json"
    elif [ -f "${HOME}/.config/containers/auth.json" ]; then
        auth_file="${HOME}/.config/containers/auth.json"
    elif [ -f "${HOME}/.docker/config.json" ]; then
        auth_file="${HOME}/.docker/config.json"
    fi

    if [ -n "$auth_file" ] && [ -f "$auth_file" ]; then
        log "Using auth file: ${auth_file}"
        oc_mirror_args="${oc_mirror_args} --authfile ${auth_file}"
    else
        error "Auth file not found after podman login"
        exit 1
    fi

    # Check if archives directory exists and has tar files
    if [ ! -d "${SCRIPT_DIR}/archives" ]; then
        error "Archives directory not found: ${SCRIPT_DIR}/archives"
        exit 1
    fi

    # Verify there are mirror tar files
    local mirror_tar=$(ls -1 "${SCRIPT_DIR}/archives"/mirror_*.tar 2>/dev/null | head -1)
    if [ -z "$mirror_tar" ]; then
        error "No oc-mirror tar files found in archives/"
        exit 1
    fi

    log "Found mirror archives in: ${SCRIPT_DIR}/archives"
    local archives_dir="${SCRIPT_DIR}/archives"

    # Check if ImageSetConfiguration exists
    local imageset_config="${SCRIPT_DIR}/imageset-config.yaml"
    if [ ! -f "${imageset_config}" ]; then
        error "ImageSetConfiguration not found: ${imageset_config}"
        error "This file is required for oc-mirror"
        exit 1
    fi

    # Create working directory for mirror
    local work_dir="${SCRIPT_DIR}/data/oc-mirror-workspace"
    mkdir -p "${work_dir}"

    log "Mirroring to ${registry_url}..."
    log "This may take several minutes depending on the number of images..."

    # Run oc-mirror from the archives directory to the registry
    cd "${work_dir}"
    if oc-mirror --v2 --from "file://${archives_dir}" --config "${imageset_config}" ${oc_mirror_args} "docker://${registry_url}"; then
        log "✓ Successfully mirrored images to ${registry_url}"

        # Find and copy IDMS/ITMS files from oc-mirror output
        # oc-mirror v2 creates files in archives/working-dir/cluster-resources/
        log "Searching for IDMS/ITMS files..."

        # Search in multiple possible locations
        local search_paths=(
            "${archives_dir}/working-dir/cluster-resources"
            "${work_dir}/cluster-resources"
            "${work_dir}"
        )

        local idms_file=""
        local itms_file=""

        for search_path in "${search_paths[@]}"; do
            if [ -d "${search_path}" ]; then
                log "Checking ${search_path}"

                # Look for IDMS file
                if [ -z "${idms_file}" ]; then
                    idms_file=$(find "${search_path}" -maxdepth 2 -type f \( -name "*idms*.yaml" -o -name "*imageDigestMirrorSet*.yaml" \) 2>/dev/null | head -1)
                fi

                # Look for ITMS file
                if [ -z "${itms_file}" ]; then
                    itms_file=$(find "${search_path}" -maxdepth 2 -type f \( -name "*itms*.yaml" -o -name "*imageTagMirrorSet*.yaml" \) 2>/dev/null | head -1)
                fi
            fi
        done

        # Copy IDMS file if found
        if [ -n "${idms_file}" ] && [ -f "${idms_file}" ]; then
            log "Found IDMS file: ${idms_file}"
            cp "${idms_file}" "${DATA_DIR}/imageDigestMirrorSet.yaml"
            chmod 644 "${DATA_DIR}/imageDigestMirrorSet.yaml"
            log "✓ Copied IDMS file to ${DATA_DIR}/imageDigestMirrorSet.yaml"
        else
            log "⚠ No IDMS file found"
        fi

        # Copy ITMS file if found
        if [ -n "${itms_file}" ] && [ -f "${itms_file}" ]; then
            log "Found ITMS file: ${itms_file}"
            cp "${itms_file}" "${DATA_DIR}/imageTagMirrorSet.yaml"
            chmod 644 "${DATA_DIR}/imageTagMirrorSet.yaml"
            log "✓ Copied ITMS file to ${DATA_DIR}/imageTagMirrorSet.yaml"
        else
            log "⚠ No ITMS file found"
        fi

        # Check for IDMS/ITMS files and display instructions
        if [ -f "${DATA_DIR}/imageDigestMirrorSet.yaml" ] || [ -f "${DATA_DIR}/imageTagMirrorSet.yaml" ]; then
            log ""
            log "Mirror complete! To configure your cluster to use the mirrored content:"
            log ""
            if [ -f "${DATA_DIR}/imageDigestMirrorSet.yaml" ]; then
                log "  kubectl apply -f ${DATA_DIR}/imageDigestMirrorSet.yaml"
            fi
            if [ -f "${DATA_DIR}/imageTagMirrorSet.yaml" ]; then
                log "  kubectl apply -f ${DATA_DIR}/imageTagMirrorSet.yaml"
            fi
            log ""
        fi
    else
        error "oc-mirror failed"
        return 1
    fi

    cd "${SCRIPT_DIR}"
}

clean_up() {
    log "Cleaning up Airgap Architect (preserving data in script directory)..."

    stop_containers

    log "Removing images..."
    podman rmi "${FRONTEND_IMAGE}" 2>/dev/null || true
    podman rmi "${BACKEND_IMAGE}" 2>/dev/null || true

    log "Cleanup complete (data preserved in ${SCRIPT_DIR})"
}

uninstall_mirror_registry() {
    log ""
    log "=== Uninstalling Mirror-Registry ==="
    log ""

    # Check if mirror-registry is installed
    if ! check_mirror_registry; then
        log "Mirror-registry is not installed or not running"
        return 0
    fi

    # Load existing configuration to find the mirror-registry binary
    local config_file=""
    local config_dir="${HOME}/.config/airgap-architect"

    if [ -f "${config_dir}/mirror-registry-config.json" ]; then
        config_file="${config_dir}/mirror-registry-config.json"
    elif [ -f "${DATA_DIR}/mirror-registry-config.json" ]; then
        config_file="${DATA_DIR}/mirror-registry-config.json"
    fi

    local data_path=""
    local saved_install_dir=""
    if [ -n "${config_file}" ]; then
        data_path=$(grep -o '"dataPath": *"[^"]*"' "${config_file}" | sed 's/.*: *"\([^"]*\)".*/\1/')
        saved_install_dir=$(grep -o '"installDir": *"[^"]*"' "${config_file}" | sed 's/.*: *"\([^"]*\)".*/\1/')
        log "Found existing mirror-registry at: ${data_path}"
        [ -n "${saved_install_dir}" ] && log "Installation directory: ${saved_install_dir}"
    fi

    # Confirm uninstall
    log ""
    log "WARNING: This will remove the mirror-registry installation and all data."
    log "Data path: ${data_path:-unknown}"
    echo ""
    read -p "Are you sure you want to uninstall mirror-registry? [y/N]: " confirm_uninstall

    if [ "${confirm_uninstall}" != "y" ] && [ "${confirm_uninstall}" != "Y" ]; then
        log "Uninstall cancelled"
        return 0
    fi

    # Expand tilde in data_path if present
    if [ -n "${data_path}" ]; then
        # Expand ~ to actual home directory
        data_path="${data_path/#\~/$HOME}"
    fi

    # Try to find mirror-registry binary in common locations
    local mirror_registry_bin=""
    local temp_dir=""  # Declare here so it's in scope for cleanup

    # Prefer the saved install directory from config (has all supporting files)
    if [ -n "${saved_install_dir}" ] && [ -f "${saved_install_dir}/mirror-registry" ]; then
        mirror_registry_bin="${saved_install_dir}/mirror-registry"
        log "Using mirror-registry from saved install directory: ${saved_install_dir}"
    # Check in data path's parent (where mirror-registry installer usually stays)
    elif [ -n "${data_path}" ] && [ -f "$(dirname "${data_path}")/mirror-registry" ]; then
        mirror_registry_bin="$(dirname "${data_path}")/mirror-registry"
    # Check in CLI install directory (but this won't have execution-environment.tar)
    elif [ -f "${CLI_INSTALL_DIR}/mirror-registry" ]; then
        log "WARNING: Found mirror-registry in ${CLI_INSTALL_DIR} but it may lack supporting files"
        mirror_registry_bin="${CLI_INSTALL_DIR}/mirror-registry"
    # Check in user's home
    elif [ -f "${HOME}/mirror-registry" ]; then
        mirror_registry_bin="${HOME}/mirror-registry"
    # Check if mirror-registry is in PATH
    elif command -v mirror-registry &>/dev/null; then
        log "WARNING: Found mirror-registry in PATH but it may lack supporting files"
        mirror_registry_bin=$(which mirror-registry)
    # Look for it in common bundle extraction locations
    elif [ -d "${CLI_TOOLS_DIR}" ] && [ -f "${CLI_TOOLS_DIR}/mirror-registry" ]; then
        mirror_registry_bin="${CLI_TOOLS_DIR}/mirror-registry"
    fi

    if [ -z "${mirror_registry_bin}" ]; then
        log "mirror-registry binary not found in common locations"
        log "Attempting to extract from bundle..."

        if [ ! -f "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" ]; then
            error "Cannot find mirror-registry binary or bundle"
            error "You may need to manually remove mirror-registry:"
            error "  1. Stop user services: systemctl --user stop quay-app.service"
            error "  2. Remove podman containers: podman rm -f quay-app quay-postgres quay-redis"
            error "  3. Remove data directory: rm -rf ${data_path:-~/quay-install}"
            return 1
        fi

        # Extract mirror-registry to temp location
        temp_dir="${SCRIPT_DIR}/.tmp-uninstall-$$"
        mkdir -p "${temp_dir}"

        if tar -xzf "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" -C "$temp_dir" 2>/dev/null; then
            : # Success
        elif tar -xf "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" -C "$temp_dir" 2>/dev/null; then
            : # Success
        elif file "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" 2>/dev/null | grep -q "executable"; then
            cp "${CLI_TOOLS_DIR}/mirror-registry.tar.gz" "$temp_dir/mirror-registry"
        else
            error "Failed to extract mirror-registry"
            rm -rf "$temp_dir"
            return 1
        fi

        if [ -f "$temp_dir/mirror-registry" ]; then
            chmod +x "$temp_dir/mirror-registry"
            mirror_registry_bin="$temp_dir/mirror-registry"
            log "Extracted mirror-registry for uninstall"
        else
            error "Could not extract mirror-registry binary"
            rm -rf "$temp_dir"
            return 1
        fi
    fi

    log "Using mirror-registry binary: ${mirror_registry_bin}"

    # Build uninstall command arguments
    local uninstall_args=""
    if [ -n "${data_path}" ]; then
        uninstall_args="--quayRoot ${data_path}"
    fi

    log "Running mirror-registry uninstall..."
    log "This will:"
    log "  - Stop all Quay containers"
    log "  - Remove systemd service"
    log "  - Delete data directory: ${data_path:-/opt/quay}"
    log ""

    # In STIG mode, we may need sudo for some operations
    if [ "${STIG_DETECTED}" = true ]; then
        request_sudo_access || return 1
    fi

    # Run uninstall - must be run from the directory containing all mirror-registry files
    local uninstall_result=0
    local original_dir=$(pwd)

    # If we extracted to temp_dir, cd there to run the command
    if [ -n "${temp_dir}" ] && [ -d "${temp_dir}" ]; then
        cd "${temp_dir}"
    elif [ -n "${mirror_registry_bin}" ]; then
        # If using an existing binary, cd to its directory
        cd "$(dirname "${mirror_registry_bin}")"
    fi

    # Run the uninstall command
    if ./mirror-registry uninstall ${uninstall_args} -v; then
        cd "${original_dir}"
        log "✓ Mirror-registry uninstalled successfully"

        # Clean up configuration files
        log "Removing configuration files..."
        rm -f "${DATA_DIR}/mirror-registry-config.json" 2>/dev/null || true
        rm -f "${DATA_DIR}/mirror-registry-credentials.txt" 2>/dev/null || true
        rm -f "${DATA_DIR}/mirror-registry-ca.pem" 2>/dev/null || true
        rm -f "${DATA_DIR}/mirror-registry-external.txt" 2>/dev/null || true

        rm -f "${config_dir}/mirror-registry-config.json" 2>/dev/null || true
        rm -f "${config_dir}/mirror-registry-credentials.txt" 2>/dev/null || true
        rm -f "${config_dir}/mirror-registry-ca.pem" 2>/dev/null || true
        rm -f "${config_dir}/mirror-registry-external.txt" 2>/dev/null || true

        log "✓ Configuration files removed"

        # Remove temporary extraction directory if we created one
        if [ -n "${temp_dir}" ] && [ -d "${temp_dir}" ]; then
            rm -rf "${temp_dir}"
        fi

        log ""
        log "Mirror-registry has been completely uninstalled."
        log "To reinstall, run: $0 start"

    else
        cd "${original_dir}"
        error "Mirror-registry uninstall failed"

        # Clean up temp dir on failure
        if [ -n "${temp_dir}" ] && [ -d "${temp_dir}" ]; then
            rm -rf "${temp_dir}"
        fi

        return 1
    fi

    # Clean up STIG environment if needed
    if [ "${STIG_DETECTED}" = true ]; then
        stop_sudo_keepalive
    fi
}

# Parse arguments
COMMAND=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --bundle-dir)
            BUNDLE_DIR="$2"
            shift 2
            ;;
        --data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        start|stop|restart|status|logs|mirror|clean|uninstall-mirror-registry)
            COMMAND="$1"
            shift
            break
            ;;
        *)
            error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Default to start if no command specified
COMMAND="${COMMAND:-start}"

check_podman

case "${COMMAND}" in
    start)
        start_containers
        ;;
    stop)
        stop_containers
        ;;
    restart)
        restart_containers
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$@"
        ;;
    mirror)
        mirror_to_registry
        ;;
    clean)
        clean_up
        ;;
    uninstall-mirror-registry)
        uninstall_mirror_registry
        ;;
    *)
        error "Unknown command: ${COMMAND}"
        usage
        exit 1
        ;;
esac
