"""Regression checks for CLI installation choices under Bash errexit."""

import io
import os
from pathlib import Path
import re
import shlex
import subprocess
import tarfile
import tempfile
import unittest


def script_source():
    return Path(os.environ.get(
        "ARCHITECT_IMPORT_SCRIPT",
        str(Path(__file__).with_name("import-airgap-architect.sh")),
    )).read_text()


def shell_function(source, name):
    lines = source.splitlines()
    start = next(i for i, line in enumerate(lines) if line == name + "() {")
    next_function = next(
        (i for i in range(start + 1, len(lines)) if re.match(r"^[a-zA-Z_][a-zA-Z_0-9]*\(\) \{", lines[i])),
        len(lines),
    )
    end = max(i for i in range(start + 1, next_function) if lines[i] == "}")
    return "\n".join(lines[start:end + 1])


class CLIInstallationChoices(unittest.TestCase):
    def setUp(self):
        self.function = shell_function(script_source(), "install_cli_tools")
        self.temporary = tempfile.TemporaryDirectory(prefix="architect-cli-test-")
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.install = root / "bin"
        self.bundle = root / "cli-tools"
        self.install.mkdir()
        self.bundle.mkdir()
        self.tools = ("oc", "kubectl", "openshift-install", "oc-mirror")
        for tool in self.tools:
            executable = self.install / tool
            executable.write_bytes(self.contents("existing", tool))
            executable.chmod(0o755)
        archives = {
            "openshift-client-linux.tar.gz": ("oc", "kubectl"),
            "openshift-install-linux.tar.gz": ("openshift-install",),
            "oc-mirror.tar.gz": ("oc-mirror",),
        }
        for name, tools in archives.items():
            with tarfile.open(self.bundle / name, "w:gz") as archive:
                for tool in tools:
                    content = self.contents("bundled", tool)
                    entry = tarfile.TarInfo(tool)
                    entry.size = len(content)
                    entry.mode = 0o755
                    archive.addfile(entry, io.BytesIO(content))

    @staticmethod
    def contents(kind, tool):
        return f"#!/bin/sh\n# {kind} {tool}\nexit 0\n".encode()

    def run_installer(self, choices):
        code = "\n".join([
            "set -euo pipefail",
            'log() { printf "%s\\n" "$*"; }',
            "check_cli_tools() { return 0; }",
            "STIG_DETECTED=false",
            "CLI_INSTALL_DIR=" + shlex.quote(str(self.install)),
            "CLI_TOOLS_DIR=" + shlex.quote(str(self.bundle)),
            self.function,
            "install_cli_tools",
            'printf "STARTUP_CONTINUED\\n"',
        ])
        environment = dict(os.environ)
        environment["PATH"] = str(self.install) + os.pathsep + environment.get("PATH", "")
        return subprocess.run(
            ["bash", "-c", code],
            input="\n".join(choices) + "\n",
            text=True, capture_output=True, env=environment, timeout=30,
        )

    def assert_tools(self, kind):
        for tool in self.tools:
            self.assertEqual((self.install / tool).read_bytes(), self.contents(kind, tool))

    def test_skip_preserves_tools_and_continues_startup(self):
        result = self.run_installer(["s", "s", "s"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("STARTUP_CONTINUED", result.stdout)
        self.assert_tools("existing")

    def test_default_or_invalid_choices_skip_and_continue(self):
        for choice in ("", "invalid", "S"):
            with self.subTest(choice=choice):
                result = self.run_installer([choice] * 3)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("STARTUP_CONTINUED", result.stdout)
                self.assert_tools("existing")

    def test_overwrite_installs_bundled_tools(self):
        result = self.run_installer(["o", "o", "o"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("STARTUP_CONTINUED", result.stdout)
        self.assert_tools("bundled")

    def test_overwrite_all_prompts_once(self):
        result = self.run_installer(["a"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("STARTUP_CONTINUED", result.stdout)
        self.assertEqual(result.stdout.count("Options:"), 1)
        self.assert_tools("bundled")

    def test_cancel_at_each_tool_stops_startup_cleanly(self):
        for position in range(3):
            with self.subTest(position=position):
                result = self.run_installer(["s"] * position + ["c"])
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertNotIn("STARTUP_CONTINUED", result.stdout)
                self.assertIn("Installation cancelled by user", result.stdout)
                self.assert_tools("existing")


class MirrorRegistryExecutionStorage(unittest.TestCase):
    def setUp(self):
        self.source = script_source()
        self.temporary = tempfile.TemporaryDirectory(prefix="architect-mount-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.data = self.root / "data with spaces"
        self.data.mkdir()
        self.install = self.root / "executable installer"
        self.base = "\n".join([
            "set -euo pipefail",
            'log() { printf "%s\\n" "$*"; }',
            'error() { printf "ERROR: %s\\n" "$*" >&2; }',
            "DATA_DIR=" + shlex.quote(str(self.data)),
            "MIRROR_REGISTRY_INSTALL_DIR=",
            shell_function(self.source, "installer_mount_options"),
            shell_function(self.source, "select_mirror_registry_install_dir"),
            shell_function(self.source, "prepare_mirror_registry_install_dir"),
        ])

    def run_shell(self, code):
        return subprocess.run(
            ["bash", "-c", self.base + "\n" + code],
            text=True, capture_output=True, timeout=30,
        )

    def test_executable_data_directory_keeps_existing_location(self):
        result = self.run_shell('findmnt() { printf "rw,nodev,relatime\\n"; }; select_mirror_registry_install_dir')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(self.data / "mirror-registry-install"))

    def test_noexec_data_chooses_executable_fallback(self):
        result = self.run_shell("\n".join([
            "findmnt() {",
            f'  if [ "${{!#}}" = {shlex.quote(str(self.data))} ]; then printf "rw,noexec,nodev\\n"; else printf "rw,nodev\\n"; fi',
            "}",
            "select_mirror_registry_install_dir",
        ]))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "/opt/airgap-architect/mirror-registry-install")
        self.assertIn("noexec", result.stderr)

    def test_noexec_explicit_override_is_rejected(self):
        result = self.run_shell("\n".join([
            "MIRROR_REGISTRY_INSTALL_DIR=" + shlex.quote(str(self.install)),
            'findmnt() { printf "rw,noexec\\n"; }',
            "select_mirror_registry_install_dir",
        ]))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("MIRROR_REGISTRY_INSTALL_DIR is on a noexec filesystem", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_noexec_fallback_is_rejected(self):
        result = self.run_shell('findmnt() { printf "rw,noexec\\n"; }; select_mirror_registry_install_dir')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Fallback installer directory is also noexec", result.stderr)

    def test_mount_lookup_failure_has_clear_error(self):
        result = self.run_shell('findmnt() { return 1; }; select_mirror_registry_install_dir')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Cannot determine mount options", result.stderr)

    def test_mount_lookup_checks_nearest_existing_ancestor(self):
        result = self.run_shell("\n".join([
            'findmnt() { printf "%s\\n" "${!#}"; }',
            "installer_mount_options " + shlex.quote(str(self.data / "missing" / "installer")),
        ]))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(self.data))

    def test_installer_runs_with_all_companion_files_at_override(self):
        bundle = self.root / "cli-tools"
        bundle.mkdir()
        marker = self.root / "installer-invoked"
        executable = ("#!/bin/bash\nset -e\n"
                      'for name in execution-environment.tar image-archive.tar sqlite3.tar; do test -f "$name"; done\n'
                      f'printf "%s\\n" "$PWD" > {shlex.quote(str(marker))}\n'
                      'test "$1" = install\n'
                      'while [ "$#" -gt 0 ]; do if [ "$1" = --quayHostname ]; then test "$2" = registry.example.test:8444; fi; shift; done\n'
                      "exit 1\n").encode()
        with tarfile.open(bundle / "mirror-registry.tar.gz", "w:gz") as archive:
            for name, contents in (("mirror-registry", executable), ("execution-environment.tar", b"fixture"),
                                   ("image-archive.tar", b"fixture"), ("sqlite3.tar", b"fixture")):
                entry = tarfile.TarInfo(name)
                entry.size = len(contents)
                entry.mode = 0o755 if name == "mirror-registry" else 0o644
                archive.addfile(entry, io.BytesIO(contents))
        result = self.run_shell("\n".join([
            "MIRROR_REGISTRY_INSTALL_DIR=" + shlex.quote(str(self.install)),
            "CLI_TOOLS_DIR=" + shlex.quote(str(bundle)),
            "MIRROR_REGISTRY_INSTALL=true",
            "MIRROR_REGISTRY_HOSTNAME=registry.example.test",
            "MIRROR_REGISTRY_PORT=8444",
            "MIRROR_REGISTRY_DATA_PATH=/unused",
            "MIRROR_REGISTRY_STORAGE=quay-storage",
            "MIRROR_REGISTRY_SQLITE_STORAGE=sqlite-storage",
            "MIRROR_REGISTRY_INIT_PASSWORD=fixture",
            "MIRROR_REGISTRY_SSL_CERT=",
            "MIRROR_REGISTRY_SSL_KEY=",
            "STIG_DETECTED=false",
            "check_cli_tools() { return 0; }",
            "check_mirror_registry() { return 1; }",
            "chcon() { return 0; }",
            'findmnt() { printf "rw,nodev\\n"; }',
            shell_function(self.source, "install_mirror_registry"),
            "install_mirror_registry",
        ]))
        # The fixture deliberately exits before real registry/configuration setup.
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertTrue(marker.exists(), result.stdout + result.stderr)
        self.assertEqual(marker.read_text().strip(), str(self.install))
        self.assertIn("Mirror-registry installation failed", result.stderr)
        self.assertFalse((self.data / "mirror-registry-install").exists())


class RegistryReadiness(unittest.TestCase):
    def run_check(self, names, health):
        code = "\n".join([
            "set -euo pipefail",
            "MIRROR_REGISTRY_HOSTNAME=registry.example.test",
            "MIRROR_REGISTRY_PORT=8444",
            "log() { :; }",
            'systemctl() { return 0; }',
            "podman() { test \"$1\" = ps; test \"$2\" = --format; printf '%s\\n' " + shlex.quote(names) + "; }",
            f'curl() {{ case " $* " in *" -f "*) return {health} ;; *) return 0 ;; esac; }}',
            shell_function(script_source(), "check_mirror_registry"),
            "check_mirror_registry",
        ])
        return subprocess.run(["bash", "-c", code], capture_output=True, text=True, timeout=30)

    def test_service_alone_is_not_running_registry(self):
        result = self.run_check("", 0)
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_running_container_with_unhealthy_endpoint_is_rejected(self):
        result = self.run_check("quay-app", 22)
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_running_container_with_healthy_endpoint_is_accepted(self):
        result = self.run_check("quay-app", 0)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_redis_alone_is_not_registry(self):
        result = self.run_check("quay-redis", 0)
        self.assertEqual(result.returncode, 1, result.stderr)


class OCMirrorTemporaryExecution(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="architect-oc-tmp-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.base = "\n".join([
            "set -euo pipefail",
            "TMPDIR=" + shlex.quote(str(self.root)),
            "OC_MIRROR_TMPDIR=",
            "log() { :; }",
            'error() { printf "%s\\n" "$*" >&2; }',
            shell_function(script_source(), "prepare_oc_mirror_tmpdir"),
        ])

    def run_check(self, code):
        return subprocess.run(["bash", "-c", self.base + "\n" + code], text=True, capture_output=True, timeout=30)

    def test_executable_tmp_is_preserved(self):
        result = self.run_check('installer_mount_options() { printf "rw,nodev\\n"; }; prepare_oc_mirror_tmpdir; printf "%s\\n" "$OC_MIRROR_TMPDIR"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(self.root))

    def test_noexec_override_fails_clearly(self):
        result = self.run_check('OC_MIRROR_TMPDIR="$TMPDIR"; installer_mount_options() { printf "rw,noexec\\n"; }; prepare_oc_mirror_tmpdir')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("OC_MIRROR_TMPDIR is on a noexec filesystem", result.stderr)

    def test_missing_executable_override_is_created(self):
        selected = self.root / "custom tmp"
        result = self.run_check("\n".join([
            "OC_MIRROR_TMPDIR=" + shlex.quote(str(selected)),
            'installer_mount_options() { printf "rw,nodev\\n"; }',
            'prepare_mirror_registry_install_dir() { mkdir -p "$1"; }',
            "prepare_oc_mirror_tmpdir",
            'printf "%s\\n" "$OC_MIRROR_TMPDIR"',
        ]))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(selected))
        self.assertTrue(selected.is_dir())

    def test_noexec_fallback_fails_clearly(self):
        result = self.run_check('installer_mount_options() { printf "rw,noexec\\n"; }; prepare_oc_mirror_tmpdir')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fallback directory is noexec", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
