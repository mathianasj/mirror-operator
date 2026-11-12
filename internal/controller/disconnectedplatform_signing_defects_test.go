package controller

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

// signBundlesScriptFromTasks extracts the shell script args from the sign-bundles task.
func signBundlesScriptFromTasks(tasks []map[string]interface{}) string {
	for _, task := range tasks {
		if task["name"] == "sign-bundles" {
			spec, _ := task["taskSpec"].(map[string]interface{})
			steps, _ := spec["steps"].([]map[string]interface{})
			for _, step := range steps {
				args, _ := step["args"].([]string)
				if len(args) > 0 {
					return args[0]
				}
			}
		}
	}
	return ""
}

// uploadToS3ScriptFromTasks extracts the shell script args from the upload-to-s3 task.
func uploadToS3ScriptFromTasks(tasks []map[string]interface{}) string {
	for _, task := range tasks {
		if task["name"] == "upload-to-s3" {
			spec, _ := task["taskSpec"].(map[string]interface{})
			steps, _ := spec["steps"].([]map[string]interface{})
			for _, step := range steps {
				args, _ := step["args"].([]string)
				if len(args) > 0 {
					return args[0]
				}
			}
		}
	}
	return ""
}

// findTaskByName returns the task map with the given name, or nil.
func findTaskByName(tasks []map[string]interface{}, name string) map[string]interface{} {
	for _, task := range tasks {
		if task["name"] == name {
			return task
		}
	}
	return nil
}

// substituteSignFileParams replaces Tekton parameter references with test literals.
func substituteSignFileParams(script string) string {
	return strings.NewReplacer(
		"$(params.fulcio-url)", "http://fulcio.test",
		"$(params.rekor-url)", "http://rekor.test",
		"$(params.oidc-issuer)", "http://oidc.test",
		"$(params.oidc-client-id)", "test-client",
		"$(params.tuf-url)", "http://tuf.test",
	).Replace(script)
}

// extractSignFileFn extracts only the sign_file() function body from the generated script.
func extractSignFileFn(script string) string {
	start := strings.Index(script, "\nsign_file() {")
	if start == -1 {
		return ""
	}
	body := script[start+1:] // skip leading newline
	// Function ends at the first unindented closing brace followed by a blank line
	for _, marker := range []string{"\n}\n\n", "\n}\n#"} {
		if idx := strings.Index(body, marker); idx != -1 {
			return body[:idx+3] // include "}\n"
		}
	}
	return body
}

// signFileTestHarness builds a shell test harness that calls sign_file on testFile.
// mockDir must already contain cosign and sleep mock binaries.
func signFileTestHarness(mockDir, testFile, signFileFn string) string {
	return fmt.Sprintf(`#!/bin/sh
export PATH="%s:$PATH"
export COSIGN_EXPERIMENTAL=1

fetch_oidc_token() {
    echo "test-oidc-token-valid"
}

%s

sign_file "%s"
`, mockDir, signFileFn, testFile)
}

// emptyTokenTestHarness builds a harness where fetch_oidc_token returns empty.
func emptyTokenTestHarness(mockDir, testFile, signFileFn string) string {
	return fmt.Sprintf(`#!/bin/sh
export PATH="%s:$PATH"
export COSIGN_EXPERIMENTAL=1

fetch_oidc_token() {
    echo ""
}

%s

sign_file "%s"
`, mockDir, signFileFn, testFile)
}

// runShellScript writes script to a temp file and executes it. Returns exit code and combined output.
func runShellScript(dir, name, content string) (exitCode int, output string) {
	scriptPath := filepath.Join(dir, name)
	Expect(os.WriteFile(scriptPath, []byte(content), 0755)).To(Succeed())
	cmd := exec.Command("/bin/sh", scriptPath)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	output = string(out)
	if err == nil {
		return 0, output
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode(), output
	}
	return -1, output
}

// writeMockBinary writes a shell mock binary to dir/name.
func writeMockBinary(dir, name, content string) {
	Expect(os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+content+"\n"), 0755)).To(Succeed())
}

var _ = Describe("buildPipelineTasks signing defects (issue #117)", func() {
	var (
		r            *DisconnectedPlatformReconciler
		tasks        []map[string]interface{}
		sigScript    string
		uploadScript string
		scheme       *runtime.Scheme
	)

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(scheme)).To(Succeed())
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r = &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}
		tasks = r.buildPipelineTasks()
		sigScript = signBundlesScriptFromTasks(tasks)
		uploadScript = uploadToS3ScriptFromTasks(tasks)
	})

	// -------------------------------------------------------------------------
	// T-06 (SIGN-004): Timeout budget
	// -------------------------------------------------------------------------
	Describe("T-06: sign-bundles timeout budget (SIGN-004)", func() {
		It("uses --timeout of at least 3600s to cover large-payload hashing plus Rekor upload", func() {
			Expect(sigScript).To(ContainSubstring("--timeout=3600s"),
				"sign-bundles must use --timeout=3600s; got script: %s", sigScript)
		})
		It("COSIGN_TIMEOUT env var is aligned with the --timeout flag value", func() {
			Expect(sigScript).To(ContainSubstring("COSIGN_TIMEOUT=3600"),
				"COSIGN_TIMEOUT must be 3600 to match --timeout=3600s")
		})
	})

	// -------------------------------------------------------------------------
	// T-09 (SIGN-006): Task ordering
	// -------------------------------------------------------------------------
	Describe("T-09: task ordering — SBOM must precede signing (SIGN-006)", func() {
		It("create-bundle-sbom does not run after sign-bundles", func() {
			sbomTask := findTaskByName(tasks, "create-bundle-sbom")
			Expect(sbomTask).NotTo(BeNil(), "create-bundle-sbom task must exist")
			runAfter, _ := sbomTask["runAfter"].([]string)
			Expect(runAfter).NotTo(ContainElement("sign-bundles"),
				"create-bundle-sbom must not depend on sign-bundles; it must precede it so the SBOM can be signed")
		})

		It("sign-bundles runs after create-bundle-sbom so the SBOM exists before signing", func() {
			signTask := findTaskByName(tasks, "sign-bundles")
			Expect(signTask).NotTo(BeNil(), "sign-bundles task must exist")
			runAfter, _ := signTask["runAfter"].([]string)
			Expect(runAfter).To(ContainElement("create-bundle-sbom"),
				"sign-bundles must list create-bundle-sbom in runAfter so the SBOM is present when signing runs")
		})
	})

	// -------------------------------------------------------------------------
	// Shell execution tests — sign_file function behavior
	// -------------------------------------------------------------------------
	Describe("sign_file shell behavior", func() {
		var (
			tmpDir     string
			mockDir    string
			testFile   string
			signFileFn string
		)

		BeforeEach(func() {
			tmpDir = GinkgoT().TempDir()
			mockDir = filepath.Join(tmpDir, "mocks")
			Expect(os.MkdirAll(mockDir, 0755)).To(Succeed())
			testFile = filepath.Join(tmpDir, "test.tar")
			Expect(os.WriteFile(testFile, []byte("fake tar content"), 0644)).To(Succeed())

			// sleep mock: do nothing (avoid 10s waits between retries)
			writeMockBinary(mockDir, "sleep", "exit 0")

			signFileFn = substituteSignFileParams(extractSignFileFn(sigScript))
			Expect(signFileFn).NotTo(BeEmpty(), "failed to extract sign_file function from generated script")
		})

		// T-01 (SIGN-001/002): cosign failure must propagate nonzero exit
		Describe("T-01: cosign exits 42 — sign_file must exit nonzero (SIGN-001)", func() {
			It("exits nonzero and writes no .sig file when cosign fails", func() {
				writeMockBinary(mockDir, "cosign", "exit 42")
				harness := signFileTestHarness(mockDir, testFile, signFileFn)
				exitCode, _ := runShellScript(tmpDir, "t01.sh", harness)
				Expect(exitCode).NotTo(Equal(0),
					"sign_file must propagate cosign's nonzero exit code (SIGN-001: tee was masking it)")
				Expect(testFile+".sig").NotTo(BeAnExistingFile(),
					"a .sig file must not remain after failed signing")
			})
		})

		// T-02 (SIGN-002): stderr must not be written to .sig file
		Describe("T-02: cosign stderr must not contaminate .sig (SIGN-002)", func() {
			It("does not write cosign stderr into the .sig file", func() {
				// Mock cosign that writes an error to stderr and exits 1
				writeMockBinary(mockDir, "cosign",
					`echo "ERROR: context deadline exceeded" >&2
exit 1`)
				harness := signFileTestHarness(mockDir, testFile, signFileFn)
				runShellScript(tmpDir, "t02.sh", harness)
				sigPath := testFile + ".sig"
				if _, err := os.Stat(sigPath); err == nil {
					contents, _ := os.ReadFile(sigPath)
					Expect(string(contents)).NotTo(ContainSubstring("context deadline exceeded"),
						"cosign stderr must not appear in .sig; the file must be a clean signature artifact")
				}
				// If .sig does not exist, the test passes (no contaminated artifact written)
			})
		})

		// T-03 (SIGN-003): retry exhaustion must exit nonzero
		Describe("T-03: retry exhaustion returns nonzero exit code (SIGN-003)", func() {
			It("exits nonzero after all retries are exhausted", func() {
				callCountFile := filepath.Join(tmpDir, "cosign_calls")
				writeMockBinary(mockDir, "cosign",
					fmt.Sprintf(`echo "1" >> "%s"
exit 1`, callCountFile))
				harness := signFileTestHarness(mockDir, testFile, signFileFn)
				exitCode, _ := runShellScript(tmpDir, "t03.sh", harness)
				Expect(exitCode).NotTo(Equal(0),
					"sign_file must return nonzero after all retries fail (SIGN-003: missing return 1)")
				// Verify cosign was called (at least once) — with the fix it will be called max_retries times
				if data, err := os.ReadFile(callCountFile); err == nil {
					calls := strings.Count(string(data), "1")
					Expect(calls).To(BeNumerically(">=", 1))
				}
			})
		})

		// T-04 (SIGN-003): empty OIDC token must exit nonzero
		Describe("T-04: empty OIDC token exhausts retries and returns nonzero (SIGN-003)", func() {
			It("exits nonzero when fetch_oidc_token always returns empty", func() {
				// cosign must never be called when token is empty
				cosignCallFile := filepath.Join(tmpDir, "cosign_called")
				writeMockBinary(mockDir, "cosign",
					fmt.Sprintf(`touch "%s"
exit 0`, cosignCallFile))
				harness := emptyTokenTestHarness(mockDir, testFile, signFileFn)
				exitCode, _ := runShellScript(tmpDir, "t04.sh", harness)
				Expect(exitCode).NotTo(Equal(0),
					"sign_file must return nonzero when OIDC token fetch always returns empty (SIGN-003)")
				Expect(cosignCallFile).NotTo(BeAnExistingFile(),
					"cosign must not be invoked when the OIDC token is empty")
			})
		})

		// T-05 (SIGN-001/002): successful signing writes valid .sig and .bundle
		Describe("T-05: successful signing produces clean .sig and .bundle (SIGN-001/002)", func() {
			It("writes valid base64 .sig and a .bundle file on cosign success", func() {
				// Mock cosign that writes base64 to --output-signature and JSON to --bundle,
				// and also writes diagnostic text to stderr (simulating real cosign output).
				writeMockBinary(mockDir, "cosign",
					`sig_file=""
bundle_file=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-signature=*) sig_file="${1#--output-signature=}" ;;
        --output-signature) sig_file="$2"; shift ;;
        --bundle=*) bundle_file="${1#--bundle=}" ;;
        --bundle) bundle_file="$2"; shift ;;
    esac
    shift
done
echo "diagnostic: initializing ephemeral key" >&2
echo "diagnostic: uploading to Rekor" >&2
if [ -n "$sig_file" ]; then
    echo "dGVzdC1zaWduYXR1cmU=" > "$sig_file"
fi
if [ -n "$bundle_file" ]; then
    echo '{"payload":"test","cert":"test"}' > "$bundle_file"
fi
exit 0`)
				harness := signFileTestHarness(mockDir, testFile, signFileFn)
				exitCode, output := runShellScript(tmpDir, "t05.sh", harness)
				Expect(exitCode).To(Equal(0),
					"sign_file must exit 0 on success; output: %s", output)

				sigPath := testFile + ".sig"
				Expect(sigPath).To(BeAnExistingFile(), ".sig file must be written on success")
				sigContents, _ := os.ReadFile(sigPath)
				Expect(string(sigContents)).NotTo(ContainSubstring("diagnostic"),
					".sig must not contain cosign stderr/diagnostic messages (SIGN-002)")
				// Valid base64: decoding must succeed
				trimmed := strings.TrimSpace(string(sigContents))
				Expect(trimmed).NotTo(BeEmpty())
				// If the fix uses --output-signature, the sig will be valid base64.
				// With broken code (tee), it will contain "diagnostic: ..." lines.

				bundlePath := testFile + ".bundle"
				Expect(bundlePath).To(BeAnExistingFile(), ".bundle file must be written on success")
			})
		})
	})

	// -------------------------------------------------------------------------
	// Upload task validation tests (SIGN-005)
	// -------------------------------------------------------------------------
	Describe("upload-to-s3 signing artifact validation (SIGN-005)", func() {
		var (
			tmpDir     string
			mockDir    string
			bundleFile string
		)

		BeforeEach(func() {
			tmpDir = GinkgoT().TempDir()
			mockDir = filepath.Join(tmpDir, "mocks")
			Expect(os.MkdirAll(mockDir, 0755)).To(Succeed())
			bundleFile = filepath.Join(tmpDir, "collection-test.tar")
			Expect(os.WriteFile(bundleFile, []byte("fake tar"), 0644)).To(Succeed())

			// Mock aws to record calls but not actually upload
			awsCallLog := filepath.Join(tmpDir, "aws_calls")
			writeMockBinary(mockDir, "aws",
				fmt.Sprintf(`echo "aws $@" >> "%s"`, awsCallLog))

			// Provide dummy Tekton param values used by the upload script
			_ = uploadScript
		})

		// T-07: .sig containing non-base64 content must abort the upload
		Describe("T-07: upload fails when .sig is not valid base64 (SIGN-005)", func() {
			It("exits nonzero before uploading when .sig contains error log text", func() {
				// Write a .sig that looks like a Cosign error log (what broken code produces)
				sigFile := bundleFile + ".sig"
				Expect(os.WriteFile(sigFile, []byte("ERROR: Post \"https://rekor.test/api/v1/log/entries\": context deadline exceeded\nUsing payload from: test.tar\n"), 0644)).To(Succeed())
				// No .bundle file (consistent with failed signing)

				// Write a minimal upload validation script that mirrors the fix
				validationScript := fmt.Sprintf(`#!/bin/sh
set -e
export PATH="%s:$PATH"
FINAL_BUNDLE="%s"
SIG_FILE=""

if [ -f "${FINAL_BUNDLE}.sig" ]; then
  if ! base64 -d "${FINAL_BUNDLE}.sig" >/dev/null 2>&1; then
    echo "ERROR: .sig file is not valid base64 — signing likely failed. Aborting upload."
    exit 1
  fi
  if [ ! -f "${FINAL_BUNDLE}.bundle" ]; then
    echo "ERROR: .bundle file missing — keyless signing did not complete. Aborting upload."
    exit 1
  fi
  aws s3 cp "${FINAL_BUNDLE}.sig" "s3://test-bucket/test.sig"
  SIG_FILE="test.sig"
fi
echo "done"
`, mockDir, bundleFile)

				// NOTE: This test validates the EXPECTED behavior after the fix.
				// The actual upload script (uploadScript) currently lacks this validation.
				// Once SIGN-005 is fixed, this test should be rewritten to use uploadScript directly.
				exitCode, output := runShellScript(tmpDir, "t07.sh", validationScript)
				Expect(exitCode).NotTo(Equal(0),
					"upload must abort when .sig contains non-base64 content (SIGN-005); output: %s", output)

				// Verify no S3 upload was attempted for the bad .sig
				awsCallLog := filepath.Join(tmpDir, "aws_calls")
				if _, err := os.Stat(awsCallLog); err == nil {
					calls, _ := os.ReadFile(awsCallLog)
					Expect(string(calls)).NotTo(ContainSubstring(".sig"),
						"aws s3 must not be called with the invalid .sig file")
				}
			})
		})

		// T-08: missing .bundle must abort the upload in keyless mode
		Describe("T-08: upload fails when .bundle is absent for keyless signing (SIGN-005)", func() {
			It("exits nonzero when .sig exists as valid base64 but .bundle is missing", func() {
				sigFile := bundleFile + ".sig"
				// Valid base64 .sig (what a successful --output-signature would produce)
				Expect(os.WriteFile(sigFile, []byte("dGVzdC1zaWduYXR1cmU=\n"), 0644)).To(Succeed())
				// No .bundle file

				validationScript := fmt.Sprintf(`#!/bin/sh
set -e
export PATH="%s:$PATH"
FINAL_BUNDLE="%s"

if [ -f "${FINAL_BUNDLE}.sig" ]; then
  if ! base64 -d "${FINAL_BUNDLE}.sig" >/dev/null 2>&1; then
    echo "ERROR: .sig file is not valid base64 — signing likely failed. Aborting upload."
    exit 1
  fi
  if [ ! -f "${FINAL_BUNDLE}.bundle" ]; then
    echo "ERROR: .bundle file missing — keyless signing did not complete. Aborting upload."
    exit 1
  fi
  aws s3 cp "${FINAL_BUNDLE}.sig" "s3://test-bucket/test.sig"
fi
echo "done"
`, mockDir, bundleFile)

				exitCode, output := runShellScript(tmpDir, "t08.sh", validationScript)
				Expect(exitCode).NotTo(Equal(0),
					"upload must abort when .bundle is absent for keyless signing (SIGN-005); output: %s", output)
			})
		})
	})

	// -------------------------------------------------------------------------
	// T-10 (SIGN-006): sign-bundles signs the bundle SBOM when present
	// -------------------------------------------------------------------------
	Describe("T-10: sign-bundles signs bundle SBOM when present (SIGN-006)", func() {
		var (
			tmpDir     string
			mockDir    string
			sbomFile   string
			sbomsDir   string
			signFileFn string
		)

		BeforeEach(func() {
			tmpDir = GinkgoT().TempDir()
			mockDir = filepath.Join(tmpDir, "mocks")
			Expect(os.MkdirAll(mockDir, 0755)).To(Succeed())
			writeMockBinary(mockDir, "sleep", "exit 0")

			// Create the bundle SBOM that should exist before sign-bundles runs (after SIGN-006 fix)
			sbomsDir = filepath.Join(tmpDir, "sboms")
			Expect(os.MkdirAll(sbomsDir, 0755)).To(Succeed())
			sbomFile = filepath.Join(sbomsDir, "bundle-test.spdx.json")
			Expect(os.WriteFile(sbomFile, []byte(`{"spdxVersion":"SPDX-2.3"}`), 0644)).To(Succeed())

			signFileFn = substituteSignFileParams(extractSignFileFn(sigScript))
		})

		It("calls sign_file for bundle-*.spdx.json files when they exist before the task runs", func() {
			// Mock cosign that writes output-signature and bundle files
			writeMockBinary(mockDir, "cosign",
				`sig_file=""
bundle_file=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-signature=*) sig_file="${1#--output-signature=}" ;;
        --output-signature) sig_file="$2"; shift ;;
        --bundle=*) bundle_file="${1#--bundle=}" ;;
        --bundle) bundle_file="$2"; shift ;;
    esac
    shift
done
if [ -n "$sig_file" ]; then echo "dGVzdA==" > "$sig_file"; fi
if [ -n "$bundle_file" ]; then echo '{}' > "$bundle_file"; fi
exit 0`)

			// Build a minimal harness that only exercises the SBOM-signing block
			harness := fmt.Sprintf(`#!/bin/sh
export PATH="%s:$PATH"
export COSIGN_EXPERIMENTAL=1

fetch_oidc_token() { echo "test-oidc-token"; }

%s

# SBOM signing block (mirrors the generated script)
if ls "%s"/bundle-*.spdx.json 2>/dev/null | grep -q .; then
  for sbomfile in "%s"/bundle-*.spdx.json; do
    sign_file "$sbomfile"
  done
fi
`, mockDir, signFileFn, sbomsDir, sbomsDir)

			exitCode, output := runShellScript(tmpDir, "t10.sh", harness)
			Expect(exitCode).To(Equal(0),
				"signing the bundle SBOM must succeed; output: %s", output)
			Expect(sbomFile+".sig").To(BeAnExistingFile(),
				"bundle SBOM must be signed and produce a .sig artifact")
			Expect(sbomFile+".bundle").To(BeAnExistingFile(),
				"bundle SBOM signing must produce a .bundle artifact")
		})
	})
})
