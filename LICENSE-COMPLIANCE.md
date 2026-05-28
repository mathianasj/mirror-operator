# License Compliance Report

**Project**: mirror-operator  
**Date**: 2026-05-28  
**Analysis**: Go module dependencies

## Summary

✅ **No GPL, LGPL, or AGPL licenses found in runtime dependencies**

The mirror-operator uses only permissive open-source licenses that are compatible with commercial and proprietary use.

## License Distribution

| License Type | Count | Status |
|--------------|-------|--------|
| Apache-2.0 | ~1220 | ✅ Permissive |
| MIT | ~285 | ✅ Permissive |
| BSD (various) | ~9 | ✅ Permissive |
| MPL-2.0 | ~5 | ⚠️ Weak Copyleft |

## Key Dependencies

### Core Kubernetes/Controller Runtime
- **k8s.io/api** - Apache-2.0
- **k8s.io/apimachinery** - Apache-2.0
- **k8s.io/client-go** - Apache-2.0
- **sigs.k8s.io/controller-runtime** - Apache-2.0

### Tekton Pipelines
- **github.com/tektoncd/pipeline** - Apache-2.0

### Testing Frameworks
- **github.com/onsi/ginkgo/v2** - MIT
- **github.com/onsi/gomega** - MIT

### Other Major Dependencies
- **Go standard library** - BSD-3-Clause
- **Prometheus libraries** - Apache-2.0
- **OpenTelemetry** - Apache-2.0
- **gRPC** - Apache-2.0

## Mozilla Public License (MPL-2.0) Dependencies

A small number of dependencies use MPL-2.0, which is a **weak copyleft** license:

- **go-simpler.org/musttag** - MPL-2.0 (linting tool, dev-only)
- **hashicorp/go-*** packages - MPL-2.0 (indirect dependencies)

### MPL-2.0 Compliance Notes

MPL-2.0 is considered **safe for commercial use** because:
1. It only requires source code disclosure for modifications to MPL-licensed files themselves
2. It does NOT require disclosure of your own code that uses MPL libraries
3. It's explicitly designed to allow mixing with proprietary code
4. File-level copyleft (not project-level like GPL)

These packages are used as libraries and are not modified, so no additional compliance burden.

## Restrictive Licenses

### No GPL/LGPL/AGPL Found

Search for restrictive licenses yielded **ZERO** results in runtime dependencies:
- ❌ No GPL (GNU General Public License)
- ❌ No LGPL (GNU Lesser General Public License)  
- ❌ No AGPL (GNU Affero General Public License)

The grep search found references to "GPL" only in:
- Go toolchain's BoringSSL documentation (not a dependency license)
- Third-party tool licenses (golangci-lint) used only in development

## BoringCrypto Note

The Go toolchain includes BoringCrypto (FIPS-validated cryptography) which is:
- **License**: Mix of OpenSSL, ISC (permissive)
- **Usage**: Optional build mode (`GOEXPERIMENT=boringcrypto`)
- **Status**: ✅ Not restrictive, permissive licensing

## Commercial Use

✅ **The mirror-operator can be freely used in commercial/proprietary products**

All licenses are compatible with:
- Commercial distribution
- Proprietary modifications
- Closed-source derivatives
- SaaS/cloud offerings

## Compliance Requirements

### Apache-2.0 (Primary License)
- ✅ Include LICENSE and NOTICE files in distributions
- ✅ State significant changes if modifying Apache-licensed code
- ✅ Preserve copyright notices

### MIT License
- ✅ Include copyright notice and permission notice

### BSD Licenses
- ✅ Include copyright notice in redistributions

### MPL-2.0 (Minimal Copyleft)
- ✅ If you modify MPL-licensed files, you must make those modifications available
- ✅ Using as a library requires no source disclosure

## Verification Steps Taken

1. Analyzed all Go modules via `go list -m all`
2. Scanned ~1500 LICENSE files in `go/pkg/mod`
3. Searched for GPL/LGPL/AGPL keywords across all licenses
4. Manually reviewed flagged licenses
5. Confirmed runtime vs dev-only dependencies

## Recommendation

✅ **APPROVED FOR OPEN SOURCE CONSUMPTION**

The mirror-operator dependency tree contains only permissive and weak-copyleft licenses. There are no strong copyleft licenses (GPL/LGPL/AGPL) that would restrict commercial use or require source code disclosure.

Organizations can:
- Use this operator in commercial products
- Create proprietary derivatives
- Bundle with closed-source software
- Offer as part of SaaS platforms

## Additional Notes

- This analysis covers Go module dependencies only
- Container images may include additional software with separate licenses
- Always review licenses of any third-party tools called at runtime (e.g., oc-mirror binary)
- Red Hat/OpenShift specific components should be verified separately

## References

- [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
- [MIT License](https://opensource.org/licenses/MIT)
- [Mozilla Public License 2.0](https://www.mozilla.org/en-US/MPL/2.0/)
- [BSD Licenses](https://opensource.org/licenses/BSD-3-Clause)

---

**Generated**: 2026-05-28  
**Analyst**: Automated license scanning with manual review  
**Contact**: See repository maintainers for questions
