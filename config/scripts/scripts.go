package scripts

import _ "embed"

//go:embed import-airgap-architect.sh
var ImportAirgapArchitectScript string

// BundledCRDs holds the mirror-operator CRD YAMLs keyed by their target
// filename in BOOTSTRAP_MANIFESTS_DIR. These copies are kept in sync with
// config/crd/bases/ by running 'make sync-crds' after 'make manifests'.
var BundledCRDs = map[string]string{
	"00-crd-clusterbootstraps.yaml":     crdClusterBootstraps,
	"00-crd-collectionpipelines.yaml":   crdCollectionPipelines,
	"00-crd-disconnectedplatforms.yaml": crdDisconnectedPlatforms,
	"00-crd-mirrorimports.yaml":         crdMirrorImports,
	"00-crd-mirrorreleases.yaml":        crdMirrorReleases,
}

//go:embed bundledCrds/mirror.mirror.mathianasj.github.com_clusterbootstraps.yaml
var crdClusterBootstraps string

//go:embed bundledCrds/mirror.mirror.mathianasj.github.com_collectionpipelines.yaml
var crdCollectionPipelines string

//go:embed bundledCrds/mirror.mirror.mathianasj.github.com_disconnectedplatforms.yaml
var crdDisconnectedPlatforms string

//go:embed bundledCrds/mirror.mirror.mathianasj.github.com_mirrorimports.yaml
var crdMirrorImports string

//go:embed bundledCrds/mirror.mirror.mathianasj.github.com_mirrorreleases.yaml
var crdMirrorReleases string
