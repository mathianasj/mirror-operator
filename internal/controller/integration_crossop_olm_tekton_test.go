package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Integration CrossOp: OLM and Tekton", func() {
	var r *DisconnectedPlatformReconciler

	BeforeEach(func() {
		r = &DisconnectedPlatformReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		ensureNamespace(architectNamespace)
	})

	Context("OLM Subscription and CSV Status", func() {
		It("reads CSV status from subscription status.currentCSV", func() {
			opNS := "olm-crossop-csvstatus"
			ensureNamespace(opNS)

			op := operatorDef{
				name:      "test-operator",
				pkg:       "test-operator-pkg",
				channel:   "stable",
				catalog:   "test-catalog",
				catalogNS: "openshift-marketplace",
				ns:        opNS,
			}

			sub := newUnstructuredObj("operators.coreos.com", "v1alpha1", "Subscription",
				"mirror-operator-test-operator", opNS)
			unstructured.SetNestedField(sub.Object, "test-operator-pkg", "spec", "name")
			unstructured.SetNestedField(sub.Object, "stable", "spec", "channel")
			ensureUnstructured(sub)

			setNestedStatus(sub, map[string]interface{}{
				"currentCSV": "test-operator.v1.2.3",
			})
			Expect(k8sClient.Status().Update(ctx, sub)).To(Succeed())

			csv := newUnstructuredObj("operators.coreos.com", "v1alpha1", "ClusterServiceVersion",
				"test-operator.v1.2.3", opNS)
			ensureUnstructured(csv)
			setNestedStatus(csv, map[string]interface{}{
				"phase": "Succeeded",
			})
			Expect(k8sClient.Status().Update(ctx, csv)).To(Succeed())

			status := r.csvStatus(ctx, op)
			Expect(status).To(Equal("Succeeded"))
		})

		It("returns empty status when subscription has no currentCSV", func() {
			opNS := "olm-crossop-nocsv"
			ensureNamespace(opNS)

			op := operatorDef{
				name: "no-csv-op",
				ns:   opNS,
			}

			sub := newUnstructuredObj("operators.coreos.com", "v1alpha1", "Subscription",
				"mirror-operator-no-csv-op", opNS)
			ensureUnstructured(sub)

			status := r.csvStatus(ctx, op)
			Expect(status).To(BeEmpty())
		})

		It("returns empty status when subscription does not exist", func() {
			op := operatorDef{
				name: "missing-sub",
				ns:   "nonexistent-ns-olm",
			}

			status := r.csvStatus(ctx, op)
			Expect(status).To(BeEmpty())
		})

		It("returns CSV phase when phase is Installing", func() {
			opNS := "olm-crossop-installing"
			ensureNamespace(opNS)

			op := operatorDef{
				name: "installing-op",
				ns:   opNS,
			}

			sub := newUnstructuredObj("operators.coreos.com", "v1alpha1", "Subscription",
				"mirror-operator-installing-op", opNS)
			ensureUnstructured(sub)
			setNestedStatus(sub, map[string]interface{}{
				"currentCSV": "installing-op.v0.1.0",
			})
			Expect(k8sClient.Status().Update(ctx, sub)).To(Succeed())

			csv := newUnstructuredObj("operators.coreos.com", "v1alpha1", "ClusterServiceVersion",
				"installing-op.v0.1.0", opNS)
			ensureUnstructured(csv)
			setNestedStatus(csv, map[string]interface{}{
				"phase": "Installing",
			})
			Expect(k8sClient.Status().Update(ctx, csv)).To(Succeed())

			status := r.csvStatus(ctx, op)
			Expect(status).To(Equal("Installing"))
		})
	})

	Context("PackageManifest Discovery", func() {
		It("reads catalog info from PackageManifest status", func() {
			ensureNamespace("openshift-marketplace")

			pm := newUnstructuredObj("packages.operators.coreos.com", "v1", "PackageManifest",
				"crossop-test-pkg", "openshift-marketplace")
			ensureUnstructured(pm)
			setNestedStatus(pm, map[string]interface{}{
				"catalogSource":          "redhat-operators",
				"catalogSourceNamespace": "openshift-marketplace",
				"defaultChannel":         "stable-v3",
			})
			Expect(k8sClient.Status().Update(ctx, pm)).To(Succeed())

			catalog, catalogNS, channel, err := r.discoverPackageInfo(ctx, "crossop-test-pkg")
			Expect(err).NotTo(HaveOccurred())
			Expect(catalog).To(Equal("redhat-operators"))
			Expect(catalogNS).To(Equal("openshift-marketplace"))
			Expect(channel).To(Equal("stable-v3"))
		})

		It("returns error when PackageManifest does not exist", func() {
			_, _, _, err := r.discoverPackageInfo(ctx, "nonexistent-package")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})

		It("returns error when PackageManifest has no catalogSource", func() {
			ensureNamespace("openshift-marketplace")

			pm := newUnstructuredObj("packages.operators.coreos.com", "v1", "PackageManifest",
				"crossop-empty-catalog", "openshift-marketplace")
			ensureUnstructured(pm)

			_, _, _, err := r.discoverPackageInfo(ctx, "crossop-empty-catalog")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no catalogSource"))
		})
	})

	Context("OLM Subscription Creation", func() {
		It("creates subscription with correct name prefix and spec", func() {
			opNS := "olm-crossop-subcreate"
			ensureNamespace(opNS)

			op := operatorDef{
				name:      "test-sub-create",
				pkg:       "test-sub-pkg",
				channel:   "stable",
				catalog:   "test-catalog",
				catalogNS: "openshift-marketplace",
				ns:        opNS,
			}

			err := r.ensureSubscription(ctx, op, nil)
			Expect(err).NotTo(HaveOccurred())

			sub := &unstructured.Unstructured{}
			sub.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "operators.coreos.com", Version: "v1alpha1", Kind: "Subscription",
			})
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: "mirror-operator-test-sub-create", Namespace: opNS,
			}, sub)
			Expect(err).NotTo(HaveOccurred())

			pkgName, _, _ := unstructured.NestedString(sub.Object, "spec", "name")
			Expect(pkgName).To(Equal("test-sub-pkg"))

			channel, _, _ := unstructured.NestedString(sub.Object, "spec", "channel")
			Expect(channel).To(Equal("stable"))

			source, _, _ := unstructured.NestedString(sub.Object, "spec", "source")
			Expect(source).To(Equal("test-catalog"))
		})

		It("overrides channel and catalog from OLMSubscriptionConfig", func() {
			opNS := "olm-crossop-override"
			ensureNamespace(opNS)

			op := operatorDef{
				name:      "test-override-op",
				pkg:       "override-pkg",
				channel:   "default-channel",
				catalog:   "default-catalog",
				catalogNS: "default-ns",
				ns:        opNS,
			}
			cfg := &mirrorv1.OLMSubscriptionConfig{
				Channel:         "custom-channel",
				CatalogSource:   "custom-catalog",
				CatalogSourceNS: "custom-ns",
			}

			err := r.ensureSubscription(ctx, op, cfg)
			Expect(err).NotTo(HaveOccurred())

			sub := &unstructured.Unstructured{}
			sub.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "operators.coreos.com", Version: "v1alpha1", Kind: "Subscription",
			})
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "mirror-operator-test-override-op", Namespace: opNS,
			}, sub)).To(Succeed())

			channel, _, _ := unstructured.NestedString(sub.Object, "spec", "channel")
			Expect(channel).To(Equal("custom-channel"))

			source, _, _ := unstructured.NestedString(sub.Object, "spec", "source")
			Expect(source).To(Equal("custom-catalog"))

			sourceNS, _, _ := unstructured.NestedString(sub.Object, "spec", "sourceNamespace")
			Expect(sourceNS).To(Equal("custom-ns"))
		})
	})

	Context("Tekton PipelineRun Tracking — CollectionPipeline", func() {
		It("detects successful PipelineRun completion", func() {
			ns := "tekton-crossop-success"
			ensureNamespace(ns)

			cp := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-success-cp",
					Namespace: ns,
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}
			Expect(k8sClient.Create(ctx, cp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cp) })

			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-success-pr",
					Namespace: ns,
				},
				Spec: pipelinev1.PipelineRunSpec{},
			}
			Expect(k8sClient.Create(ctx, pr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pr) })

			pr.Status = pipelinev1.PipelineRunStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: "Succeeded", Status: "True"},
					},
				},
				PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
					StartTime:      &now,
					CompletionTime: &now,
					Results: []pipelinev1.PipelineRunResult{
						{Name: "bundle-url", Value: pipelinev1.ParamValue{Type: "string", StringVal: "s3://bucket/bundle.tar"}},
						{Name: "signature-url", Value: pipelinev1.ParamValue{Type: "string", StringVal: "s3://bucket/bundle.tar.sig"}},
					},
				},
			}
			Expect(k8sClient.Status().Update(ctx, pr)).To(Succeed())

			cp.Status.PipelineRunRef = "crossop-success-pr"
			cp.Status.Phase = "Collecting"
			Expect(k8sClient.Status().Update(ctx, cp)).To(Succeed())

			cpR := &CollectionPipelineReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := cpR.trackPipelineRun(ctx, cp, ctrl.Request{NamespacedName: types.NamespacedName{
				Name: cp.Name, Namespace: ns,
			}})
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.CollectionPipeline{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cp), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal("Complete"))
			Expect(updated.Status.BundleURL).To(Equal("s3://bucket/bundle.tar"))
		})

		It("detects failed PipelineRun", func() {
			ns := "tekton-crossop-fail"
			ensureNamespace(ns)

			cp := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-fail-cp",
					Namespace: ns,
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}
			Expect(k8sClient.Create(ctx, cp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cp) })

			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-fail-pr",
					Namespace: ns,
				},
				Spec: pipelinev1.PipelineRunSpec{},
			}
			Expect(k8sClient.Create(ctx, pr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pr) })

			pr.Status = pipelinev1.PipelineRunStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: "Succeeded", Status: "False"},
					},
				},
				PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
					StartTime:      &now,
					CompletionTime: &now,
				},
			}
			Expect(k8sClient.Status().Update(ctx, pr)).To(Succeed())

			cp.Status.PipelineRunRef = "crossop-fail-pr"
			cp.Status.Phase = "Collecting"
			Expect(k8sClient.Status().Update(ctx, cp)).To(Succeed())

			cpR := &CollectionPipelineReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := cpR.trackPipelineRun(ctx, cp, ctrl.Request{NamespacedName: types.NamespacedName{
				Name: cp.Name, Namespace: ns,
			}})
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.CollectionPipeline{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cp), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal("Failed"))
		})

		It("marks pipeline Stale when PipelineRun is deleted", func() {
			ns := "tekton-crossop-stale"
			ensureNamespace(ns)

			cp := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-stale-cp",
					Namespace: ns,
				},
				Spec: mirrorv1.CollectionPipelineSpec{
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}
			Expect(k8sClient.Create(ctx, cp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cp) })

			cp.Status.PipelineRunRef = "deleted-pr-that-does-not-exist"
			cp.Status.Phase = "Collecting"
			Expect(k8sClient.Status().Update(ctx, cp)).To(Succeed())

			cpR := &CollectionPipelineReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := cpR.trackPipelineRun(ctx, cp, ctrl.Request{NamespacedName: types.NamespacedName{
				Name: cp.Name, Namespace: ns,
			}})
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.CollectionPipeline{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cp), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal("Stale"))
			Expect(updated.Status.PipelineRunRef).To(BeEmpty())
		})
	})

	Context("Tekton PipelineRun Tracking — MirrorImport", func() {
		It("detects successful import PipelineRun completion", func() {
			ns := "tekton-crossop-import-ok"
			ensureNamespace(ns)

			imp := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-import-ok",
					Namespace: ns,
				},
				Spec: mirrorv1.MirrorImportSpec{
					Bundle: mirrorv1.BundleSource{
						PVC:      "bundle-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "registry.example.com/mirror",
					},
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}
			Expect(k8sClient.Create(ctx, imp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, imp) })

			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-import-ok-pr",
					Namespace: ns,
				},
				Spec: pipelinev1.PipelineRunSpec{},
			}
			Expect(k8sClient.Create(ctx, pr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pr) })

			pr.Status = pipelinev1.PipelineRunStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: "Succeeded", Status: "True"},
					},
				},
				PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
					StartTime:      &now,
					CompletionTime: &now,
				},
			}
			Expect(k8sClient.Status().Update(ctx, pr)).To(Succeed())

			imp.Status.PipelineRunRef = "crossop-import-ok-pr"
			imp.Status.Phase = "Importing"
			Expect(k8sClient.Status().Update(ctx, imp)).To(Succeed())

			impR := &MirrorImportReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := impR.trackImportPipelineRun(ctx, imp, ctrl.Request{NamespacedName: types.NamespacedName{
				Name: imp.Name, Namespace: ns,
			}})
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.MirrorImport{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(imp), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal("Publishing"))
		})

		It("detects failed import PipelineRun", func() {
			ns := "tekton-crossop-import-fail"
			ensureNamespace(ns)

			imp := &mirrorv1.MirrorImport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-import-fail",
					Namespace: ns,
				},
				Spec: mirrorv1.MirrorImportSpec{
					Bundle: mirrorv1.BundleSource{
						PVC:      "bundle-pvc",
						Filename: "bundle.tar",
					},
					TargetRegistry: mirrorv1.RegistryConfig{
						URL: "registry.example.com/mirror",
					},
					ImageSetConfig: "kind: ImageSetConfiguration",
				},
			}
			Expect(k8sClient.Create(ctx, imp)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, imp) })

			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "crossop-import-fail-pr",
					Namespace: ns,
				},
				Spec: pipelinev1.PipelineRunSpec{},
			}
			Expect(k8sClient.Create(ctx, pr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pr) })

			pr.Status = pipelinev1.PipelineRunStatus{
				Status: duckv1.Status{
					Conditions: []apis.Condition{
						{Type: "Succeeded", Status: "False"},
					},
				},
				PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
					StartTime:      &now,
					CompletionTime: &now,
				},
			}
			Expect(k8sClient.Status().Update(ctx, pr)).To(Succeed())

			imp.Status.PipelineRunRef = "crossop-import-fail-pr"
			imp.Status.Phase = "Importing"
			Expect(k8sClient.Status().Update(ctx, imp)).To(Succeed())

			impR := &MirrorImportReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := impR.trackImportPipelineRun(ctx, imp, ctrl.Request{NamespacedName: types.NamespacedName{
				Name: imp.Name, Namespace: ns,
			}})
			Expect(err).NotTo(HaveOccurred())

			updated := &mirrorv1.MirrorImport{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(imp), updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal("Failed"))
		})
	})

	Context("PipelineRun Phase Detection", func() {
		It("returns Pending when no start time", func() {
			pr := &pipelinev1.PipelineRun{}
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Pending"))
			Expect(importPipelineRunPhase(pr)).To(Equal("Pending"))
		})

		It("returns Collecting/Running when started but not completed", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						StartTime: &now,
					},
				},
			}
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Collecting"))
			Expect(importPipelineRunPhase(pr)).To(Equal("Running"))
		})

		It("returns Complete when succeeded", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					Status: duckv1.Status{
						Conditions: []apis.Condition{
							{Type: "Succeeded", Status: "True"},
						},
					},
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						StartTime:      &now,
						CompletionTime: &now,
					},
				},
			}
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Complete"))
			Expect(importPipelineRunPhase(pr)).To(Equal("Complete"))
		})

		It("returns Failed when completed without success", func() {
			now := metav1.Now()
			pr := &pipelinev1.PipelineRun{
				Status: pipelinev1.PipelineRunStatus{
					Status: duckv1.Status{
						Conditions: []apis.Condition{
							{Type: "Succeeded", Status: "False"},
						},
					},
					PipelineRunStatusFields: pipelinev1.PipelineRunStatusFields{
						StartTime:      &now,
						CompletionTime: &now,
					},
				},
			}
			Expect(collectionPipelineRunPhase(pr)).To(Equal("Failed"))
			Expect(importPipelineRunPhase(pr)).To(Equal("Failed"))
		})
	})
})

var _ = fmt.Sprintf
var _ = time.Now
