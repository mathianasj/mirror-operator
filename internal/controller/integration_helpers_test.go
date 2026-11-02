package controller

import (
	"context"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type externalCRDDef struct {
	Group     string
	Version   string
	Kind      string
	Plural    string
	Scope     apiextensionsv1.ResourceScope
	HasStatus bool
}

var externalCRDs = []externalCRDDef{
	{Group: "operators.coreos.com", Version: "v1alpha1", Kind: "Subscription", Plural: "subscriptions", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "operators.coreos.com", Version: "v1", Kind: "OperatorGroup", Plural: "operatorgroups", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "operators.coreos.com", Version: "v1alpha1", Kind: "ClusterServiceVersion", Plural: "clusterserviceversions", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "operators.coreos.com", Version: "v1alpha1", Kind: "CatalogSource", Plural: "catalogsources", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "packages.operators.coreos.com", Version: "v1", Kind: "PackageManifest", Plural: "packagemanifests", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "tekton.dev", Version: "v1", Kind: "Pipeline", Plural: "pipelines", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "tekton.dev", Version: "v1", Kind: "PipelineRun", Plural: "pipelineruns", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "tekton.dev", Version: "v1", Kind: "TaskRun", Plural: "taskruns", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "quay.redhat.com", Version: "v1", Kind: "QuayRegistry", Plural: "quayregistries", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "k8s.keycloak.org", Version: "v2alpha1", Kind: "Keycloak", Plural: "keycloaks", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "k8s.keycloak.org", Version: "v2alpha1", Kind: "KeycloakRealmImport", Plural: "keycloakrealmimports", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Securesign", Plural: "securesigns", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "rhtas.redhat.com", Version: "v1alpha1", Kind: "Tuf", Plural: "tufs", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "rhtpa.io", Version: "v1", Kind: "TrustedProfileAnalyzer", Plural: "trustedprofileanalyzers", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "updateservice.operator.openshift.io", Version: "v1", Kind: "UpdateService", Plural: "updateservices", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "objectbucket.io", Version: "v1alpha1", Kind: "ObjectBucketClaim", Plural: "objectbucketclaims", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "route.openshift.io", Version: "v1", Kind: "Route", Plural: "routes", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "console.openshift.io", Version: "v1", Kind: "ConsolePlugin", Plural: "consoleplugins", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "config.openshift.io", Version: "v1", Kind: "Image", Plural: "images", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "config.openshift.io", Version: "v1", Kind: "Ingress", Plural: "ingresses", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "config.openshift.io", Version: "v1", Kind: "Infrastructure", Plural: "infrastructures", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "config.openshift.io", Version: "v1", Kind: "ImageDigestMirrorSet", Plural: "imagedigestmirrorsets", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "config.openshift.io", Version: "v1", Kind: "ImageTagMirrorSet", Plural: "imagetagmirrorsets", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "config.openshift.io", Version: "v1", Kind: "ClusterImagePolicy", Plural: "clusterimagepolicies", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "operator.openshift.io", Version: "v1", Kind: "Console", Plural: "consoles", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "operator.openshift.io", Version: "v1", Kind: "IngressController", Plural: "ingresscontrollers", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "oauth.openshift.io", Version: "v1", Kind: "OAuthClient", Plural: "oauthclients", Scope: apiextensionsv1.ClusterScoped, HasStatus: false},
	{Group: "operator.open-cluster-management.io", Version: "v1", Kind: "MultiClusterHub", Plural: "multiclusterhubs", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "hive.openshift.io", Version: "v1", Kind: "ClusterImageSet", Plural: "clusterimagesets", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "AgentServiceConfig", Plural: "agentserviceconfigs", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "agent-install.openshift.io", Version: "v1beta1", Kind: "InfraEnv", Plural: "infraenvs", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "metal3.io", Version: "v1alpha1", Kind: "Provisioning", Plural: "provisionings", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "machineconfiguration.openshift.io", Version: "v1", Kind: "MachineConfig", Plural: "machineconfigs", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
	{Group: "cert-manager.io", Version: "v1", Kind: "Certificate", Plural: "certificates", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "cert-manager.io", Version: "v1", Kind: "Issuer", Plural: "issuers", Scope: apiextensionsv1.NamespaceScoped, HasStatus: true},
	{Group: "cert-manager.io", Version: "v1", Kind: "ClusterIssuer", Plural: "clusterissuers", Scope: apiextensionsv1.ClusterScoped, HasStatus: true},
}

func registerExternalCRDs(ctx context.Context, c client.Client) error {
	for _, def := range externalCRDs {
		crd := buildMinimalCRD(def)
		existing := &apiextensionsv1.CustomResourceDefinition{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(crd), existing); err != nil {
			if apierrors.IsNotFound(err) {
				if err := c.Create(ctx, crd); err != nil {
					return fmt.Errorf("creating CRD %s: %w", crd.Name, err)
				}
			} else {
				return fmt.Errorf("checking CRD %s: %w", crd.Name, err)
			}
		}
	}
	return nil
}

func buildMinimalCRD(def externalCRDDef) *apiextensionsv1.CustomResourceDefinition {
	crdName := fmt.Sprintf("%s.%s", def.Plural, def.Group)

	validation := &apiextensionsv1.CustomResourceValidation{
		OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
			Type:                   "object",
			XPreserveUnknownFields: boolPtr(true),
		},
	}

	version := apiextensionsv1.CustomResourceDefinitionVersion{
		Name:    def.Version,
		Served:  true,
		Storage: true,
		Schema:  validation,
	}

	if def.HasStatus {
		version.Subresources = &apiextensionsv1.CustomResourceSubresources{
			Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
		}
	}

	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: def.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     def.Kind,
				Plural:   def.Plural,
				Singular: "",
			},
			Scope:    def.Scope,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{version},
		},
	}
}

func newUnstructuredObj(group, version, kind, name, namespace string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: version, Kind: kind})
	obj.SetName(name)
	if namespace != "" {
		obj.SetNamespace(namespace)
	}
	return obj
}

func setNestedStatus(obj *unstructured.Unstructured, fields map[string]interface{}) {
	status, ok := obj.Object["status"].(map[string]interface{})
	if !ok {
		status = map[string]interface{}{}
	}
	for k, v := range fields {
		status[k] = v
	}
	obj.Object["status"] = status
}

func updateUnstructuredStatus(ctx context.Context, c client.Client, obj *unstructured.Unstructured) error {
	return c.Status().Update(ctx, obj)
}
