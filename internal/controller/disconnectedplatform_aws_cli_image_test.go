package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("AWS CLI image fully-qualified reference (issue #141)", func() {
	var (
		r     *DisconnectedPlatformReconciler
		tasks []map[string]interface{}
	)

	BeforeEach(func() {
		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(scheme)).To(Succeed())
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r = &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}
		tasks = r.buildPipelineTasks()
	})

	It("upload-to-s3 task uses docker.io/amazon/aws-cli:latest (fully qualified)", func() {
		task := findTaskByName(tasks, "upload-to-s3")
		Expect(task).NotTo(BeNil(), "upload-to-s3 task must exist")

		spec, _ := task["taskSpec"].(map[string]interface{})
		steps, _ := spec["steps"].([]map[string]interface{})

		for _, step := range steps {
			img, _ := step["image"].(string)
			Expect(img).NotTo(Equal("amazon/aws-cli:latest"),
				"upload-to-s3 step image must include docker.io/ prefix; got %q — "+
					"oc-mirror normalizes bare docker.io images so the airgapped mirror path uses docker.io/amazon/aws-cli, "+
					"but the task would look it up under amazon/aws-cli causing a mismatch", img)
			if img != "" {
				Expect(img).To(Equal("docker.io/amazon/aws-cli:latest"),
					"upload-to-s3 step image must be docker.io/amazon/aws-cli:latest")
			}
		}
	})
})
