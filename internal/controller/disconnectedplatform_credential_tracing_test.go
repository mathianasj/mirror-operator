package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

// stepScriptFromTask extracts the shell script from the first step of the named task.
func stepScriptFromTask(tasks []map[string]interface{}, taskName, stepName string) string {
	for _, task := range tasks {
		if task["name"] != taskName {
			continue
		}
		spec, _ := task["taskSpec"].(map[string]interface{})
		steps, _ := spec["steps"].([]map[string]interface{})
		for _, step := range steps {
			if stepName == "" || step["name"] == stepName {
				args, _ := step["args"].([]string)
				if len(args) > 0 {
					return args[0]
				}
			}
		}
	}
	return ""
}

var _ = Describe("credential tracing safety (issue #139)", func() {
	var (
		r      *DisconnectedPlatformReconciler
		scheme *runtime.Scheme
	)

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(scheme)).To(Succeed())
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r = &DisconnectedPlatformReconciler{Client: c, Scheme: scheme}
	})

	Describe("connected pipeline tasks", func() {
		var tasks []map[string]interface{}

		BeforeEach(func() {
			tasks = r.buildPipelineTasks()
		})

		It("upload-to-s3 upload-artifacts step does not use set -x tracing", func() {
			script := stepScriptFromTask(tasks, "upload-to-s3", "upload-artifacts")
			Expect(script).NotTo(BeEmpty(), "upload-to-s3 task must exist")
			Expect(script).NotTo(ContainSubstring("set -ex"),
				"upload-to-s3 must not use set -ex: AWS credentials would be traced to logs")
			Expect(script).NotTo(ContainSubstring("set -x"),
				"upload-to-s3 must not use set -x: AWS credentials would be traced to logs")
		})

		It("upload-to-s3 still exits on error", func() {
			script := stepScriptFromTask(tasks, "upload-to-s3", "upload-artifacts")
			Expect(script).To(ContainSubstring("set -e"),
				"upload-to-s3 must still use set -e for error safety")
		})

		It("save-images step does not use set -x tracing", func() {
			// save-images uses the pull-secret authfile path; tracing exposes it
			script := stepScriptFromTask(tasks, "export-architect-images", "save-images")
			if script == "" {
				// task may be named differently; search all tasks
				for _, task := range tasks {
					spec, _ := task["taskSpec"].(map[string]interface{})
					steps, _ := spec["steps"].([]map[string]interface{})
					for _, step := range steps {
						if step["name"] == "save-images" {
							args, _ := step["args"].([]string)
							if len(args) > 0 {
								script = args[0]
							}
						}
					}
				}
			}
			Expect(script).NotTo(BeEmpty(), "save-images step must exist")
			Expect(script).NotTo(ContainSubstring("set -ex"),
				"save-images must not use set -ex: pull-secret path would be traced to logs")
			Expect(script).NotTo(ContainSubstring("set -x"),
				"save-images must not use set -x: pull-secret path would be traced to logs")
		})
	})

	Describe("import pipeline tasks", func() {
		var tasks []map[string]interface{}

		BeforeEach(func() {
			tasks = r.buildImportPipelineTasks()
		})

		It("mirror-content oc-mirror step does not use set -x tracing", func() {
			script := stepScriptFromTask(tasks, "mirror-content", "oc-mirror")
			Expect(script).NotTo(BeEmpty(), "mirror-content task must exist")
			Expect(script).NotTo(ContainSubstring("set -ex"),
				"mirror-content must not use set -ex: pull-secret copy would be traced to logs")
			Expect(script).NotTo(ContainSubstring("set -x"),
				"mirror-content must not use set -x: pull-secret copy would be traced to logs")
		})

		It("mirror-content still exits on error", func() {
			script := stepScriptFromTask(tasks, "mirror-content", "oc-mirror")
			Expect(script).To(ContainSubstring("set -e"),
				"mirror-content must still use set -e for error safety")
		})

		It("mirror-to-nodes oc-mirror-nodes step does not use set -x tracing", func() {
			script := stepScriptFromTask(tasks, "mirror-to-nodes", "oc-mirror-nodes")
			Expect(script).NotTo(BeEmpty(), "mirror-to-nodes task must exist")
			Expect(script).NotTo(ContainSubstring("set -ex"),
				"mirror-to-nodes must not use set -ex: pull-secret copy would be traced to logs")
			Expect(script).NotTo(ContainSubstring("set -x"),
				"mirror-to-nodes must not use set -x: pull-secret copy would be traced to logs")
		})

		It("mirror-to-nodes still exits on error", func() {
			script := stepScriptFromTask(tasks, "mirror-to-nodes", "oc-mirror-nodes")
			Expect(script).To(ContainSubstring("set -e"),
				"mirror-to-nodes must still use set -e for error safety")
		})
	})
})
