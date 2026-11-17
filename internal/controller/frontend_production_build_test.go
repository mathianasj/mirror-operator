package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("frontendContainer production build (issue #131)", func() {
	const (
		testImage    = "quay.io/example/frontend:latest"
		testName     = "test-frontend"
		backendHost  = "backend.apps.cluster.example.com"
		frontendHost = "frontend.apps.cluster.example.com"
	)

	var container map[string]interface{}

	BeforeEach(func() {
		container = frontendContainer(testName, testImage, nil, backendHost, frontendHost)
	})

	It("does not bind to port 5173 (Vite dev server port)", func() {
		ports, _ := container["ports"].([]interface{})
		for _, p := range ports {
			pm, _ := p.(map[string]interface{})
			port := fmt.Sprintf("%v", pm["containerPort"])
			Expect(port).NotTo(Equal("5173"),
				"frontend must not use port 5173 (Vite dev server); use 8080 (nginx, non-privileged, OpenShift-compatible)")
		}
	})

	It("listens on port 8080 (nginx, OpenShift-compatible)", func() {
		ports, _ := container["ports"].([]interface{})
		Expect(ports).NotTo(BeEmpty())
		pm, _ := ports[0].(map[string]interface{})
		Expect(fmt.Sprintf("%v", pm["containerPort"])).To(Equal("8080"),
			"frontend must listen on port 8080 so nginx runs as an arbitrary UID without CAP_NET_BIND_SERVICE")
	})

	It("does not mount the Vite dev-server cache directory", func() {
		mounts, _ := container["volumeMounts"].([]interface{})
		for _, m := range mounts {
			mm, _ := m.(map[string]interface{})
			mp, _ := mm["mountPath"].(string)
			Expect(mp).NotTo(ContainSubstring(".vite"),
				"frontend must not mount a Vite cache directory — it must run a pre-built static build served by nginx")
		}
	})

	It("readiness probe targets port 8080", func() {
		probe, _ := container["readinessProbe"].(map[string]interface{})
		tcp, _ := probe["tcpSocket"].(map[string]interface{})
		Expect(fmt.Sprintf("%v", tcp["port"])).To(Equal("8080"))
	})

	It("liveness probe targets port 8080", func() {
		probe, _ := container["livenessProbe"].(map[string]interface{})
		tcp, _ := probe["tcpSocket"].(map[string]interface{})
		Expect(fmt.Sprintf("%v", tcp["port"])).To(Equal("8080"))
	})
})
