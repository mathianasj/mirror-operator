package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mirrorv1 "github.com/mathianasj/mirror-operator/api/v1"
)

var _ = Describe("Event Handlers", func() {
	var (
		ctx        context.Context
		testScheme *runtime.Scheme
	)

	BeforeEach(func() {
		ctx = context.Background()
		testScheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(testScheme)).To(Succeed())
		Expect(mirrorv1.AddToScheme(testScheme)).To(Succeed())
	})

	Describe("secretEventHandler", func() {
		It("enqueues platforms when the pull secret is updated", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &secretEventHandler{client: c}

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      defaultPullSecretName,
					Namespace: defaultPullSecretNS,
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: secret,
				ObjectNew: secret,
			}, q)

			Expect(q.Len()).To(Equal(1))
			item, _ := q.Get()
			Expect(item.Name).To(Equal("test-platform"))
		})

		It("ignores non-pull-secret updates", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &secretEventHandler{client: c}

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "some-other-secret",
					Namespace: "default",
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: secret,
				ObjectNew: secret,
			}, q)

			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("nodeEventHandler", func() {
		It("enqueues platforms when a master node becomes ready", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &nodeEventHandler{client: c}

			oldNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-0",
					Labels: map[string]string{"node-role.kubernetes.io/master": ""},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			}

			newNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-0",
					Labels: map[string]string{"node-role.kubernetes.io/master": ""},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: oldNode,
				ObjectNew: newNode,
			}, q)

			Expect(q.Len()).To(Equal(1))
		})

		It("ignores non-master node updates", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &nodeEventHandler{client: c}

			oldNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "worker-0",
					Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			}

			newNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "worker-0",
					Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: oldNode,
				ObjectNew: newNode,
			}, q)

			Expect(q.Len()).To(Equal(0))
		})

		It("ignores master node updates when ready status unchanged", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-0",
					Labels: map[string]string{"node-role.kubernetes.io/master": ""},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: node,
				ObjectNew: node,
			}, q)

			Expect(q.Len()).To(Equal(0))
		})

		It("enqueues platforms when a master node is deleted", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &nodeEventHandler{client: c}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "master-0",
					Labels: map[string]string{"node-role.kubernetes.io/master": ""},
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{
				Object: node,
			}, q)

			Expect(q.Len()).To(Equal(1))
		})

		It("does not enqueue when a worker node is deleted", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "worker-0",
					Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{
				Object: node,
			}, q)

			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("nodeReadyStatus", func() {
		It("returns True when node is ready", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionTrue))
		})

		It("returns False when node is not ready", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionFalse))
		})

		It("returns Unknown when no Ready condition exists", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionUnknown))
		})

		It("returns Unknown when node has no conditions", func() {
			node := &corev1.Node{}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionUnknown))
		})
	})

	Describe("collectionPipelineEventHandler", func() {
		It("enqueues platforms on Create", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &collectionPipelineEventHandler{client: c}

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Create(ctx, event.TypedCreateEvent[client.Object]{
				Object: pipeline,
			}, q)

			Expect(q.Len()).To(Equal(1))
		})

		It("enqueues platforms on Delete", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &collectionPipelineEventHandler{client: c}

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{
				Object: pipeline,
			}, q)

			Expect(q.Len()).To(Equal(1))
		})

		It("enqueues platforms when pipeline phase changes to Complete", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-platform",
					Namespace: "default",
				},
				Spec: mirrorv1.DisconnectedPlatformSpec{
					Mode: "connected",
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &collectionPipelineEventHandler{client: c}

			oldPipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Running",
				},
			}

			newPipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Complete",
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: oldPipeline,
				ObjectNew: newPipeline,
			}, q)

			Expect(q.Len()).To(Equal(1))
		})

		It("does not enqueue when pipeline phase does not change", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &collectionPipelineEventHandler{client: c}

			pipeline := &mirrorv1.CollectionPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Status: mirrorv1.CollectionPipelineStatus{
					Phase: "Running",
				},
			}

			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(ctx, event.TypedUpdateEvent[client.Object]{
				ObjectOld: pipeline,
				ObjectNew: pipeline,
			}, q)

			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("taskRunEventHandler", func() {
		It("deletes OSUS pods when mirror-to-intermediate TaskRun succeeds", func() {
			osusPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osus-pod-1",
					Namespace: "openshift-update-service",
					Labels:    map[string]string{"app": "update-service-oc-mirror"},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(osusPod).Build()
			h := &taskRunEventHandler{client: c}

			oldTaskRun := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata": map[string]interface{}{
					"name":      "mirror-to-intermediate-run",
					"namespace": "test-ns",
					"labels": map[string]interface{}{
						"tekton.dev/pipelineTask": "mirror-to-intermediate",
					},
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Succeeded",
							"status": "Unknown",
						},
					},
				},
			}}

			newTaskRun := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata": map[string]interface{}{
					"name":      "mirror-to-intermediate-run",
					"namespace": "test-ns",
					"labels": map[string]interface{}{
						"tekton.dev/pipelineTask": "mirror-to-intermediate",
					},
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Succeeded",
							"status": "True",
						},
					},
				},
			}}

			h.handleTaskRun(ctx, oldTaskRun, newTaskRun)

			// OSUS pod should be deleted
			podList := &corev1.PodList{}
			err := c.List(ctx, podList)
			Expect(err).NotTo(HaveOccurred())
			Expect(podList.Items).To(BeEmpty())
		})

		It("ignores TaskRuns that are not mirror-to-intermediate", func() {
			osusPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osus-pod-1",
					Namespace: "openshift-update-service",
					Labels:    map[string]string{"app": "update-service-oc-mirror"},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(osusPod).Build()
			h := &taskRunEventHandler{client: c}

			newTaskRun := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata": map[string]interface{}{
					"name":      "some-other-task-run",
					"namespace": "test-ns",
					"labels": map[string]interface{}{
						"tekton.dev/pipelineTask": "some-other-task",
					},
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Succeeded",
							"status": "True",
						},
					},
				},
			}}

			h.handleTaskRun(ctx, newTaskRun, newTaskRun)

			// OSUS pod should still exist
			podList := &corev1.PodList{}
			err := c.List(ctx, podList)
			Expect(err).NotTo(HaveOccurred())
			Expect(podList.Items).To(HaveLen(1))
		})

		It("ignores already-succeeded TaskRuns", func() {
			osusPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "osus-pod-1",
					Namespace: "openshift-update-service",
					Labels:    map[string]string{"app": "update-service-oc-mirror"},
				},
			}

			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(osusPod).Build()
			h := &taskRunEventHandler{client: c}

			taskRun := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata": map[string]interface{}{
					"name":      "mirror-to-intermediate-run",
					"namespace": "test-ns",
					"labels": map[string]interface{}{
						"tekton.dev/pipelineTask": "mirror-to-intermediate",
					},
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Succeeded",
							"status": "True",
						},
					},
				},
			}}

			h.handleTaskRun(ctx, taskRun, taskRun)

			// OSUS pod should still exist (already succeeded)
			podList := &corev1.PodList{}
			err := c.List(ctx, podList)
			Expect(err).NotTo(HaveOccurred())
			Expect(podList.Items).To(HaveLen(1))
		})
	})

	Describe("taskRunSucceeded", func() {
		It("returns true when Succeeded condition is True", func() {
			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Succeeded",
							"status": "True",
						},
					},
				},
			}}
			Expect(taskRunSucceeded(u)).To(BeTrue())
		})

		It("returns false when Succeeded condition is False", func() {
			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Succeeded",
							"status": "False",
						},
					},
				},
			}}
			Expect(taskRunSucceeded(u)).To(BeFalse())
		})

		It("returns false when no conditions exist", func() {
			u := &unstructured.Unstructured{Object: map[string]interface{}{}}
			Expect(taskRunSucceeded(u)).To(BeFalse())
		})

		It("returns false when no Succeeded condition", func() {
			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Ready",
							"status": "True",
						},
					},
				},
			}}
			Expect(taskRunSucceeded(u)).To(BeFalse())
		})
	})

	Describe("secretEventHandler - no-op methods", func() {
		It("Create does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Delete does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("collectionPipelineEventHandler - no-op methods", func() {
		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &collectionPipelineEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			pipeline := &mirrorv1.CollectionPipeline{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: pipeline}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Update with non-CollectionPipeline objects does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &collectionPipelineEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Update(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: secret, ObjectNew: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("nodeEventHandler - no-op methods", func() {
		It("Create does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: node}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: node}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Update with non-Node objects does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Update(ctx, event.TypedUpdateEvent[client.Object]{ObjectOld: secret, ObjectNew: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("taskRunEventHandler - no-op methods", func() {
		It("Create does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata":   map[string]interface{}{"name": "test", "namespace": "default"},
			}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: u}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Delete does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata":   map[string]interface{}{"name": "test", "namespace": "default"},
			}}
			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: u}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata":   map[string]interface{}{"name": "test", "namespace": "default"},
			}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: u}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Update with non-Unstructured objects does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.handleTaskRun(ctx, secret, secret)
			// Should silently return without error
		})
	})

	Describe("handleSecret edge cases", func() {
		It("silently returns when obj is not a Secret", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			h.handleSecret(ctx, node, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("secretEventHandler no-op methods", func() {
		It("Create does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Delete does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &secretEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: secret}, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("collectionPipelineEventHandler no-op methods", func() {
		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &collectionPipelineEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			pipeline := &mirrorv1.CollectionPipeline{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: pipeline}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Create enqueues platforms", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default"},
				Spec:       mirrorv1.DisconnectedPlatformSpec{Mode: "connected"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &collectionPipelineEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			pipeline := &mirrorv1.CollectionPipeline{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: pipeline}, q)
			Expect(q.Len()).To(Equal(1))
		})

		It("Delete enqueues platforms", func() {
			platform := &mirrorv1.DisconnectedPlatform{
				ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default"},
				Spec:       mirrorv1.DisconnectedPlatformSpec{Mode: "connected"},
			}
			c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(platform).Build()
			h := &collectionPipelineEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			pipeline := &mirrorv1.CollectionPipeline{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: pipeline}, q)
			Expect(q.Len()).To(Equal(1))
		})
	})

	Describe("nodeEventHandler no-op methods", func() {
		It("Create does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: node}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &nodeEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: node}, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("taskRunEventHandler no-op methods", func() {
		It("Create does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata":   map[string]interface{}{"name": "test", "namespace": "default"},
			}}
			h.Create(ctx, event.TypedCreateEvent[client.Object]{Object: u}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Delete does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata":   map[string]interface{}{"name": "test", "namespace": "default"},
			}}
			h.Delete(ctx, event.TypedDeleteEvent[client.Object]{Object: u}, q)
			Expect(q.Len()).To(Equal(0))
		})

		It("Generic does nothing", func() {
			c := fake.NewClientBuilder().WithScheme(testScheme).Build()
			h := &taskRunEventHandler{client: c}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			u := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "tekton.dev/v1",
				"kind":       "TaskRun",
				"metadata":   map[string]interface{}{"name": "test", "namespace": "default"},
			}}
			h.Generic(ctx, event.TypedGenericEvent[client.Object]{Object: u}, q)
			Expect(q.Len()).To(Equal(0))
		})
	})

	Describe("nodeReadyStatus", func() {
		It("returns the Ready condition status", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionTrue))
		})

		It("returns Unknown when no Ready condition exists", func() {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
					},
				},
			}
			Expect(nodeReadyStatus(node)).To(Equal(corev1.ConditionUnknown))
		})
	})
})
