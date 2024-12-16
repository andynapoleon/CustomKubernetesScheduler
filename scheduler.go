package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"
)

type CustomScheduler struct {
	clientset       *kubernetes.Clientset
	schedulerName   string
	informerFactory informers.SharedInformerFactory
	podInformer     cache.SharedIndexInformer
	nodeInformer    cache.SharedIndexInformer
	queue           workqueue.RateLimitingInterface
	podLister       []*corev1.Pod
	nodeLister      []*corev1.Node
	mutex           sync.RWMutex
	identity        string
	leaseLockName   string
	leaseLockNS     string
}

func newCustomScheduler(clientset *kubernetes.Clientset, schedulerName string) *CustomScheduler {
	informerFactory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := informerFactory.Core().V1().Pods().Informer()
	nodeInformer := informerFactory.Core().V1().Nodes().Informer()

	scheduler := &CustomScheduler{
		clientset:       clientset,
		schedulerName:   schedulerName,
		informerFactory: informerFactory,
		podInformer:     podInformer,
		nodeInformer:    nodeInformer,
		queue:           workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		podLister:       make([]*corev1.Pod, 0),
		nodeLister:      make([]*corev1.Node, 0),
		leaseLockName:   "custom-scheduler",
		leaseLockNS:     "kube-system",
	}

	// Pod event handlers
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			if scheduler.shouldSchedule(pod) {
				scheduler.queue.Add(pod.Name)
			}
		},
	})

	// Node event handlers
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			scheduler.mutex.Lock()
			scheduler.nodeLister = append(scheduler.nodeLister, obj.(*corev1.Node))
			scheduler.mutex.Unlock()
		},
		UpdateFunc: func(old, new interface{}) {
			scheduler.mutex.Lock()
			for i, node := range scheduler.nodeLister {
				if node.Name == new.(*corev1.Node).Name {
					scheduler.nodeLister[i] = new.(*corev1.Node)
					break
				}
			}
			scheduler.mutex.Unlock()
		},
		DeleteFunc: func(obj interface{}) {
			scheduler.mutex.Lock()
			for i, node := range scheduler.nodeLister {
				if node.Name == obj.(*corev1.Node).Name {
					scheduler.nodeLister = append(scheduler.nodeLister[:i], scheduler.nodeLister[i+1:]...)
					break
				}
			}
			scheduler.mutex.Unlock()
		},
	})

	return scheduler
}

func (s *CustomScheduler) shouldSchedule(pod *corev1.Pod) bool {
	return pod.Spec.SchedulerName == s.schedulerName &&
		pod.Spec.NodeName == "" &&
		pod.DeletionTimestamp == nil
}

func (s *CustomScheduler) Run(ctx context.Context) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("unable to get hostname: %v", err)
	}
	s.identity = hostname

	// Create lease lock resource
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      s.leaseLockName,
			Namespace: s.leaseLockNS,
		},
		Client: s.clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: s.identity,
		},
	}

	// Create leader election config
	leaderConfig := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if err := s.runScheduler(ctx); err != nil {
					log.Printf("Error running scheduler: %v", err)
				}
			},
			OnStoppedLeading: func() {
				log.Printf("Leader lost: %s", s.identity)
			},
			OnNewLeader: func(identity string) {
				if identity == s.identity {
					log.Printf("Still the leader: %s", identity)
				} else {
					log.Printf("New leader elected: %s", identity)
				}
			},
		},
	}

	// Start leader election
	leaderelection.RunOrDie(ctx, leaderConfig)
	return nil
}

func (s *CustomScheduler) runScheduler(ctx context.Context) error {
	defer s.queue.ShutDown()

	// Start informers
	s.informerFactory.Start(ctx.Done())

	// Wait for caches to sync
	if !cache.WaitForCacheSync(ctx.Done(),
		s.podInformer.HasSynced,
		s.nodeInformer.HasSynced) {
		return fmt.Errorf("failed to sync caches")
	}

	// Start scheduling worker
	go wait.Until(s.scheduleNext, 0, ctx.Done())

	<-ctx.Done()
	return nil
}

func (s *CustomScheduler) scheduleNext() {
	key, quit := s.queue.Get()
	if quit {
		return
	}
	defer s.queue.Done(key)

	podName := key.(string)
	pod, err := s.clientset.CoreV1().Pods("").Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		log.Printf("Error getting pod %s: %v", podName, err)
		return
	}

	node, err := s.scheduleOne(pod)
	if err != nil {
		log.Printf("Error scheduling pod %s: %v", podName, err)
		return
	}

	if err := s.bind(context.Background(), pod, node); err != nil {
		log.Printf("Error binding pod %s to node %s: %v", podName, node.Name, err)
		return
	}
}

// Node filtering and scoring
type NodeScore struct {
	node  *corev1.Node
	score int64
}

func (s *CustomScheduler) scheduleOne(pod *corev1.Pod) (*corev1.Node, error) {
	nodes := s.filterNodes(pod)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no suitable nodes found for pod %s", pod.Name)
	}

	scores := s.scoreNodes(pod, nodes)
	if len(scores) == 0 {
		return nil, fmt.Errorf("no nodes available after scoring for pod %s", pod.Name)
	}

	// Sort nodes by score
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	return scores[0].node, nil
}

func (s *CustomScheduler) filterNodes(pod *corev1.Pod) []*corev1.Node {
	var filteredNodes []*corev1.Node

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, node := range s.nodeLister {
		if s.nodePassesFilters(node, pod) {
			filteredNodes = append(filteredNodes, node)
		}
	}

	return filteredNodes
}

func (s *CustomScheduler) nodePassesFilters(node *corev1.Node, pod *corev1.Pod) bool {
	// Check if node is ready
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
			return false
		}
	}

	// Check if node has enough resources
	var podCPU, podMemory int64
	for _, container := range pod.Spec.Containers {
		podCPU += container.Resources.Requests.Cpu().MilliValue()
		podMemory += container.Resources.Requests.Memory().Value()
	}

	nodeCPU := node.Status.Allocatable.Cpu().MilliValue()
	nodeMemory := node.Status.Allocatable.Memory().Value()

	return nodeCPU >= podCPU && nodeMemory >= podMemory
}

func (s *CustomScheduler) scoreNodes(pod *corev1.Pod, nodes []*corev1.Node) []NodeScore {
	var scores []NodeScore

	for _, node := range nodes {
		score := s.calculateNodeScore(node, pod)
		scores = append(scores, NodeScore{
			node:  node,
			score: score,
		})
	}

	return scores
}

func (s *CustomScheduler) calculateNodeScore(node *corev1.Node, pod *corev1.Pod) int64 {
	var score int64 = 100

	log.Println("Pod:", pod.Name, "Node:", node.Name)

	// Calculate CPU score
	cpuScore := s.calculateResourceScore(
		node.Status.Allocatable.Cpu().MilliValue(),
		node.Status.Capacity.Cpu().MilliValue(),
	)

	// Calculate Memory score
	memoryScore := s.calculateResourceScore(
		node.Status.Allocatable.Memory().Value(),
		node.Status.Capacity.Memory().Value(),
	)

	// Combine scores with weights
	score = (cpuScore*60 + memoryScore*40) / 100
	return score
}

func (s *CustomScheduler) calculateResourceScore(allocatable, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	return (allocatable * 100) / capacity
}

func (s *CustomScheduler) bind(ctx context.Context, pod *corev1.Pod, node *corev1.Node) error {
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node.Name,
		},
	}

	err := s.clientset.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to bind pod %s to node %s: %v", pod.Name, node.Name, err)
	}

	log.Printf("Successfully bound pod %s to node %s", pod.Name, node.Name)
	return nil
}
