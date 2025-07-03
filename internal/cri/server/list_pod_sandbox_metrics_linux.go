/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	cg1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	cg2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	"github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/containerd/containerd/v2/internal/cri/util"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/typeurl/v2"
	"golang.org/x/time/rate"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"

	containerstore "github.com/containerd/containerd/v2/internal/cri/store/container"
	sandboxstore "github.com/containerd/containerd/v2/internal/cri/store/sandbox"
)

type containerCPUMetrics struct {
	UsageUsec          uint64
	UserUsec           uint64
	SystemUsec         uint64
	NRPeriods          uint64
	NRThrottledPeriods uint64
	ThrottledUsec      uint64
}

type containerMemoryMetrics struct {
	Cache        uint64
	RSS          uint64
	Swap         uint64
	KernelUsage  uint64
	FileMapped   uint64
	FailCount    uint64
	MemoryUsage  uint64
	MaxUsage     uint64
	WorkingSet   uint64
	ActiveFile   uint64
	InactiveFile uint64
	PgFault      uint64
	PgMajFault   uint64
}

type containerNetworkMetrics struct {
	Name      string
	RxBytes   uint64
	RxPackets uint64
	RxErrors  uint64
	RxDropped uint64
	TxBytes   uint64
	TxPackets uint64
	TxErrors  uint64
	TxDropped uint64
}

type containerPerDiskStats struct {
	Device string            `json:"device"`
	Major  uint64            `json:"major"`
	Minor  uint64            `json:"minor"`
	Stats  map[string]uint64 `json:"stats"`
}

type containerDiskIoMetrics struct {
	IoServiceBytes []containerPerDiskStats `json:"io_service_bytes,omitempty"`
	IoServiced     []containerPerDiskStats `json:"io_serviced,omitempty"`
	IoQueued       []containerPerDiskStats `json:"io_queued,omitempty"`
	Sectors        []containerPerDiskStats `json:"sectors,omitempty"`
	IoServiceTime  []containerPerDiskStats `json:"io_service_time,omitempty"`
	IoWaitTime     []containerPerDiskStats `json:"io_wait_time,omitempty"`
	IoMerged       []containerPerDiskStats `json:"io_merged,omitempty"`
	IoTime         []containerPerDiskStats `json:"io_time,omitempty"`
}

// ListPodSandboxMetrics gets pod sandbox metrics from CRI Runtime
func (c *criService) ListPodSandboxMetrics(ctx context.Context, r *runtime.ListPodSandboxMetricsRequest) (*runtime.ListPodSandboxMetricsResponse, error) {
	ctx = util.WithNamespace(ctx)
	sandboxes := c.sandboxStore.List()
	podMetrics := make([]*runtime.PodSandboxMetrics, 0, len(sandboxes))
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Rate limiter to prevent overwhelming the system with concurrent requests
	limiter := rate.NewLimiter(rate.Limit(10), 10) // Allow 10 concurrent requests with burst of 10
	semaphore := make(chan struct{}, 10)           // Limit to 10 concurrent goroutines

	activeSandboxIDs := make(map[string]bool)

	for _, sandbox := range sandboxes {
		// Only collect metrics for ready sandboxes
		if sandbox.Status.Get().State != sandboxstore.StateReady {
			continue
		}

		activeSandboxIDs[sandbox.ID] = true

		// Wait for rate limiter permission
		if err := limiter.Wait(ctx); err != nil {
			log.G(ctx).WithError(err).Debug("rate limiter context cancelled")
			break
		}

		semaphore <- struct{}{} // Acquire semaphore
		wg.Add(1)
		go func(sb sandboxstore.Sandbox) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			metrics, err := c.collectPodSandboxMetrics(ctx, sb)
			if err != nil {
				switch {
				case errdefs.IsUnavailable(err), errdefs.IsNotFound(err):
					log.G(ctx).WithField("podsandboxid", sb.ID).WithError(err).Debug("failed to get pod sandbox metrics, this is likely a transient error")
				case errdefs.IsCanceled(err):
					log.G(ctx).WithField("podsandboxid", sb.ID).WithError(err).Debug("metrics collection cancelled")
				default:
					log.G(ctx).WithField("podsandboxid", sb.ID).WithError(err).Error("failed to collect pod sandbox metrics")
				}
				return
			}

			mu.Lock()
			podMetrics = append(podMetrics, metrics)
			mu.Unlock()
		}(sandbox)
	}

	wg.Wait()

	// Clean up metrics for stopped sandboxes
	if c.metricsServer != nil {
		c.metricsServer.cleanupStoppedSandboxMetrics(activeSandboxIDs)
	}

	return &runtime.ListPodSandboxMetricsResponse{
		PodMetrics: podMetrics,
	}, nil
}

// updatePodSandboxMetrics updates the cached metrics for a specific sandbox
func (c *criService) updatePodSandboxMetrics(ctx context.Context, sandboxID string) *SandboxMetrics {
	// Always create fresh metrics instead of returning cached ones
	sm := &SandboxMetrics{
		metric: &runtime.PodSandboxMetrics{
			PodSandboxId:     sandboxID,
			Metrics:          []*runtime.Metric{},
			ContainerMetrics: []*runtime.ContainerMetrics{},
		},
	}

	// generate sandbox metrics
	request := &tasks.MetricsRequest{Filters: []string{"id==" + sandboxID}}
	resp, err := c.client.TaskService().Metrics(ctx, request)
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to fetch metrics for task")
		return sm
	}
	if len(resp.Metrics) != 1 {
		log.G(ctx).Errorf("unexpected metrics response: %+v", resp.Metrics)
		return sm
	}

	cpu, err := c.cpuMetrics(ctx, resp.Metrics[0])
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get CPU metrics")
	} else {
		sm.metric.Metrics = append(sm.metric.Metrics, generateContainerCPUMetrics(cpu)...)
	}

	memory, err := c.memoryMetrics(ctx, resp.Metrics[0])
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get memory metrics")
	} else {
		sm.metric.Metrics = append(sm.metric.Metrics, generateContainerMemoryMetrics(memory)...)
	}

	network, err := c.networkMetrics(ctx, resp.Metrics[0])
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get network metrics")
	} else {
		sm.metric.Metrics = append(sm.metric.Metrics, generateSandboxNetworkMetrics(network)...)
	}

	// get metrics for each container in the sandbox
	containers := c.containerStore.List()
	for _, container := range containers {
		if container.SandboxID == sandboxID {
			metrics, err := c.listContainerMetrics(ctx, container.ID)
			if err != nil {
				log.G(ctx).WithError(err).Errorf("failed to list metrics for container %s", container.ID)
				continue
			}
			sm.metric.ContainerMetrics = append(sm.metric.ContainerMetrics, metrics)
		}
	}

	// Safely update the metrics cache
	if c.metricsServer != nil {
		c.metricsServer.mu.Lock()
		if c.metricsServer.sandboxMetrics == nil {
			c.metricsServer.sandboxMetrics = make(map[string]*SandboxMetrics)
		}
		c.metricsServer.sandboxMetrics[sandboxID] = sm
		c.metricsServer.mu.Unlock()
	}
	return sm
}

// listContainerMetrics gives the metrics for a given container in a sandbox or a given sandbox
func (c *criService) listContainerMetrics(ctx context.Context, containerID string) (*runtime.ContainerMetrics, error) {
	request := &tasks.MetricsRequest{Filters: []string{"id==" + containerID}}
	resp, err := c.client.TaskService().Metrics(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics for task: %w", err)
	}
	if len(resp.Metrics) != 1 {
		return nil, fmt.Errorf("unexpected metrics response: %+v", resp.Metrics)
	}

	cm := &runtime.ContainerMetrics{
		ContainerId: containerID,
		Metrics:     make([]*runtime.Metric, 0),
	}

	cpu, err := c.cpuMetrics(ctx, resp.Metrics[0])
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get CPU metrics")
	} else {
		cm.Metrics = append(cm.Metrics, generateContainerCPUMetrics(cpu)...)
	}

	memory, err := c.memoryMetrics(ctx, resp.Metrics[0])
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get memory metrics")
	} else {
		cm.Metrics = append(cm.Metrics, generateContainerMemoryMetrics(memory)...)
	}

	diskio, err := c.diskIOMetrics(ctx, resp.Metrics[0])
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to get disk I/O metrics")
	} else {
		cm.Metrics = append(cm.Metrics, generateDiskIOMetrics(diskio)...)
	}

	// Collect filesystem metrics
	filesystem, err := c.collectFilesystemMetrics(ctx, containerID)
	if err != nil {
		log.G(ctx).WithError(err).Debug("failed to get filesystem metrics")
	} else {
		cm.Metrics = append(cm.Metrics, generateFilesystemMetrics(filesystem)...)
	}

	return cm, nil
}

func (c *criService) cpuMetrics(ctx context.Context, stats interface{}) (*containerCPUMetrics, error) {
	cm := &containerCPUMetrics{}
	switch metrics := stats.(type) {
	case *cg1.Metrics:
		if metrics.GetCPU() != nil {
			if usage := metrics.GetCPU().GetUsage(); usage != nil {
				cm.UserUsec = usage.GetUser()
				cm.SystemUsec = usage.GetKernel()
				cm.UsageUsec = usage.GetTotal()
			}
			if throttling := metrics.GetCPU().GetThrottling(); throttling != nil {
				cm.NRPeriods = throttling.GetPeriods()
				cm.NRThrottledPeriods = throttling.GetThrottledPeriods()
				cm.ThrottledUsec = throttling.GetThrottledTime()
			}
		}
		return cm, nil
	case *cg2.Metrics:
		if metrics.GetCPU() != nil {
			cm.UserUsec = metrics.CPU.GetUserUsec()
			cm.SystemUsec = metrics.CPU.GetSystemUsec()
			cm.UsageUsec = metrics.CPU.GetUsageUsec()
			cm.NRPeriods = metrics.CPU.GetNrPeriods()
			cm.NRThrottledPeriods = metrics.CPU.GetNrThrottled()
			cm.ThrottledUsec = metrics.CPU.GetThrottledUsec()
		}
		return cm, nil
	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", metrics, reflect.TypeOf(metrics).Elem().PkgPath())
	}
}

func (c *criService) memoryMetrics(ctx context.Context, stats interface{}) (*containerMemoryMetrics, error) {
	cm := &containerMemoryMetrics{}
	switch metrics := stats.(type) {
	case *cg1.Metrics:
		if metrics.GetMemory() != nil {
			cm.Cache = metrics.Memory.GetTotalCache()
			cm.RSS = metrics.Memory.GetTotalRSS()
			cm.FileMapped = metrics.Memory.GetMappedFile()
			cm.ActiveFile = metrics.Memory.GetTotalActiveFile()
			cm.InactiveFile = metrics.Memory.GetTotalInactiveFile()
			cm.PgFault = metrics.Memory.GetPgFault()
			cm.PgMajFault = metrics.Memory.GetPgMajFault()
			cm.WorkingSet = getWorkingSet(metrics.GetMemory())
			if usage := metrics.GetMemory().GetUsage(); usage != nil {
				cm.FailCount = usage.GetFailcnt()
				cm.MemoryUsage = usage.GetUsage()
				cm.MaxUsage = usage.GetMax()
			}
			if metrics.GetMemory().GetKernel() != nil {
				cm.KernelUsage = metrics.GetMemory().GetKernel().GetUsage()
			}
			if metrics.GetMemory().GetSwap() != nil {
				cm.Swap = metrics.GetMemory().GetSwap().GetUsage()
			}
		}
		return cm, nil
	case *cg2.Metrics:
		if metrics.GetMemory() != nil {
			cm.Cache = metrics.GetMemory().GetFile()
			cm.RSS = metrics.GetMemory().GetAnon()
			cm.KernelUsage = metrics.GetMemory().GetKernelStack()
			cm.FileMapped = metrics.GetMemory().GetFileMapped()
			cm.Swap = metrics.GetMemory().GetSwapUsage() - metrics.GetMemory().GetUsage()
			cm.MemoryUsage = metrics.GetMemory().GetUsage()
			cm.MaxUsage = metrics.GetMemory().GetMaxUsage()
			cm.ActiveFile = metrics.GetMemory().GetActiveFile()
			cm.PgFault = metrics.GetMemory().GetPgfault()
			cm.PgMajFault = metrics.GetMemory().GetPgmajfault()
			cm.WorkingSet = getWorkingSetV2(metrics.Memory)
		}
		if metrics.GetMemoryEvents() != nil {
			cm.FailCount = metrics.GetMemoryEvents().GetMax()
		}
		return cm, nil
	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", metrics, reflect.TypeOf(metrics).Elem().PkgPath())
	}
}

func (c *criService) networkMetrics(ctx context.Context, stats interface{}) ([]containerNetworkMetrics, error) {
	cm := make([]containerNetworkMetrics, 0)
	switch metrics := stats.(type) {
	case *cg1.Metrics:
		for _, m := range metrics.GetNetwork() {
			cm = append(cm, containerNetworkMetrics{
				Name:      m.GetName(),
				RxBytes:   m.GetRxBytes(),
				TxBytes:   m.GetTxBytes(),
				RxErrors:  m.GetRxErrors(),
				TxErrors:  m.GetTxErrors(),
				RxDropped: m.GetRxDropped(),
				TxDropped: m.GetTxDropped(),
				RxPackets: m.GetRxPackets(),
				TxPackets: m.GetTxPackets(),
			})
		}
		return cm, nil
	case *cg2.Metrics:
		// Note: cgroups v2 doesn't have network metrics in the same way as v1
		// Network metrics are typically collected at the pod level via netns
		return cm, nil
	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", metrics, reflect.TypeOf(metrics).Elem().PkgPath())
	}
}

// collectPodSandboxMetrics collects metrics for a specific pod sandbox and its containers
func (c *criService) collectPodSandboxMetrics(ctx context.Context, sandbox sandboxstore.Sandbox) (*runtime.PodSandboxMetrics, error) {
	meta := sandbox.Metadata
	config := sandbox.Config

	cstatus, err := c.sandboxService.SandboxStatus(ctx, sandbox.Sandboxer, sandbox.ID, true)
	if err != nil {
		return nil, fmt.Errorf("failed getting status for sandbox %s: %w", sandbox.ID, err)
	}

	// Get sandbox stats
	stats, err := metricsForSandbox(sandbox, cstatus.Info)
	if err != nil {
		return nil, fmt.Errorf("failed getting metrics for sandbox %s: %w", sandbox.ID, err)
	}

	podMetrics := &runtime.PodSandboxMetrics{
		PodSandboxId: meta.ID,
		Metrics:      []*runtime.Metric{},
	}

	timestamp := time.Now().UnixNano()

	// Extract pod-level labels
	podName := config.GetMetadata().GetName()
	namespace := config.GetMetadata().GetNamespace()
	podLabels := []string{podName, namespace, meta.ID}

	if stats != nil {
		// Collect pod-level network metrics
		if sandbox.NetNSPath != "" {
			rxBytes, rxErrors, txBytes, txErrors, rxPackets, rxDropped, txPackets, txDropped := getContainerNetIO(ctx, sandbox.NetNSPath)

			podMetrics.Metrics = append(podMetrics.Metrics, []*runtime.Metric{
				{
					Name:        "container_network_receive_bytes_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: rxBytes},
				},
				{
					Name:        "container_network_receive_packets_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: rxPackets},
				},
				{
					Name:        "container_network_receive_packets_dropped_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: rxDropped},
				},
				{
					Name:        "container_network_receive_errors_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: rxErrors},
				},
				{
					Name:        "container_network_transmit_bytes_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: txBytes},
				},
				{
					Name:        "container_network_transmit_packets_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: txPackets},
				},
				{
					Name:        "container_network_transmit_packets_dropped_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: txDropped},
				},
				{
					Name:        "container_network_transmit_errors_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: append(podLabels, "eth0"),
					Value:       &runtime.UInt64Value{Value: txErrors},
				},
			}...)
		}
	}

	// Collect container metrics
	containers := c.containerStore.List()
	for _, container := range containers {
		if container.SandboxID != sandbox.ID {
			continue
		}

		containerMetrics, err := c.collectContainerMetrics(ctx, container, podName, namespace)
		if err != nil {
			log.G(ctx).WithField("containerid", container.ID).WithError(err).Debug("failed to collect container metrics")
			continue
		}

		podMetrics.ContainerMetrics = append(podMetrics.ContainerMetrics, containerMetrics)
	}

	return podMetrics, nil
}

// collectContainerMetrics collects metrics for a specific container
func (c *criService) collectContainerMetrics(ctx context.Context, container containerstore.Container, podName, namespace string) (*runtime.ContainerMetrics, error) {
	meta := container.Metadata
	config := container.Config

	// Get container stats
	task, err := container.Container.Task(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get task for container %s: %w", container.ID, err)
	}

	taskMetrics, err := task.Metrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics for container %s: %w", container.ID, err)
	}

	stats, err := typeurl.UnmarshalAny(taskMetrics.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metrics for container %s: %w", container.ID, err)
	}

	containerName := config.GetMetadata().GetName()
	containerLabels := []string{containerName, podName, namespace, meta.ID}
	timestamp := time.Now().UnixNano()

	containerMetrics := &runtime.ContainerMetrics{
		ContainerId: meta.ID,
		Metrics:     []*runtime.Metric{},
	}

	// Collect CPU metrics
	cpuMetrics, err := c.extractCPUMetrics(stats, containerLabels, timestamp)
	if err != nil {
		log.G(ctx).WithField("containerid", container.ID).WithError(err).Debug("failed to extract CPU metrics")
	} else {
		containerMetrics.Metrics = append(containerMetrics.Metrics, cpuMetrics...)
	}

	// Collect memory metrics
	memoryMetrics, err := c.extractMemoryMetrics(stats, containerLabels, timestamp)
	if err != nil {
		log.G(ctx).WithField("containerid", container.ID).WithError(err).Debug("failed to extract memory metrics")
	} else {
		containerMetrics.Metrics = append(containerMetrics.Metrics, memoryMetrics...)
	}

	// Collect process metrics
	processMetrics, err := c.extractProcessMetrics(stats, containerLabels, timestamp)
	if err != nil {
		log.G(ctx).WithField("containerid", container.ID).WithError(err).Debug("failed to extract process metrics")
	} else {
		containerMetrics.Metrics = append(containerMetrics.Metrics, processMetrics...)
	}

	// Collect disk I/O metrics
	diskMetrics, err := c.extractDiskMetrics(stats, containerLabels, timestamp)
	if err != nil {
		log.G(ctx).WithField("containerid", container.ID).WithError(err).Debug("failed to extract disk metrics")
	} else {
		containerMetrics.Metrics = append(containerMetrics.Metrics, diskMetrics...)
	}

	// Collect filesystem metrics
	fsMetrics, err := c.extractFilesystemMetrics(ctx, container.ID, containerLabels, timestamp)
	if err != nil {
		log.G(ctx).WithField("containerid", container.ID).WithError(err).Debug("failed to extract filesystem metrics")
	} else {
		containerMetrics.Metrics = append(containerMetrics.Metrics, fsMetrics...)
	}

	return containerMetrics, nil
}

// extractCPUMetrics extracts CPU-related metrics from container stats
func (c *criService) extractCPUMetrics(stats interface{}, labels []string, timestamp int64) ([]*runtime.Metric, error) {
	var metrics []*runtime.Metric

	switch s := stats.(type) {
	case *cg1.Metrics:
		if s.CPU != nil && s.CPU.Usage != nil {
			metrics = append(metrics, &runtime.Metric{
				Name:        "container_cpu_usage_seconds_total",
				Timestamp:   timestamp,
				MetricType:  runtime.MetricType_COUNTER,
				LabelValues: labels,
				Value:       &runtime.UInt64Value{Value: s.CPU.Usage.Total / 1e9}, // Convert to seconds
			})

			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_cpu_user_seconds_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.Usage.User / 1e9},
				},
				{
					Name:        "container_cpu_system_seconds_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.Usage.Kernel / 1e9},
				},
			}...)

			if s.CPU.Throttling != nil {
				metrics = append(metrics, []*runtime.Metric{
					{
						Name:        "container_cpu_cfs_periods_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: labels,
						Value:       &runtime.UInt64Value{Value: s.CPU.Throttling.Periods},
					},
					{
						Name:        "container_cpu_cfs_throttled_periods_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: labels,
						Value:       &runtime.UInt64Value{Value: s.CPU.Throttling.ThrottledPeriods},
					},
					{
						Name:        "container_cpu_cfs_throttled_seconds_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: labels,
						Value:       &runtime.UInt64Value{Value: s.CPU.Throttling.ThrottledTime / 1e9},
					},
				}...)
			}
		}

	case *cg2.Metrics:
		if s.CPU != nil {
			metrics = append(metrics, &runtime.Metric{
				Name:        "container_cpu_usage_seconds_total",
				Timestamp:   timestamp,
				MetricType:  runtime.MetricType_COUNTER,
				LabelValues: labels,
				Value:       &runtime.UInt64Value{Value: s.CPU.UsageUsec / 1e6}, // Convert microseconds to seconds
			})

			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_cpu_user_seconds_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.UserUsec / 1e6}, // Convert microseconds to seconds
				},
				{
					Name:        "container_cpu_system_seconds_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.SystemUsec / 1e6}, // Convert microseconds to seconds
				},
			}...)

			// Always include CFS throttling metrics, even if zero
			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_cpu_cfs_periods_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.NrPeriods},
				},
				{
					Name:        "container_cpu_cfs_throttled_periods_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.NrThrottled},
				},
				{
					Name:        "container_cpu_cfs_throttled_seconds_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.CPU.ThrottledUsec / 1e6}, // Convert microseconds to seconds
				},
			}...)
		}

	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", s, reflect.TypeOf(s).Elem().PkgPath())
	}

	return metrics, nil
}

// extractMemoryMetrics extracts memory-related metrics from container stats
func (c *criService) extractMemoryMetrics(stats interface{}, labels []string, timestamp int64) ([]*runtime.Metric, error) {
	var metrics []*runtime.Metric

	switch s := stats.(type) {
	case *cg1.Metrics:
		if s.Memory != nil {
			if s.Memory.Usage != nil {
				metrics = append(metrics, []*runtime.Metric{
					{
						Name:        "container_memory_usage_bytes",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_GAUGE,
						LabelValues: labels,
						Value:       &runtime.UInt64Value{Value: s.Memory.Usage.Usage},
					},
					{
						Name:        "container_memory_working_set_bytes",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_GAUGE,
						LabelValues: labels,
						Value:       &runtime.UInt64Value{Value: getWorkingSet(s.Memory)},
					},
					{
						Name:        "container_memory_max_usage_bytes",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_GAUGE,
						LabelValues: labels,
						Value:       &runtime.UInt64Value{Value: s.Memory.Usage.Max},
					},
				}...)
			}

			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_memory_rss",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.TotalRSS},
				},
				{
					Name:        "container_memory_cache",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.TotalCache},
				},
				{
					Name:        "container_memory_mapped_file",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.TotalMappedFile},
				},
				{
					Name:        "container_memory_total_active_file_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.TotalActiveFile},
				},
				{
					Name:        "container_memory_total_inactive_file_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.TotalInactiveFile},
				},
			}...)

			// Add kernel memory metrics if available
			if s.Memory.Kernel != nil {
				metrics = append(metrics, &runtime.Metric{
					Name:        "container_memory_kernel_usage",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.Kernel.Usage},
				})
			}

			// Add swap metrics if available
			if s.Memory.Swap != nil {
				metrics = append(metrics, &runtime.Metric{
					Name:        "container_memory_swap",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.Swap.Usage},
				})
			}

			// Add usage failcnt if available
			if s.Memory.Usage != nil {
				metrics = append(metrics, &runtime.Metric{
					Name:        "container_memory_failcnt",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.Usage.Failcnt},
				})
			}
		}

	case *cg2.Metrics:
		if s.Memory != nil {
			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_memory_usage_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.Usage},
				},
				{
					Name:        "container_memory_max_usage_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.UsageLimit},
				},
				{
					Name:        "container_memory_working_set_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: getWorkingSetV2(s.Memory)},
				},
				{
					Name:        "container_memory_rss",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.Anon},
				},
				{
					Name:        "container_memory_cache",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.File},
				},
				{
					Name:        "container_memory_kernel_usage",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.KernelStack},
				},
				{
					Name:        "container_memory_mapped_file",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.FileMapped},
				},
				{
					Name:        "container_memory_swap",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.SwapUsage},
				},
				{
					Name:        "container_memory_failcnt",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: 0}, // cgroups v2 doesn't expose failcnt, provide 0
				},
				{
					Name:        "container_memory_total_active_file_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.ActiveFile},
				},
				{
					Name:        "container_memory_total_inactive_file_bytes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Memory.InactiveFile},
				},
			}...)
		}

	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", s, reflect.TypeOf(s).Elem().PkgPath())
	}

	return metrics, nil
}

// extractDiskMetrics extracts disk I/O metrics from container stats
func (c *criService) extractDiskMetrics(stats interface{}, labels []string, timestamp int64) ([]*runtime.Metric, error) {
	var metrics []*runtime.Metric

	switch s := stats.(type) {
	case *cg1.Metrics:
		if s.Blkio != nil {
			// Process blkio device usage stats
			for _, entry := range s.Blkio.IoServiceBytesRecursive {
				deviceLabels := append(labels, fmt.Sprintf("%d:%d", entry.Major, entry.Minor), fmt.Sprintf("%d", entry.Major), fmt.Sprintf("%d", entry.Minor), strings.ToLower(entry.Op))
				metrics = append(metrics, &runtime.Metric{
					Name:        "container_blkio_device_usage_total",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_COUNTER,
					LabelValues: deviceLabels,
					Value:       &runtime.UInt64Value{Value: entry.Value},
				})
			}

			// Process filesystem read/write stats
			for _, entry := range s.Blkio.IoServicedRecursive {
				device := fmt.Sprintf("%d:%d", entry.Major, entry.Minor)
				switch strings.ToLower(entry.Op) {
				case "read":
					metrics = append(metrics, &runtime.Metric{
						Name:        "container_fs_reads_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Value},
					})
				case "write":
					metrics = append(metrics, &runtime.Metric{
						Name:        "container_fs_writes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Value},
					})
				}
			}

			// Process filesystem bytes read/written
			for _, entry := range s.Blkio.IoServiceBytesRecursive {
				device := fmt.Sprintf("%d:%d", entry.Major, entry.Minor)
				switch strings.ToLower(entry.Op) {
				case "read":
					metrics = append(metrics, &runtime.Metric{
						Name:        "container_fs_reads_bytes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Value},
					})
				case "write":
					metrics = append(metrics, &runtime.Metric{
						Name:        "container_fs_writes_bytes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Value},
					})
				}
			}
		}

	case *cg2.Metrics:
		if s.Io != nil {
			// Process cgroups v2 I/O stats
			for _, entry := range s.Io.Usage {
				device := fmt.Sprintf("%d:%d", entry.Major, entry.Minor)
				
				metrics = append(metrics, []*runtime.Metric{
					{
						Name:        "container_fs_reads_bytes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Rbytes},
					},
					{
						Name:        "container_fs_writes_bytes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Wbytes},
					},
					{
						Name:        "container_fs_reads_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Rios},
					},
					{
						Name:        "container_fs_writes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Wios},
					},
					{
						Name:        "container_fs_sector_reads_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Rbytes / 512}, // Convert bytes to sectors
					},
					{
						Name:        "container_fs_sector_writes_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Wbytes / 512}, // Convert bytes to sectors
					},
					{
						Name:        "container_fs_reads_merged_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_fs_writes_merged_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_fs_read_seconds_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_fs_write_seconds_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_fs_io_current",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_GAUGE,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_fs_io_time_seconds_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_fs_io_time_weighted_seconds_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: 0}, // Not available in cgroups v2, provide 0
					},
					{
						Name:        "container_blkio_device_usage_total",
						Timestamp:   timestamp,
						MetricType:  runtime.MetricType_COUNTER,
						LabelValues: append(labels, device),
						Value:       &runtime.UInt64Value{Value: entry.Rbytes + entry.Wbytes},
					},
				}...)
			}
		}

	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", s, reflect.TypeOf(s).Elem().PkgPath())
	}

	return metrics, nil
}

// extractFilesystemMetrics extracts filesystem-related metrics from container stats
func (c *criService) extractFilesystemMetrics(ctx context.Context, containerID string, labels []string, timestamp int64) ([]*runtime.Metric, error) {
	var metrics []*runtime.Metric

	// Get filesystem usage from the container's snapshotter
	usage, limit, inodes, inodesFree, err := c.getContainerFilesystemUsage(ctx, containerID)
	if err != nil {
		// If we can't get filesystem stats, log debug and return zero values
		log.G(ctx).WithField("containerid", containerID).WithError(err).Debug("failed to get filesystem usage")
		usage = 0
		limit = 0
		inodes = 0
		inodesFree = 0
	}
	
	metrics = append(metrics, []*runtime.Metric{
		{
			Name:        "container_fs_usage_bytes",
			Timestamp:   timestamp,
			MetricType:  runtime.MetricType_GAUGE,
			LabelValues: labels,
			Value:       &runtime.UInt64Value{Value: usage},
		},
		{
			Name:        "container_fs_limit_bytes",
			Timestamp:   timestamp,
			MetricType:  runtime.MetricType_GAUGE,
			LabelValues: labels,
			Value:       &runtime.UInt64Value{Value: limit},
		},
		{
			Name:        "container_fs_inodes_total",
			Timestamp:   timestamp,
			MetricType:  runtime.MetricType_GAUGE,
			LabelValues: labels,
			Value:       &runtime.UInt64Value{Value: inodes},
		},
		{
			Name:        "container_fs_inodes_free",
			Timestamp:   timestamp,
			MetricType:  runtime.MetricType_GAUGE,
			LabelValues: labels,
			Value:       &runtime.UInt64Value{Value: inodesFree},
		},
	}...)

	return metrics, nil
}

// getContainerFilesystemUsage gets filesystem usage statistics for a container
func (c *criService) getContainerFilesystemUsage(ctx context.Context, containerID string) (usage, limit, inodes, inodesFree uint64, err error) {
	// Get the container from the store
	container, err := c.containerStore.Get(containerID)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to get container: %w", err)
	}

	// Get container info for snapshotter and snapshot key
	ctrInfo, err := container.Container.Info(ctx)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to get container info: %w", err)
	}

	// Get the snapshotter and snapshot key
	snapshotter := c.client.SnapshotService(ctrInfo.Snapshotter)
	
	// Try to get usage from the snapshotter
	snapshotUsage, err := snapshotter.Usage(ctx, ctrInfo.SnapshotKey)
	if err != nil {
		// If snapshotter usage fails, try to get the container's root filesystem path
		// and calculate usage directly
		return c.getFilesystemUsageFromPath(ctx, containerID)
	}

	// Convert snapshotter usage to our format
	usage = uint64(snapshotUsage.Size)
	
	// Get filesystem statistics from the underlying filesystem
	// For limits and inodes, we need to check the actual filesystem
	if rootPath, err := c.getContainerRootPath(ctx, containerID); err == nil {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(rootPath, &stat); err == nil {
			// Calculate filesystem total size as limit
			limit = uint64(stat.Blocks) * uint64(stat.Bsize)
			
			// Inode information
			inodes = uint64(stat.Files)
			inodesFree = uint64(stat.Ffree)
		}
	}
	
	return usage, limit, inodes, inodesFree, nil
}

// getContainerRootPath attempts to get the container's root filesystem path
func (c *criService) getContainerRootPath(ctx context.Context, containerID string) (string, error) {
	// Get the container from the store
	container, err := c.containerStore.Get(containerID)
	if err != nil {
		return "", fmt.Errorf("failed to get container: %w", err)
	}

	// Get container info for runtime information
	ctrInfo, err := container.Container.Info(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get container info: %w", err)
	}

	// Try to get the task to access the bundle
	task, err := container.Container.Task(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get container task: %w", err)
	}

	// Get the container's runtime status
	_, err = task.Status(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get task status: %w", err)
	}

	// Use a default namespace if we can't determine it
	namespace := "default"
	
	// For containerd, the typical bundle path structure is:
	// /var/lib/containerd/io.containerd.runtime.v2.task/{namespace}/{id}
	// The rootfs is typically at: {bundle}/rootfs
	bundlePath := fmt.Sprintf("%s/io.containerd.runtime.v2.task/%s/%s", 
		c.config.RootDir, namespace, containerID)
	rootfsPath := filepath.Join(bundlePath, "rootfs")
	
	// Check if the path exists
	if _, err := os.Stat(rootfsPath); err == nil {
		return rootfsPath, nil
	}
	
	// If that doesn't work, try alternate paths based on runtime
	if ctrInfo.Runtime.Name != "" {
		// Try runtime-specific paths
		if strings.Contains(ctrInfo.Runtime.Name, "runc") {
			// For runc, bundle path might be different
			bundlePath = fmt.Sprintf("%s/io.containerd.runtime.v1.linux/%s/%s", 
				c.config.RootDir, namespace, containerID)
			rootfsPath = filepath.Join(bundlePath, "rootfs")
			if _, err := os.Stat(rootfsPath); err == nil {
				return rootfsPath, nil
			}
		}
	}

	return "", fmt.Errorf("could not determine container root path")
}

// getFilesystemUsageFromPath calculates filesystem usage from a specific path
func (c *criService) getFilesystemUsageFromPath(ctx context.Context, containerID string) (usage, limit, inodes, inodesFree uint64, err error) {
	rootPath, err := c.getContainerRootPath(ctx, containerID)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	// Use syscall.Statfs to get filesystem statistics
	var stat syscall.Statfs_t
	if err := syscall.Statfs(rootPath, &stat); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to stat filesystem: %w", err)
	}

	// Calculate usage (used = total - available)
	totalBytes := uint64(stat.Blocks) * uint64(stat.Bsize)
	availableBytes := uint64(stat.Bavail) * uint64(stat.Bsize)
	usage = totalBytes - availableBytes
	
	// Filesystem limit is the total size
	limit = totalBytes
	
	// Inode information
	inodes = uint64(stat.Files)
	inodesFree = uint64(stat.Ffree)

	return usage, limit, inodes, inodesFree, nil
}

// extractProcessMetrics extracts process-related metrics from container stats
func (c *criService) extractProcessMetrics(stats interface{}, labels []string, timestamp int64) ([]*runtime.Metric, error) {
	var metrics []*runtime.Metric

	switch s := stats.(type) {
	case *cg1.Metrics:
		if s.Pids != nil {
			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_processes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Pids.Current},
				},
				{
					Name:        "container_threads_max",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Pids.Limit},
				},
			}...)
		}

	case *cg2.Metrics:
		if s.Pids != nil {
			metrics = append(metrics, []*runtime.Metric{
				{
					Name:        "container_processes",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Pids.Current},
				},
				{
					Name:        "container_threads_max",
					Timestamp:   timestamp,
					MetricType:  runtime.MetricType_GAUGE,
					LabelValues: labels,
					Value:       &runtime.UInt64Value{Value: s.Pids.Limit},
				},
			}...)
		}

	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", s, reflect.TypeOf(s).Elem().PkgPath())
	}

	return metrics, nil
}

// diskKey is used for organizing disk statistics
type diskKey struct {
	Major uint64
	Minor uint64
}

func diskStatsCopy0(major, minor uint64) *containerPerDiskStats {
	disk := containerPerDiskStats{
		Major: major,
		Minor: minor,
	}
	disk.Stats = make(map[string]uint64)
	return &disk
}

func diskStatsCopy1(diskStat map[diskKey]*containerPerDiskStats) []containerPerDiskStats {
	i := 0
	stat := make([]containerPerDiskStats, len(diskStat))
	for _, disk := range diskStat {
		stat[i] = *disk
		i++
	}
	return stat
}

func convertBlkIOEntryPointers(entries []*cg1.BlkIOEntry) []cg1.BlkIOEntry {
	if entries == nil {
		return nil
	}
	result := make([]cg1.BlkIOEntry, len(entries))
	for i, entry := range entries {
		if entry != nil {
			result[i] = *entry
		}
	}
	return result
}

func diskStatsCopyCG1(blkioStats []cg1.BlkIOEntry) (stat []containerPerDiskStats) {
	if len(blkioStats) == 0 {
		return
	}
	diskStat := make(map[diskKey]*containerPerDiskStats)
	for i := range blkioStats {
		major := blkioStats[i].Major
		minor := blkioStats[i].Minor
		key := diskKey{
			Major: major,
			Minor: minor,
		}
		diskp, ok := diskStat[key]
		if !ok {
			diskp = diskStatsCopy0(major, minor)
			diskStat[key] = diskp
		}
		op := blkioStats[i].Op
		if op == "" {
			op = "Count"
		}
		diskp.Stats[op] = blkioStats[i].Value
	}
	return diskStatsCopy1(diskStat)
}

func (c *criService) diskIOMetrics(ctx context.Context, stats interface{}) (*containerDiskIoMetrics, error) {
	cm := &containerDiskIoMetrics{}
	switch metrics := stats.(type) {
	case *cg1.Metrics:
		if metrics.Blkio != nil {
			cm.IoQueued = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoQueuedRecursive()))
			cm.IoMerged = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoMergedRecursive()))
			cm.IoServiceBytes = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoServiceBytesRecursive()))
			cm.IoServiced = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoServicedRecursive()))
			cm.IoTime = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoTimeRecursive()))
			cm.IoServiceTime = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoServiceTimeRecursive()))
			cm.IoWaitTime = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetIoWaitTimeRecursive()))
			cm.Sectors = diskStatsCopyCG1(convertBlkIOEntryPointers(metrics.Blkio.GetSectorsRecursive()))
		}
	case *cg2.Metrics:
		// For cgroups v2, we need to get I/O stats from the io controller
		if metrics.Io != nil {
			// Convert cgroups v2 IO stats to our format
			for _, ioEntry := range metrics.Io.Usage {
				diskStat := containerPerDiskStats{
					Major:  ioEntry.Major,
					Minor:  ioEntry.Minor,
					Device: fmt.Sprintf("%d:%d", ioEntry.Major, ioEntry.Minor),
					Stats:  make(map[string]uint64),
				}

				// Map cgroups v2 IO stats to our structure
				diskStat.Stats["Read"] = ioEntry.Rbytes
				diskStat.Stats["Write"] = ioEntry.Wbytes
				diskStat.Stats["Sync"] = ioEntry.Rios
				diskStat.Stats["Async"] = ioEntry.Wios
				diskStat.Stats["Total"] = ioEntry.Rios + ioEntry.Wios

				cm.IoServiceBytes = append(cm.IoServiceBytes, diskStat)

				// Create separate entries for read/write operations
				readStat := containerPerDiskStats{
					Major:  ioEntry.Major,
					Minor:  ioEntry.Minor,
					Device: fmt.Sprintf("%d:%d", ioEntry.Major, ioEntry.Minor),
					Stats:  map[string]uint64{"Read": ioEntry.Rios},
				}
				writeStat := containerPerDiskStats{
					Major:  ioEntry.Major,
					Minor:  ioEntry.Minor,
					Device: fmt.Sprintf("%d:%d", ioEntry.Major, ioEntry.Minor),
					Stats:  map[string]uint64{"Write": ioEntry.Wios},
				}

				cm.IoServiced = append(cm.IoServiced, readStat, writeStat)
			}
		}
	default:
		return nil, fmt.Errorf("unexpected metrics type: %T from %s", metrics, reflect.TypeOf(metrics).Elem().PkgPath())
	}
	return cm, nil
}

// ioValues is a helper method for assembling per-disk and per-filesystem stats.
func ioValues(ioStats []containerPerDiskStats, ioType string) metricValues {
	values := make(metricValues, 0, len(ioStats))
	for _, stat := range ioStats {
		values = append(values, metricValue{
			value:      stat.Stats[ioType],
			labels:     []string{stat.Device},
			metricType: runtime.MetricType_COUNTER,
		})
	}
	return values
}

// Metric generation functions
func generateSandboxNetworkMetrics(metrics []containerNetworkMetrics) []*runtime.Metric {
	nm := containerNetworkMetrics{}
	// Aggregate metrics from all interfaces
	for _, m := range metrics {
		nm.RxBytes += m.RxBytes
		nm.RxPackets += m.RxPackets
		nm.RxErrors += m.RxErrors
		nm.RxDropped += m.RxDropped
		nm.TxBytes += m.TxBytes
		nm.TxPackets += m.TxPackets
		nm.TxErrors += m.TxErrors
		nm.TxDropped += m.TxDropped
	}

	networkMetrics := []*containerMetric{
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_receive_bytes_total",
				Help: "Cumulative count of bytes received",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.RxBytes,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_receive_packets_total",
				Help: "Cumulative count of packets received",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.RxPackets,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_receive_packets_dropped_total",
				Help: "Cumulative count of packets dropped while receiving packets",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.RxDropped,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_receive_errors_total",
				Help: "Cumulative count of errors encountered while receiving",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.RxErrors,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_transmit_bytes_total",
				Help: "Cumulative count of bytes transmitted",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.TxBytes,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_transmit_packets_total",
				Help: "Cumulative count of packets transmitted",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.TxPackets,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_transmit_packets_dropped_total",
				Help: "Cumulative count of packets dropped while transmitting",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.TxDropped,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_network_transmit_errors_total",
				Help: "Cumulative count of errors encountered while transmitting",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      nm.TxErrors,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
	}
	return computeSandboxMetrics(networkMetrics, "network")
}

func generateContainerCPUMetrics(metrics *containerCPUMetrics) []*runtime.Metric {
	cpuMetrics := []*containerMetric{
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_cpu_usage_seconds_total",
				Help: "Cumulative user CPU time consumed in seconds",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.UsageUsec,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_cpu_user_seconds_total",
				Help: "Cumulative user CPU time consumed in seconds",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.UserUsec,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_cpu_system_seconds_total",
				Help: "Cumulative system CPU time consumed in seconds",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.SystemUsec,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_cpu_cfs_periods_total",
				Help: "Number of elapsed enforcement period intervals.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.NRPeriods,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_cpu_cfs_throttled_periods_total",
				Help: "Number of throttled period intervals.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.NRThrottledPeriods,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_cpu_cfs_throttled_seconds_total",
				Help: "Total time duration the container has been throttled.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.ThrottledUsec,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
	}

	return computeSandboxMetrics(cpuMetrics, "cpu")
}

func generateContainerMemoryMetrics(metrics *containerMemoryMetrics) []*runtime.Metric {
	memoryMetrics := []*containerMetric{
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_cache",
				Help: "Number of bytes of page cache memory.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.Cache,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_rss",
				Help: "Size of RSS in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.RSS,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_swap",
				Help: "Container swap usage in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.Swap,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_kernel_usage",
				Help: "Size of kernel memory allocated in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.KernelUsage,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_mapped_file",
				Help: "Size of memory mapped files in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.FileMapped,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_failcnt",
				Help: "Number of memory usage hits limits",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.FailCount,
					metricType: runtime.MetricType_COUNTER,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_usage_bytes",
				Help: "Current memory usage in bytes, including all memory regardless of when it was accessed",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.MemoryUsage,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_max_usage_bytes",
				Help: "Maximum memory usage recorded in bytes",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.MaxUsage,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_working_set_bytes",
				Help: "Current working set in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.WorkingSet,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_total_active_file_bytes",
				Help: "Current total active file in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.ActiveFile,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_memory_total_inactive_file_bytes",
				Help: "Current total inactive file in bytes.",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.InactiveFile,
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
	}
	return computeSandboxMetrics(memoryMetrics, "memory")
}

func generateDiskIOMetrics(metrics *containerDiskIoMetrics) []*runtime.Metric {
	diskIOMetrics := []*containerMetric{
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_reads_bytes_total",
				Help: "Cumulative count of bytes read",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoServiceBytes, "Read")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_writes_bytes_total",
				Help: "Cumulative count of bytes written",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoServiceBytes, "Write")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_reads_total",
				Help: "Cumulative count of reads completed",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoServiced, "Read")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_writes_total",
				Help: "Cumulative count of writes completed",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoServiced, "Write")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_sector_reads_total",
				Help: "Cumulative count of sector reads completed",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.Sectors, "Read")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_sector_writes_total",
				Help: "Cumulative count of sector writes completed",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.Sectors, "Write")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_reads_merged_total",
				Help: "Cumulative count of reads merged",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoMerged, "Read")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_writes_merged_total",
				Help: "Cumulative count of writes merged",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoMerged, "Write")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_read_seconds_total",
				Help: "Cumulative count of seconds spent reading",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoServiceTime, "Read")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_write_seconds_total",
				Help: "Cumulative count of seconds spent writing",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoServiceTime, "Write")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_io_current",
				Help: "Number of I/Os currently in progress",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoQueued, "Total")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_io_time_seconds_total",
				Help: "Cumulative count of seconds spent doing I/Os",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoTime, "Total")
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_io_time_weighted_seconds_total",
				Help: "Cumulative weighted I/O time in seconds",
			},
			valueFunc: func() metricValues {
				return ioValues(metrics.IoWaitTime, "Total")
			},
		},
	}
	return computeSandboxMetrics(diskIOMetrics, "diskIO")
}

// computeSandboxMetrics computes the metrics for both pod and container sandbox.
func computeSandboxMetrics(metrics []*containerMetric, metricName string) []*runtime.Metric {
	values := []string{metricName}
	calculatedMetrics := make([]*runtime.Metric, 0, len(metrics))

	for _, m := range metrics {
		for _, v := range m.valueFunc() {
			newMetric := &runtime.Metric{
				Name:        m.desc.Name,
				Timestamp:   time.Now().UnixNano(),
				MetricType:  v.metricType,
				Value:       &runtime.UInt64Value{Value: v.value},
				LabelValues: append(values, v.labels...),
			}
			calculatedMetrics = append(calculatedMetrics, newMetric)
		}
	}

	return calculatedMetrics
}

type containerFilesystemMetrics struct {
	Device          string
	Type            string
	Limit           uint64
	Usage           uint64
	BaseUsage       uint64
	Available       uint64
	Inodes          uint64
	InodesFree      uint64
	InodesUsed      uint64
	ReadsCompleted  uint64
	ReadsMerged     uint64
	SectorsRead     uint64
	ReadTime        uint64
	WritesCompleted uint64
	WritesMerged    uint64
	SectorsWritten  uint64
	WriteTime       uint64
	IoInProgress    uint64
	IoTime          uint64
	WeightedIoTime  uint64
}

// collectFilesystemMetrics collects filesystem usage metrics for a container
func (c *criService) collectFilesystemMetrics(ctx context.Context, containerID string) (*containerFilesystemMetrics, error) {
	// Get container information
	container, err := c.containerStore.Get(containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get container %s: %w", containerID, err)
	}

	// Get container task to find the root filesystem path
	_, err = container.Container.Task(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get task for container %s: %w", containerID, err)
	}

	// Get container spec to find mount information
	spec, err := container.Container.Spec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get spec for container %s: %w", containerID, err)
	}

	fsMetrics := &containerFilesystemMetrics{
		Device: "overlay", // Default for most containerd setups
		Type:   "overlay",
	}

	// Try to get filesystem usage from the container's root filesystem
	// This is a simplified approach - in production, you'd want to:
	// 1. Determine the actual storage driver (overlay2, devicemapper, etc.)
	// 2. Get the correct mount path for the container
	// 3. Handle different storage drivers appropriately

	if spec.Root != nil && spec.Root.Path != "" {
		// Use a simple approach to get filesystem stats
		// In a real implementation, you'd use syscalls like statfs
		if usage, err := c.getDirectoryUsage(spec.Root.Path); err == nil {
			fsMetrics.Usage = usage.Bytes
			fsMetrics.Inodes = usage.Inodes
			fsMetrics.InodesUsed = usage.Inodes
		}
	}

	return fsMetrics, nil
}

// getDirectoryUsage implements cAdvisor's GetDirUsage function for filesystem usage calculation
func (c *criService) getDirectoryUsage(dir string) (*containerFilesystemUsage, error) {
	usage := &containerFilesystemUsage{}
	const maxFiles = 100000 // Limit to prevent excessive memory usage
	fileCount := 0

	// Get root device ID for device boundary checking
	rootInfo, err := os.Stat(dir)
	if err != nil {
		return usage, fmt.Errorf("failed to stat root directory %s: %w", dir, err)
	}
	rootStat := rootInfo.Sys().(*syscall.Stat_t)
	rootDevID := rootStat.Dev

	// Track hardlinked files to avoid double-counting
	dedupedInodes := make(map[uint64]bool)

	// Use filepath.Walk to traverse the directory with cAdvisor-like optimizations
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return nil // Skip permission errors but continue
			}
			return err
		}

		// Get system-specific file stats
		stat := info.Sys().(*syscall.Stat_t)

		// Device boundary checking: skip directories on different devices
		if stat.Dev != rootDevID {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Limit the number of files processed to prevent memory exhaustion
		fileCount++
		if fileCount > maxFiles {
			return fmt.Errorf("directory contains too many files (>%d), aborting to prevent memory exhaustion", maxFiles)
		}

		// Handle hardlink deduplication
		if info.Mode().IsRegular() && stat.Nlink > 1 {
			if dedupedInodes[stat.Ino] {
				// Already counted this inode
				return nil
			}
			dedupedInodes[stat.Ino] = true
		}

		if info.Mode().IsRegular() {
			// Use block-based calculation for more accurate disk usage
			usage.Bytes += uint64(stat.Blocks) * 512 // stat.Blocks is in 512-byte units
			usage.Inodes++
		} else if info.Mode().IsDir() {
			usage.Inodes++
		}

		return nil
	})

	return usage, err
}

type containerFilesystemUsage struct {
	Bytes  uint64
	Inodes uint64
}

func generateFilesystemMetrics(metrics *containerFilesystemMetrics) []*runtime.Metric {
	filesystemMetrics := []*containerMetric{
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_usage_bytes",
				Help: "Number of bytes that are consumed by the container on this filesystem",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.Usage,
					labels:     []string{metrics.Device},
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_limit_bytes",
				Help: "Number of bytes that can be consumed by the container on this filesystem",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.Limit,
					labels:     []string{metrics.Device},
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_inodes_total",
				Help: "Number of inodes",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.Inodes,
					labels:     []string{metrics.Device},
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
		{
			desc: &runtime.MetricDescriptor{
				Name: "container_fs_inodes_free",
				Help: "Number of available inodes",
			},
			valueFunc: func() metricValues {
				return metricValues{{
					value:      metrics.InodesFree,
					labels:     []string{metrics.Device},
					metricType: runtime.MetricType_GAUGE,
				}}
			},
		},
	}
	return computeSandboxMetrics(filesystemMetrics, "filesystem")
}
