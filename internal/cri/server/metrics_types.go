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
	"sync"
	"time"

	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type MetricsServer struct {
	collectionPeriod time.Duration
	sandboxMetrics   map[string]*SandboxMetrics
	mu               sync.RWMutex
}

type SandboxMetrics struct {
	metric *runtime.PodSandboxMetrics
}

type metricValue struct {
	value      uint64
	labels     []string
	metricType runtime.MetricType
}

type metricValues []metricValue

type containerMetric struct {
	desc      *runtime.MetricDescriptor
	valueFunc func() metricValues
}

// getMetrics retrieves cached metrics for a sandbox
func (m *MetricsServer) getMetrics(sandBoxID string) *runtime.PodSandboxMetrics {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.sandboxMetrics == nil {
		return nil
	}

	sm, ok := m.sandboxMetrics[sandBoxID]
	if !ok || sm == nil {
		return nil
	}
	return sm.metric
}

// cleanupStoppedSandboxMetrics removes metrics for sandboxes that are no longer running
func (m *MetricsServer) cleanupStoppedSandboxMetrics(activeSandboxIDs map[string]bool) {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sandboxMetrics == nil {
		return
	}

	for sandboxID := range m.sandboxMetrics {
		if !activeSandboxIDs[sandboxID] {
			delete(m.sandboxMetrics, sandboxID)
		}
	}
}
