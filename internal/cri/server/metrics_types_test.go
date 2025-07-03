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
	"testing"
	"time"

	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestMetricsServerGetMetrics(t *testing.T) {
	tests := []struct {
		name           string
		metricsServer  *MetricsServer
		sandboxID      string
		expectedResult *runtime.PodSandboxMetrics
		expectedNil    bool
	}{
		{
			name:          "nil metrics server",
			metricsServer: nil,
			sandboxID:     "sandbox1",
			expectedNil:   true,
		},
		{
			name: "nil sandbox metrics map",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics:   nil,
				mu:               sync.RWMutex{},
			},
			sandboxID:   "sandbox1",
			expectedNil: true,
		},
		{
			name: "empty sandbox metrics map",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics:   make(map[string]*SandboxMetrics),
				mu:               sync.RWMutex{},
			},
			sandboxID:   "sandbox1",
			expectedNil: true,
		},
		{
			name: "sandbox not found",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics: map[string]*SandboxMetrics{
					"other-sandbox": {
						metric: &runtime.PodSandboxMetrics{
							PodSandboxId: "other-sandbox",
						},
					},
				},
				mu: sync.RWMutex{},
			},
			sandboxID:   "sandbox1",
			expectedNil: true,
		},
		{
			name: "nil sandbox metrics entry",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics: map[string]*SandboxMetrics{
					"sandbox1": nil,
				},
				mu: sync.RWMutex{},
			},
			sandboxID:   "sandbox1",
			expectedNil: true,
		},
		{
			name: "valid sandbox metrics",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics: map[string]*SandboxMetrics{
					"sandbox1": {
						metric: &runtime.PodSandboxMetrics{
							PodSandboxId: "sandbox1",
							Metrics: []*runtime.Metric{
								{
									Name:      "test_metric",
									Timestamp: time.Now().UnixNano(),
									Value:     &runtime.UInt64Value{Value: 100},
								},
							},
						},
					},
				},
				mu: sync.RWMutex{},
			},
			sandboxID: "sandbox1",
			expectedResult: &runtime.PodSandboxMetrics{
				PodSandboxId: "sandbox1",
				Metrics: []*runtime.Metric{
					{
						Name:      "test_metric",
						Timestamp: time.Now().UnixNano(),
						Value:     &runtime.UInt64Value{Value: 100},
					},
				},
			},
			expectedNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.metricsServer.getMetrics(tt.sandboxID)

			if tt.expectedNil {
				if result != nil {
					t.Errorf("getMetrics() = %v, want nil", result)
				}
				return
			}

			if result == nil {
				t.Error("getMetrics() returned nil when valid result expected")
				return
			}

			if result.PodSandboxId != tt.expectedResult.PodSandboxId {
				t.Errorf("getMetrics().PodSandboxId = %v, want %v", result.PodSandboxId, tt.expectedResult.PodSandboxId)
			}

			if len(result.Metrics) != len(tt.expectedResult.Metrics) {
				t.Errorf("getMetrics().Metrics length = %v, want %v", len(result.Metrics), len(tt.expectedResult.Metrics))
			}
		})
	}
}

func TestMetricsServerCleanupStoppedSandboxMetrics(t *testing.T) {
	tests := []struct {
		name                 string
		metricsServer        *MetricsServer
		activeSandboxIDs     map[string]bool
		expectedRemainingIDs []string
	}{
		{
			name:          "nil metrics server",
			metricsServer: nil,
			activeSandboxIDs: map[string]bool{
				"active1": true,
			},
			expectedRemainingIDs: nil,
		},
		{
			name: "nil sandbox metrics map",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics:   nil,
				mu:               sync.RWMutex{},
			},
			activeSandboxIDs: map[string]bool{
				"active1": true,
			},
			expectedRemainingIDs: nil,
		},
		{
			name: "empty sandbox metrics map",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics:   make(map[string]*SandboxMetrics),
				mu:               sync.RWMutex{},
			},
			activeSandboxIDs: map[string]bool{
				"active1": true,
			},
			expectedRemainingIDs: []string{},
		},
		{
			name: "cleanup stopped sandboxes",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics: map[string]*SandboxMetrics{
					"active1": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "active1"},
					},
					"stopped1": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "stopped1"},
					},
					"active2": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "active2"},
					},
					"stopped2": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "stopped2"},
					},
				},
				mu: sync.RWMutex{},
			},
			activeSandboxIDs: map[string]bool{
				"active1": true,
				"active2": true,
			},
			expectedRemainingIDs: []string{"active1", "active2"},
		},
		{
			name: "cleanup all sandboxes",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics: map[string]*SandboxMetrics{
					"stopped1": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "stopped1"},
					},
					"stopped2": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "stopped2"},
					},
				},
				mu: sync.RWMutex{},
			},
			activeSandboxIDs:     map[string]bool{},
			expectedRemainingIDs: []string{},
		},
		{
			name: "keep all sandboxes",
			metricsServer: &MetricsServer{
				collectionPeriod: time.Second,
				sandboxMetrics: map[string]*SandboxMetrics{
					"active1": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "active1"},
					},
					"active2": {
						metric: &runtime.PodSandboxMetrics{PodSandboxId: "active2"},
					},
				},
				mu: sync.RWMutex{},
			},
			activeSandboxIDs: map[string]bool{
				"active1": true,
				"active2": true,
			},
			expectedRemainingIDs: []string{"active1", "active2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the cleanup function
			tt.metricsServer.cleanupStoppedSandboxMetrics(tt.activeSandboxIDs)

			// Check the remaining sandbox IDs
			if tt.metricsServer == nil || tt.metricsServer.sandboxMetrics == nil {
				if tt.expectedRemainingIDs != nil && len(tt.expectedRemainingIDs) > 0 {
					t.Errorf("Expected remaining IDs %v, but metrics server or sandbox metrics is nil", tt.expectedRemainingIDs)
				}
				return
			}

			remainingIDs := make([]string, 0, len(tt.metricsServer.sandboxMetrics))
			for id := range tt.metricsServer.sandboxMetrics {
				remainingIDs = append(remainingIDs, id)
			}

			if len(remainingIDs) != len(tt.expectedRemainingIDs) {
				t.Errorf("cleanupStoppedSandboxMetrics() remaining count = %v, want %v", len(remainingIDs), len(tt.expectedRemainingIDs))
				return
			}

			// Check that all expected IDs are present
			expectedMap := make(map[string]bool)
			for _, id := range tt.expectedRemainingIDs {
				expectedMap[id] = true
			}

			for _, id := range remainingIDs {
				if !expectedMap[id] {
					t.Errorf("cleanupStoppedSandboxMetrics() found unexpected remaining ID: %v", id)
				}
			}
		})
	}
}

func TestMetricsServerConcurrentAccess(t *testing.T) {
	// Test concurrent access to metrics server
	metricsServer := &MetricsServer{
		collectionPeriod: time.Second,
		sandboxMetrics: map[string]*SandboxMetrics{
			"sandbox1": {
				metric: &runtime.PodSandboxMetrics{PodSandboxId: "sandbox1"},
			},
			"sandbox2": {
				metric: &runtime.PodSandboxMetrics{PodSandboxId: "sandbox2"},
			},
		},
		mu: sync.RWMutex{},
	}

	// Start multiple goroutines to access metrics concurrently
	done := make(chan bool, 20) // Buffered channel

	// Readers
	for i := 0; i < 5; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < 10; j++ {
				sandboxID := "sandbox1"
				if j%2 == 0 {
					sandboxID = "sandbox2"
				}

				_ = metricsServer.getMetrics(sandboxID)
			}
		}(i)
	}

	// Cleanup operations
	for i := 0; i < 3; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < 5; j++ {
				activeSandboxes := map[string]bool{
					"sandbox1": j%2 == 0,
					"sandbox2": j%3 == 0,
				}
				metricsServer.cleanupStoppedSandboxMetrics(activeSandboxes)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 8; i++ {
		<-done
	}

	// Check that no panics occurred (test will fail if panic happens)
	t.Log("Concurrent access test completed successfully")
}

func TestSandboxMetricsStruct(t *testing.T) {
	// Test the SandboxMetrics struct
	sandboxMetrics := &SandboxMetrics{
		metric: &runtime.PodSandboxMetrics{
			PodSandboxId: "test-sandbox",
			Metrics: []*runtime.Metric{
				{
					Name:      "test_metric",
					Timestamp: time.Now().UnixNano(),
					Value:     &runtime.UInt64Value{Value: 42},
				},
			},
		},
	}

	if sandboxMetrics.metric.PodSandboxId != "test-sandbox" {
		t.Errorf("SandboxMetrics.metric.PodSandboxId = %v, want %v", sandboxMetrics.metric.PodSandboxId, "test-sandbox")
	}

	if len(sandboxMetrics.metric.Metrics) != 1 {
		t.Errorf("SandboxMetrics.metric.Metrics length = %v, want %v", len(sandboxMetrics.metric.Metrics), 1)
	}

	if sandboxMetrics.metric.Metrics[0].Name != "test_metric" {
		t.Errorf("SandboxMetrics.metric.Metrics[0].Name = %v, want %v", sandboxMetrics.metric.Metrics[0].Name, "test_metric")
	}
}

func TestMetricValueAndTypes(t *testing.T) {
	// Test the metric value types
	mv := metricValue{
		value:      123,
		labels:     []string{"label1", "label2"},
		metricType: runtime.MetricType_COUNTER,
	}

	if mv.value != 123 {
		t.Errorf("metricValue.value = %v, want %v", mv.value, 123)
	}

	if len(mv.labels) != 2 {
		t.Errorf("metricValue.labels length = %v, want %v", len(mv.labels), 2)
	}

	if mv.metricType != runtime.MetricType_COUNTER {
		t.Errorf("metricValue.metricType = %v, want %v", mv.metricType, runtime.MetricType_COUNTER)
	}

	// Test metricValues slice
	mvs := metricValues{mv}
	if len(mvs) != 1 {
		t.Errorf("metricValues length = %v, want %v", len(mvs), 1)
	}

	// Test containerMetric
	cm := &containerMetric{
		desc: &runtime.MetricDescriptor{
			Name: "test_metric",
			Help: "Test metric description",
		},
		valueFunc: func() metricValues {
			return metricValues{mv}
		},
	}

	if cm.desc.Name != "test_metric" {
		t.Errorf("containerMetric.desc.Name = %v, want %v", cm.desc.Name, "test_metric")
	}

	values := cm.valueFunc()
	if len(values) != 1 {
		t.Errorf("containerMetric.valueFunc() length = %v, want %v", len(values), 1)
	}
}
