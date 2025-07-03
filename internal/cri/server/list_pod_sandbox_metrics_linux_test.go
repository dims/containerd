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
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	cg1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	cg2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestCpuMetrics(t *testing.T) {
	c := newTestCRIService()

	tests := []struct {
		name     string
		input    interface{}
		expected *containerCPUMetrics
		wantErr  bool
	}{
		{
			name: "cgroup v1 metrics",
			input: &cg1.Metrics{
				CPU: &cg1.CPUStat{
					Usage: &cg1.CPUUsage{
						Total:  1000000000, // 1 second in nanoseconds
						User:   600000000,  // 0.6 seconds
						Kernel: 400000000,  // 0.4 seconds
					},
					Throttling: &cg1.Throttle{
						Periods:          100,
						ThrottledPeriods: 10,
						ThrottledTime:    50000000, // 50ms in nanoseconds
					},
				},
			},
			expected: &containerCPUMetrics{
				UsageUsec:          1000000000,
				UserUsec:           600000000,
				SystemUsec:         400000000,
				NRPeriods:          100,
				NRThrottledPeriods: 10,
				ThrottledUsec:      50000000,
			},
			wantErr: false,
		},
		{
			name: "cgroup v2 metrics",
			input: &cg2.Metrics{
				CPU: &cg2.CPUStat{
					UsageUsec:     1000000, // 1 second in microseconds
					UserUsec:      600000,  // 0.6 seconds
					SystemUsec:    400000,  // 0.4 seconds
					NrPeriods:     100,
					NrThrottled:   10,
					ThrottledUsec: 50000, // 50ms in microseconds
				},
			},
			expected: &containerCPUMetrics{
				UsageUsec:          1000000,
				UserUsec:           600000,
				SystemUsec:         400000,
				NRPeriods:          100,
				NRThrottledPeriods: 10,
				ThrottledUsec:      50000,
			},
			wantErr: false,
		},
		{
			name:     "invalid metrics type",
			input:    "invalid",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.cpuMetrics(context.Background(), tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("cpuMetrics() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("cpuMetrics() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMemoryMetrics(t *testing.T) {
	c := newTestCRIService()

	tests := []struct {
		name     string
		input    interface{}
		expected *containerMemoryMetrics
		wantErr  bool
	}{
		{
			name: "cgroup v1 metrics",
			input: &cg1.Metrics{
				Memory: &cg1.MemoryStat{
					TotalCache:        1024000,
					TotalRSS:          2048000,
					MappedFile:        512000,
					TotalActiveFile:   256000,
					TotalInactiveFile: 128000,
					PgFault:           1000,
					PgMajFault:        10,
					Usage: &cg1.MemoryEntry{
						Usage:   4096000,
						Max:     8192000,
						Failcnt: 5,
					},
					Kernel: &cg1.MemoryEntry{
						Usage: 64000,
					},
					Swap: &cg1.MemoryEntry{
						Usage: 32000,
					},
				},
			},
			expected: &containerMemoryMetrics{
				Cache:        1024000,
				RSS:          2048000,
				FileMapped:   512000,
				ActiveFile:   256000,
				InactiveFile: 128000,
				PgFault:      1000,
				PgMajFault:   10,
				MemoryUsage:  4096000,
				MaxUsage:     8192000,
				FailCount:    5,
				KernelUsage:  64000,
				Swap:         32000,
				WorkingSet:   4096000 - 128000, // Usage - InactiveFile
			},
			wantErr: false,
		},
		{
			name: "cgroup v2 metrics",
			input: &cg2.Metrics{
				Memory: &cg2.MemoryStat{
					File:        1024000,
					Anon:        2048000,
					KernelStack: 64000,
					FileMapped:  512000,
					SwapUsage:   32000,
					Usage:       4096000,
					MaxUsage:    8192000,
					ActiveFile:  256000,
					Pgfault:     1000,
					Pgmajfault:  10,
				},
				MemoryEvents: &cg2.MemoryEvents{
					Max: 5,
				},
			},
			expected: &containerMemoryMetrics{
				Cache:       1024000,
				RSS:         2048000,
				KernelUsage: 64000,
				FileMapped:  512000,
				Swap:        32000 - 4096000, // SwapUsage - Usage
				MemoryUsage: 4096000,
				MaxUsage:    8192000,
				ActiveFile:  256000,
				PgFault:     1000,
				PgMajFault:  10,
				FailCount:   5,
				WorkingSet:  4096000, // Simplified for v2
			},
			wantErr: false,
		},
		{
			name:     "invalid metrics type",
			input:    "invalid",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.memoryMetrics(context.Background(), tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("memoryMetrics() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("memoryMetrics() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNetworkMetrics(t *testing.T) {
	c := newTestCRIService()

	tests := []struct {
		name     string
		input    interface{}
		expected []containerNetworkMetrics
		wantErr  bool
	}{
		{
			name: "cgroup v1 network metrics",
			input: &cg1.Metrics{
				Network: []*cg1.NetworkStat{
					{
						Name:      "eth0",
						RxBytes:   1024,
						TxBytes:   2048,
						RxErrors:  1,
						TxErrors:  2,
						RxDropped: 3,
						TxDropped: 4,
						RxPackets: 10,
						TxPackets: 20,
					},
				},
			},
			expected: []containerNetworkMetrics{
				{
					Name:      "eth0",
					RxBytes:   1024,
					TxBytes:   2048,
					RxErrors:  1,
					TxErrors:  2,
					RxDropped: 3,
					TxDropped: 4,
					RxPackets: 10,
					TxPackets: 20,
				},
			},
			wantErr: false,
		},
		{
			name: "cgroup v2 network metrics",
			input: &cg2.Metrics{
				Network: []*cg2.NetworkStat{
					{
						Name:      "eth0",
						RxBytes:   1024,
						TxBytes:   2048,
						RxErrors:  1,
						TxErrors:  2,
						RxDropped: 3,
						TxDropped: 4,
						RxPackets: 10,
						TxPackets: 20,
					},
				},
			},
			expected: []containerNetworkMetrics{
				{
					Name:      "eth0",
					RxBytes:   1024,
					TxBytes:   2048,
					RxErrors:  1,
					TxErrors:  2,
					RxDropped: 3,
					TxDropped: 4,
					RxPackets: 10,
					TxPackets: 20,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.networkMetrics(context.Background(), tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("networkMetrics() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("networkMetrics() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDiskIOMetrics(t *testing.T) {
	c := newTestCRIService()

	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
	}{
		{
			name: "cgroup v1 blkio metrics",
			input: &cg1.Metrics{
				Blkio: &cg1.BlkIOStat{
					IoServiceBytesRecursive: []*cg1.BlkIOEntry{
						{Major: 8, Minor: 0, Op: "Read", Value: 1024},
						{Major: 8, Minor: 0, Op: "Write", Value: 2048},
					},
					IoServicedRecursive: []*cg1.BlkIOEntry{
						{Major: 8, Minor: 0, Op: "Read", Value: 10},
						{Major: 8, Minor: 0, Op: "Write", Value: 20},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "cgroup v2 io metrics",
			input: &cg2.Metrics{
				Io: &cg2.IOStat{
					Usage: []*cg2.IOEntry{
						{
							Major:  8,
							Minor:  0,
							Rbytes: 1024,
							Wbytes: 2048,
							Rios:   10,
							Wios:   20,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "invalid metrics type",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.diskIOMetrics(context.Background(), tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("diskIOMetrics() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result == nil {
				t.Error("diskIOMetrics() returned nil result when error not expected")
			}
		})
	}
}

func TestGetDirectoryUsage(t *testing.T) {
	c := newTestCRIService()

	// Create a temporary directory with some files
	tmpDir, err := os.MkdirTemp("", "test_dir_usage")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	testFile1 := filepath.Join(tmpDir, "file1.txt")
	testFile2 := filepath.Join(tmpDir, "file2.txt")
	subDir := filepath.Join(tmpDir, "subdir")

	if err := os.WriteFile(testFile1, []byte("hello world"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	if err := os.WriteFile(testFile2, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}

	usage, err := c.getDirectoryUsage(tmpDir)
	if err != nil {
		t.Errorf("getDirectoryUsage() error = %v", err)
		return
	}

	// Should have at least the bytes from our test files
	expectedMinBytes := uint64(len("hello world") + len("test data"))
	if usage.Bytes < expectedMinBytes {
		t.Errorf("getDirectoryUsage() bytes = %d, want at least %d", usage.Bytes, expectedMinBytes)
	}

	// Should count files and directories
	expectedMinInodes := uint64(3) // 2 files + 1 directory
	if usage.Inodes < expectedMinInodes {
		t.Errorf("getDirectoryUsage() inodes = %d, want at least %d", usage.Inodes, expectedMinInodes)
	}
}

func TestGetDirectoryUsageNonExistent(t *testing.T) {
	c := newTestCRIService()

	_, err := c.getDirectoryUsage("/non/existent/path")
	if err == nil {
		t.Error("getDirectoryUsage() should return error for non-existent path")
	}
}

func TestExtractCPUMetrics(t *testing.T) {
	c := newTestCRIService()

	labels := []string{"container1", "pod1", "namespace1", "id1"}
	timestamp := time.Now().UnixNano()

	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
	}{
		{
			name: "cgroup v1 CPU stats",
			input: &cg1.Metrics{
				CPU: &cg1.CPUStat{
					Usage: &cg1.CPUUsage{
						Total:  1000000000, // 1 second
						User:   600000000,
						Kernel: 400000000,
					},
					Throttling: &cg1.Throttle{
						Periods:          100,
						ThrottledPeriods: 10,
						ThrottledTime:    50000000,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "cgroup v2 CPU stats",
			input: &cg2.Metrics{
				CPU: &cg2.CPUStat{
					UsageUsec:     1000000, // 1 second in microseconds
					UserUsec:      600000,
					SystemUsec:    400000,
					NrPeriods:     100,
					NrThrottled:   10,
					ThrottledUsec: 50000,
				},
			},
			wantErr: false,
		},
		{
			name:    "invalid type",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics, err := c.extractCPUMetrics(tt.input, labels, timestamp)

			if (err != nil) != tt.wantErr {
				t.Errorf("extractCPUMetrics() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if len(metrics) == 0 {
					t.Error("extractCPUMetrics() returned empty metrics slice")
				}

				// Verify that all metrics have proper structure
				for _, metric := range metrics {
					if metric.Name == "" {
						t.Error("Metric name should not be empty")
					}
					if metric.Timestamp != timestamp {
						t.Error("Metric timestamp should match input")
					}
					if len(metric.LabelValues) != len(labels) {
						t.Errorf("Metric labels length = %d, want %d", len(metric.LabelValues), len(labels))
					}
				}
			}
		})
	}
}

func TestGenerateContainerCPUMetrics(t *testing.T) {
	input := &containerCPUMetrics{
		UsageUsec:          1000000,
		UserUsec:           600000,
		SystemUsec:         400000,
		NRPeriods:          100,
		NRThrottledPeriods: 10,
		ThrottledUsec:      50000,
	}

	metrics := generateContainerCPUMetrics(input)

	if len(metrics) == 0 {
		t.Error("generateContainerCPUMetrics() returned empty metrics slice")
	}

	// Check that we have expected metric names
	expectedMetrics := map[string]bool{
		"container_cpu_usage_seconds_total":         false,
		"container_cpu_user_seconds_total":          false,
		"container_cpu_system_seconds_total":        false,
		"container_cpu_cfs_periods_total":           false,
		"container_cpu_cfs_throttled_periods_total": false,
		"container_cpu_cfs_throttled_seconds_total": false,
	}

	for _, metric := range metrics {
		if _, exists := expectedMetrics[metric.Name]; exists {
			expectedMetrics[metric.Name] = true
		}
	}

	// Verify all expected metrics were found
	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("Expected metric %s not found", name)
		}
	}
}

func TestGenerateContainerMemoryMetrics(t *testing.T) {
	input := &containerMemoryMetrics{
		Cache:        1024000,
		RSS:          2048000,
		Swap:         32000,
		KernelUsage:  64000,
		FileMapped:   512000,
		FailCount:    5,
		MemoryUsage:  4096000,
		MaxUsage:     8192000,
		WorkingSet:   3968000,
		ActiveFile:   256000,
		InactiveFile: 128000,
	}

	metrics := generateContainerMemoryMetrics(input)

	if len(metrics) == 0 {
		t.Error("generateContainerMemoryMetrics() returned empty metrics slice")
	}

	// Check that we have expected metric names
	expectedMetrics := map[string]bool{
		"container_memory_cache":                     false,
		"container_memory_rss":                       false,
		"container_memory_swap":                      false,
		"container_memory_kernel_usage":              false,
		"container_memory_mapped_file":               false,
		"container_memory_failcnt":                   false,
		"container_memory_usage_bytes":               false,
		"container_memory_max_usage_bytes":           false,
		"container_memory_working_set_bytes":         false,
		"container_memory_total_active_file_bytes":   false,
		"container_memory_total_inactive_file_bytes": false,
	}

	for _, metric := range metrics {
		if _, exists := expectedMetrics[metric.Name]; exists {
			expectedMetrics[metric.Name] = true
		}
	}

	// Verify all expected metrics were found
	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("Expected metric %s not found", name)
		}
	}
}

func TestListPodSandboxMetrics(t *testing.T) {
	// Note: This would require a more complex setup with mock sandbox store
	// For now, we test basic structure
	c := newTestCRIService()

	req := &runtime.ListPodSandboxMetricsRequest{}
	resp, err := c.ListPodSandboxMetrics(context.Background(), req)

	if err != nil {
		t.Errorf("ListPodSandboxMetrics() error = %v", err)
		return
	}

	if resp == nil {
		t.Error("ListPodSandboxMetrics() returned nil response")
		return
	}

	// Should return empty metrics for test environment
	if resp.PodMetrics == nil {
		t.Error("ListPodSandboxMetrics() returned nil PodMetrics")
	}
}
