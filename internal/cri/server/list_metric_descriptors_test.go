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
	"strings"
	"testing"

	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestListMetricDescriptors(t *testing.T) {
	c := newTestCRIService()

	req := &runtime.ListMetricDescriptorsRequest{}
	resp, err := c.ListMetricDescriptors(context.Background(), req)
	if err != nil {
		t.Fatalf("ListMetricDescriptors failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Response should not be nil")
	}

	if len(resp.Descriptors) == 0 {
		t.Fatal("Should return at least one metric descriptor")
	}

	// Verify some expected metrics are present
	expectedMetrics := map[string]bool{
		"container_cpu_usage_seconds_total":     false,
		"container_memory_usage_bytes":          false,
		"container_network_receive_bytes_total": false,
		"container_processes":                   false,
		"container_fs_usage_bytes":              false,
		"container_memory_cache":                false,
	}

	for _, descriptor := range resp.Descriptors {
		if _, exists := expectedMetrics[descriptor.Name]; exists {
			expectedMetrics[descriptor.Name] = true
		}

		// Verify descriptor has required fields
		if descriptor.Name == "" {
			t.Errorf("Descriptor name should not be empty")
		}
		if descriptor.Help == "" {
			t.Errorf("Descriptor help should not be empty for metric %s", descriptor.Name)
		}
		if len(descriptor.LabelKeys) == 0 {
			t.Errorf("Descriptor should have label keys for metric %s", descriptor.Name)
		}
	}

	// Check that all expected metrics were found
	for metric, found := range expectedMetrics {
		if !found {
			t.Errorf("Expected metric %s not found in descriptors", metric)
		}
	}

	t.Logf("Successfully returned %d metric descriptors", len(resp.Descriptors))

	// Test that metrics are organized by categories
	categories := map[string]int{
		CpuUsageMetrics:     0,
		MemoryUsageMetrics:  0,
		NetworkUsageMetrics: 0,
		ProcessMetrics:      0,
		DiskUsageMetrics:    0,
		DiskIOMetrics:       0,
	}

	// Count metrics by checking their names
	for _, descriptor := range resp.Descriptors {
		switch {
		case containsString(descriptor.Name, "cpu"):
			categories[CpuUsageMetrics]++
		case containsString(descriptor.Name, "memory"):
			categories[MemoryUsageMetrics]++
		case containsString(descriptor.Name, "network"):
			categories[NetworkUsageMetrics]++
		case containsString(descriptor.Name, "processes") || containsString(descriptor.Name, "threads"):
			categories[ProcessMetrics]++
		case containsString(descriptor.Name, "fs_usage") || containsString(descriptor.Name, "fs_limit"):
			categories[DiskUsageMetrics]++
		case containsString(descriptor.Name, "fs_reads") || containsString(descriptor.Name, "fs_writes"):
			categories[DiskIOMetrics]++
		}
	}

	// Verify we have metrics in each major category
	for category, count := range categories {
		if count == 0 && category != "misc" {
			t.Logf("Warning: No metrics found for category %s", category)
		} else {
			t.Logf("Category %s has %d metrics", category, count)
		}
	}
}

func TestListMetricDescriptorsError(t *testing.T) {
	c := newTestCRIService()

	// Test with nil request
	resp, err := c.ListMetricDescriptors(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListMetricDescriptors with nil request failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Response should not be nil")
	}
}

func TestGetMetricDescriptors(t *testing.T) {
	c := newTestCRIService()

	descriptors := c.getMetricDescriptors()

	if descriptors == nil {
		t.Fatal("getMetricDescriptors() returned nil")
	}

	// Check that we have all expected categories
	expectedCategories := []string{
		CpuUsageMetrics,
		MemoryUsageMetrics,
		NetworkUsageMetrics,
		ProcessMetrics,
		DiskUsageMetrics,
		DiskIOMetrics,
		OOMMetrics,
		"misc",
	}

	for _, category := range expectedCategories {
		if _, exists := descriptors[category]; !exists {
			t.Errorf("Expected category %s not found in descriptors", category)
		}
	}

	// Verify each category has descriptors
	for category, descs := range descriptors {
		if len(descs) == 0 {
			t.Errorf("Category %s has no descriptors", category)
		}

		// Verify each descriptor has required fields
		for _, desc := range descs {
			if desc.Name == "" {
				t.Errorf("Descriptor in category %s has empty name", category)
			}
			if desc.Help == "" {
				t.Errorf("Descriptor %s in category %s has empty help", desc.Name, category)
			}
			// LabelKeys can be empty for some metrics
		}
	}
}

func TestMetricDescriptorConstants(t *testing.T) {
	// Test that all metric category constants are properly defined
	expectedConstants := map[string]string{
		"CpuUsageMetrics":     CpuUsageMetrics,
		"MemoryUsageMetrics":  MemoryUsageMetrics,
		"CpuLoadMetrics":      CpuLoadMetrics,
		"DiskIOMetrics":       DiskIOMetrics,
		"DiskUsageMetrics":    DiskUsageMetrics,
		"NetworkUsageMetrics": NetworkUsageMetrics,
		"ProcessMetrics":      ProcessMetrics,
		"OOMMetrics":          OOMMetrics,
	}

	for constantName, constantValue := range expectedConstants {
		if constantValue == "" {
			t.Errorf("Constant %s is empty", constantName)
		}
	}

	// Test base label keys
	if len(baseLabelKeys) == 0 {
		t.Error("baseLabelKeys should not be empty")
	}

	expectedBaseLabelKeys := []string{"id", "name"}
	if len(baseLabelKeys) != len(expectedBaseLabelKeys) {
		t.Errorf("baseLabelKeys length = %d, want %d", len(baseLabelKeys), len(expectedBaseLabelKeys))
	}

	for i, expected := range expectedBaseLabelKeys {
		if i >= len(baseLabelKeys) || baseLabelKeys[i] != expected {
			t.Errorf("baseLabelKeys[%d] = %s, want %s", i, baseLabelKeys[i], expected)
		}
	}
}

func TestContainsStringEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected bool
	}{
		{
			name:     "empty string and substring",
			s:        "",
			substr:   "",
			expected: true,
		},
		{
			name:     "empty substring",
			s:        "test",
			substr:   "",
			expected: true,
		},
		{
			name:     "empty string",
			s:        "",
			substr:   "test",
			expected: false,
		},
		{
			name:     "exact match",
			s:        "test",
			substr:   "test",
			expected: true,
		},
		{
			name:     "substring at beginning",
			s:        "testing",
			substr:   "test",
			expected: true,
		},
		{
			name:     "substring at end",
			s:        "unittest",
			substr:   "test",
			expected: true,
		},
		{
			name:     "substring in middle",
			s:        "container_test_metric",
			substr:   "test",
			expected: true,
		},
		{
			name:     "not found",
			s:        "container_cpu_metric",
			substr:   "memory",
			expected: false,
		},
		{
			name:     "case sensitive",
			s:        "Container",
			substr:   "container",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsString(tt.s, tt.substr)
			if result != tt.expected {
				t.Errorf("containsString(%q, %q) = %v, want %v", tt.s, tt.substr, result, tt.expected)
			}
		})
	}
}

func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}
