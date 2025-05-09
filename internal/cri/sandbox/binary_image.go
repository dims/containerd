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

package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/log"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// SandboxImageName is the base name of the generated sandbox image
	SandboxImageName = "containerd.io/sandbox"

	// SandboxImageFormat defines the format for the sandbox image name
	// This ensures consistent naming between creation and lookup
	SandboxImageFormat = "%s:%s-%d" // format: name:timestamp-size
)

// FormatTimestamp converts a time.Time to a consistent string format for image names
// Format: lowercase RFC3339 with colons replaced by hyphens
func FormatTimestamp(t time.Time) string {
	return strings.ToLower(strings.ReplaceAll(t.UTC().Format(time.RFC3339), ":", "-"))
}

// CreateSandboxImageFromBinary creates a container image from a binary file
// The binary will be used as the entrypoint for the container
func CreateSandboxImageFromBinary(ctx context.Context, cs content.Store, binaryPath string) (images.Image, error) {
	log.G(ctx).Infof("Creating sandbox image from binary: %s", binaryPath)

	// Check if the binary has changed since the last time we created the image
	binaryInfo, err := os.Stat(binaryPath)
	if err != nil {
		return images.Image{}, fmt.Errorf("failed to stat sandbox binary %q: %w", binaryPath, err)
	}

	// Use the binary's modification time and size as part of the image name
	// This ensures we recreate the image if the binary changes
	modTime := FormatTimestamp(binaryInfo.ModTime())
	size := binaryInfo.Size()
	imageName := fmt.Sprintf(SandboxImageFormat, SandboxImageName, modTime, size)

	// Create a simple OCI image with the binary as the entrypoint
	now := time.Now()
	config := ocispec.Image{
		Platform: ocispec.Platform{
			OS:           platforms.DefaultSpec().OS,
			Architecture: platforms.DefaultSpec().Architecture,
		},
		Config: ocispec.ImageConfig{
			Entrypoint: []string{"/pause"},
			Env:        []string{"PATH=/"},
		},
		RootFS: ocispec.RootFS{
			Type:    "layers",
			DiffIDs: []digest.Digest{},
		},
		Created: &now,
	}

	// Create the layer containing the binary
	layerDesc, err := createBinaryLayer(ctx, cs, binaryPath)
	if err != nil {
		return images.Image{}, fmt.Errorf("failed to create binary layer: %w", err)
	}

	// Add the layer to the config
	config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, layerDesc.Digest)

	// Create the config descriptor
	configDesc, err := writeConfig(ctx, cs, config)
	if err != nil {
		return images.Image{}, fmt.Errorf("failed to write config: %w", err)
	}

	// Create the manifest
	manifestDesc, err := writeManifest(ctx, cs, configDesc, []ocispec.Descriptor{layerDesc})
	if err != nil {
		return images.Image{}, fmt.Errorf("failed to write manifest: %w", err)
	}

	// Create the image
	image := images.Image{
		Name:   imageName,
		Target: manifestDesc,
	}

	return image, nil
}

func createBinaryLayer(ctx context.Context, cs content.Store, binaryPath string) (ocispec.Descriptor, error) {
	// Create a simple tar layer with the binary
	binaryName := filepath.Base(binaryPath)

	// Create a temporary file for the tar content
	tmpFile, err := os.CreateTemp("", "sandbox-layer-*.tar")
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Execute tar command to create a tar with the binary
	tarCmd := fmt.Sprintf("tar -cf %s -C %s %s", tmpFile.Name(), filepath.Dir(binaryPath), binaryName)
	if err := exec.Command("sh", "-c", tarCmd).Run(); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to create tar: %w", err)
	}

	// Read the tar file
	tmpFile.Seek(0, 0)
	tarContent, err := io.ReadAll(tmpFile)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to read tar content: %w", err)
	}

	// Calculate the digest
	dgst := digest.FromBytes(tarContent)

	// Write the layer to the content store
	ref := fmt.Sprintf("sandbox-layer-%s", dgst.Encoded())
	if err := content.WriteBlob(ctx, cs, ref, bytes.NewReader(tarContent), ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayer,
		Digest:    dgst,
		Size:      int64(len(tarContent)),
	}); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to write layer blob: %w", err)
	}

	return ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayer,
		Digest:    dgst,
		Size:      int64(len(tarContent)),
	}, nil
}

func writeConfig(ctx context.Context, cs content.Store, config ocispec.Image) (ocispec.Descriptor, error) {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to marshal image config: %w", err)
	}

	dgst := digest.FromBytes(configJSON)
	ref := fmt.Sprintf("sandbox-config-%s", dgst.Encoded())

	if err := content.WriteBlob(ctx, cs, ref, bytes.NewReader(configJSON), ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    dgst,
		Size:      int64(len(configJSON)),
	}); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to write config blob: %w", err)
	}

	return ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    dgst,
		Size:      int64(len(configJSON)),
	}, nil
}

func writeManifest(ctx context.Context, cs content.Store, configDesc ocispec.Descriptor, layers []ocispec.Descriptor) (ocispec.Descriptor, error) {
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		Config: configDesc,
		Layers: layers,
	}

	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	dgst := digest.FromBytes(manifestJSON)
	ref := fmt.Sprintf("sandbox-manifest-%s", dgst.Encoded())

	if err := content.WriteBlob(ctx, cs, ref, bytes.NewReader(manifestJSON), ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    dgst,
		Size:      int64(len(manifestJSON)),
	}); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to write manifest blob: %w", err)
	}

	return ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    dgst,
		Size:      int64(len(manifestJSON)),
	}, nil
}
