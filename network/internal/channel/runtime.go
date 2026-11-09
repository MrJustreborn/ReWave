package channel

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

// Runtime is the channel lifecycle port: starting and stopping the
// channel containers. The Docker implementation lives in this package;
// tests substitute fakes.
type Runtime interface {
	// EnsureRunning creates and starts the channel container with the
	// given playlist (idempotent — an existing container is replaced so
	// the fresh playlist takes effect).
	EnsureRunning(ctx context.Context, ch Channel, playlistEnv string) error
	// Stop stops and removes the channel container (idempotent).
	Stop(ctx context.Context, ch Channel) error
}

// DockerRuntimeConfig configures the container runtime.
type DockerRuntimeConfig struct {
	// DockerHost is the daemon address, e.g. unix:///var/run/docker.sock.
	DockerHost string
	// Image is the channel image channels are created from.
	Image string
	// MediaBind is the media-pool bind mount, "hostPath:containerPath:ro".
	MediaBind string
}

// DockerRuntime manages one container per channel through the Docker
// API. Channels are stateless, replaceable players: starting a channel
// recreates its container (fresh playlist), stopping removes it.
type DockerRuntime struct {
	config DockerRuntimeConfig
	client *client.Client
}

// NewDockerRuntime connects to the Docker daemon.
func NewDockerRuntime(config DockerRuntimeConfig) (*DockerRuntime, error) {
	cli, err := client.NewClientWithOpts(
		client.WithHost(config.DockerHost),
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &DockerRuntime{config: config, client: cli}, nil
}

// EnsureRunning replaces and starts the channel's container.
func (r *DockerRuntime) EnsureRunning(ctx context.Context, ch Channel, playlistEnv string) error {
	name := ContainerName(ch)

	// Replace any existing container: the playlist is baked into the
	// environment, so a fresh playlist needs a fresh container.
	if err := r.client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove existing channel container %s: %w", name, err)
	}

	_, err := r.client.ContainerCreate(ctx,
		&container.Config{
			Image: r.config.Image,
			Env:   []string{"PLAYLIST=" + playlistEnv},
			Labels: map[string]string{
				"rewave.channel.id":   ch.ID,
				"rewave.channel.name": ch.Definition.Name,
			},
		},
		&container.HostConfig{
			// Host networking is required for UDP multicast (Linux).
			NetworkMode: "host",
			Binds:       []string{r.config.MediaBind},
		},
		nil, nil, name)
	if err != nil {
		return fmt.Errorf("create channel container %s: %w", name, err)
	}
	if err := r.client.ContainerStart(ctx, name, container.StartOptions{}); err != nil {
		// The container exists but failed to start — remove it so a
		// retry starts clean.
		_ = r.client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
		return fmt.Errorf("start channel container %s: %w", name, err)
	}
	return nil
}

// Stop stops and removes the channel's container.
func (r *DockerRuntime) Stop(ctx context.Context, ch Channel) error {
	name := ContainerName(ch)
	if err := r.client.ContainerStop(ctx, name, container.StopOptions{}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("stop channel container %s: %w", name, err)
	}
	if err := r.client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove channel container %s: %w", name, err)
	}
	return nil
}

// ContainerName derives the container name from the channel: the
// sanitized channel name plus the id prefix (the sanitized name alone
// could collide after sanitization).
func ContainerName(ch Channel) string {
	return "rewave-channel-" + sanitize(ch.Definition.Name) + "-" + ch.ID[:min(8, len(ch.ID))]
}

// sanitize maps a channel name to a Docker-name-safe form.
func sanitize(name string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(name) {
		isAllowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAllowed {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	result := strings.Trim(b.String(), "-")
	if result == "" {
		return "channel"
	}
	return result
}
