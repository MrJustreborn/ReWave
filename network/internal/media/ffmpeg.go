package media

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os/exec"
	"regexp"
	"strconv"
)

// Detector is the ingestion port: probing a video's duration and
// detecting ad-break positions. FFmpegDetector is the real
// implementation (ffprobe/ffmpeg subprocesses); tests use fakes.
type Detector interface {
	// ProbeDuration returns the video duration in seconds, as reported
	// by ffprobe.
	ProbeDuration(ctx context.Context, path string) (float64, error)
	// DetectAdBreaks runs scene-change based ad-break detection and
	// returns the recommended break positions in seconds.
	DetectAdBreaks(ctx context.Context, path string, minDuration float64) ([]float64, error)
}

// FFmpegDetector implements Detector via ffprobe/ffmpeg subprocesses.
type FFmpegDetector struct{}

// ProbeDuration runs ffprobe and parses the reported duration.
func (FFmpegDetector) ProbeDuration(ctx context.Context, path string) (float64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path)
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe %s failed: %w", path, err)
	}
	duration, parseErr := strconv.ParseFloat(trimFirstLine(output), 64)
	if parseErr != nil {
		return 0, fmt.Errorf("could not parse duration from ffprobe output %q", string(output))
	}
	return duration, nil
}

// Ad-break detection defaults, identical to the former TypeScript
// prototype: scene threshold 0.4, a 60 s search window around each
// target position, videos under 60 min are skipped, target positions at
// 30/50/70 % of the duration.
const (
	defaultThreshold         = 0.4
	defaultWindow            = 60.0
	defaultMinDuration       = 3600.0
	defaultTargetCenterRatio = 0.5
)

var defaultTargetPositions = [3]float64{0.3, 0.5, 0.7}

// DetectAdBreaks implements the ported algorithm: probe the duration,
// skip short videos, and for each target position find the scene cut
// closest to the window center.
func (d FFmpegDetector) DetectAdBreaks(ctx context.Context, path string, minDuration float64) ([]float64, error) {
	if minDuration <= 0 {
		minDuration = defaultMinDuration
	}
	duration, err := d.ProbeDuration(ctx, path)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Duration: %.1f min\n", duration/60)
	if duration < minDuration {
		fmt.Printf("Shorter than %.0f min -> no ads.\n", minDuration/60)
		return []float64{}, nil
	}

	var candidates []float64
	for _, position := range defaultTargetPositions {
		center := duration * position
		start := math.Max(center-defaultWindow, 0)
		cuts, err := d.findSceneChanges(ctx, path, start, defaultWindow*2, defaultThreshold)
		if err != nil {
			return nil, err
		}
		if len(cuts) == 0 {
			fmt.Printf("Range %.0f%%: no cuts found.\n", position*100)
			continue
		}
		best := nearestTo(cuts, center)
		candidates = append(candidates, math.Round(best*100)/100)
		fmt.Printf("Range %.0f%% -> %d cuts, best spot: %.2fs\n", position*100, len(cuts), best)
	}
	return candidates, nil
}

// findSceneChanges runs ffmpeg's scene-change detection over a window
// and returns the absolute timestamps of the detected cuts.
func (d FFmpegDetector) findSceneChanges(ctx context.Context, path string, start, length, threshold float64) ([]float64, error) {
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "info",
		"-ss", formatSeconds(start),
		"-t", formatSeconds(length),
		"-i", path,
		"-vf", fmt.Sprintf("select='gt(scene,%g)',showinfo", threshold),
		"-f", "null", "-")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg scene detection failed: %w: %s", err, truncateForLog(stderr.String()))
	}
	return parseSceneCuts(stderr.String(), start), nil
}

var ptsTimePattern = regexp.MustCompile(`pts_time:([0-9.]+)`)

// parseSceneCuts extracts scene-cut timestamps (relative to the sought
// start) from ffmpeg showinfo stderr output. Pure function — tested
// with canned ffmpeg output.
func parseSceneCuts(output string, start float64) []float64 {
	var times []float64
	for _, match := range ptsTimePattern.FindAllStringSubmatch(output, -1) {
		relative, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			continue
		}
		times = append(times, start+relative)
	}
	return times
}

// nearestTo returns the value closest to target.
func nearestTo(values []float64, target float64) float64 {
	best := values[0]
	for _, value := range values[1:] {
		if math.Abs(value-target) < math.Abs(best-target) {
			best = value
		}
	}
	return best
}

func formatSeconds(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func trimFirstLine(output []byte) string {
	text := string(output)
	for i, c := range text {
		if c == '\n' {
			return text[:i]
		}
	}
	return text
}

func truncateForLog(text string) string {
	const max = 500
	if len(text) > max {
		return text[:max] + "…"
	}
	return text
}
