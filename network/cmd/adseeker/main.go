// Command adseeker runs the network's ad-break detection from the
// command line (the same code path as ingestion):
//
//	go run ./cmd/adseeker <video> [--min-duration <seconds>]
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"rewave/network/internal/media"
)

func main() {
	video, minDuration, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "adseeker:", err)
		fmt.Fprintln(os.Stderr, "usage: adseeker <video> [--min-duration <seconds>]")
		os.Exit(1)
	}

	detector := media.FFmpegDetector{}
	breaks, err := detector.DetectAdBreaks(context.Background(), video, minDuration)
	if err != nil {
		fmt.Fprintln(os.Stderr, "adseeker:", err)
		os.Exit(1)
	}
	if len(breaks) > 0 {
		fmt.Println("\nRecommended ad breaks:")
		for _, position := range breaks {
			fmt.Printf(" -> %.2fs\n", position)
		}
	}
}

// parseArgs mirrors the former TypeScript CLI: the video is a positional
// argument, --min-duration may appear before or after it.
func parseArgs(args []string) (video string, minDuration float64, err error) {
	minDuration = 3600
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--min-duration" {
			if i+1 >= len(args) {
				return "", 0, fmt.Errorf("--min-duration requires a value")
			}
			parsed, parseErr := strconv.ParseFloat(args[i+1], 64)
			if parseErr != nil || parsed < 0 {
				return "", 0, fmt.Errorf("invalid --min-duration value %q", args[i+1])
			}
			minDuration = parsed
			i++
			continue
		}
		if video == "" {
			video = arg
			continue
		}
		return "", 0, fmt.Errorf("unexpected argument %q", arg)
	}
	if video == "" {
		return "", 0, fmt.Errorf("a video path is required")
	}
	return video, minDuration, nil
}
