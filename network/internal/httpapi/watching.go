package httpapi

import (
	"rewave/network/internal/httpapi/gen"
	"rewave/network/internal/watching"
)

// watchResultToAPI converts a watch result into the WatchResponse DTO.
func watchResultToAPI(result watching.Result) gen.WatchResponse {
	response := gen.WatchResponse{
		Viewers: result.Viewers,
		Status:  gen.ChannelStatus(result.Status),
	}
	if result.KeepAliveUntil != nil {
		until := result.KeepAliveUntil.UTC()
		response.KeepAliveUntil = &until
	}
	return response
}
