package httpapi

import (
	"net/http"

	"rewave/network/internal/apperr"
	"rewave/network/internal/httpapi/gen"
)

// Server implements the generated OpenAPI ServerInterface. Until an
// operation's use case is implemented, it answers 501 via the error
// mapper; implementing an operation means filling in the method below.
type Server struct{}

// New creates the API server. Service dependencies will be added as the
// use cases are implemented.
func New() *Server {
	return &Server{}
}

var _ gen.ServerInterface = (*Server)(nil)

func notImplemented(w http.ResponseWriter, operation string) {
	WriteError(w, apperr.NotImplemented(operation+" is not implemented yet"))
}

// ListChannels (GET /v1/channels).
func (s *Server) ListChannels(w http.ResponseWriter, _ *http.Request) {
	notImplemented(w, "listChannels")
}

// CreateChannel (POST /v1/channels).
func (s *Server) CreateChannel(w http.ResponseWriter, _ *http.Request) {
	notImplemented(w, "createChannel")
}

// DeleteChannel (DELETE /v1/channels/{channelId}).
func (s *Server) DeleteChannel(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "deleteChannel")
}

// GetChannel (GET /v1/channels/{channelId}).
func (s *Server) GetChannel(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "getChannel")
}

// ReplaceChannel (PUT /v1/channels/{channelId}).
func (s *Server) ReplaceChannel(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "replaceChannel")
}

// GetChannelPlaylist (GET /v1/channels/{channelId}/playlist).
func (s *Server) GetChannelPlaylist(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "getChannelPlaylist")
}

// StartChannel (POST /v1/channels/{channelId}/start).
func (s *Server) StartChannel(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "startChannel")
}

// StopChannel (POST /v1/channels/{channelId}/stop).
func (s *Server) StopChannel(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "stopChannel")
}

// WatchChannel (POST /v1/channels/{channelId}/watch).
func (s *Server) WatchChannel(w http.ResponseWriter, _ *http.Request, _ gen.ChannelId) {
	notImplemented(w, "watchChannel")
}

// ListVideos (GET /v1/videos).
func (s *Server) ListVideos(w http.ResponseWriter, _ *http.Request, _ gen.ListVideosParams) {
	notImplemented(w, "listVideos")
}

// AddVideo (POST /v1/videos).
func (s *Server) AddVideo(w http.ResponseWriter, _ *http.Request) {
	notImplemented(w, "addVideo")
}

// DeleteVideo (DELETE /v1/videos/{videoId}).
func (s *Server) DeleteVideo(w http.ResponseWriter, _ *http.Request, _ gen.VideoId) {
	notImplemented(w, "deleteVideo")
}

// GetVideo (GET /v1/videos/{videoId}).
func (s *Server) GetVideo(w http.ResponseWriter, _ *http.Request, _ gen.VideoId) {
	notImplemented(w, "getVideo")
}
