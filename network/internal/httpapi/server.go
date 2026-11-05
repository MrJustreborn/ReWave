package httpapi

import (
	"net/http"

	"rewave/network/internal/apperr"
	"rewave/network/internal/channel"
	"rewave/network/internal/httpapi/gen"
)

// Server implements the generated OpenAPI ServerInterface. Operations
// whose use case is not implemented yet answer 501 via the error
// mapper; implementing an operation means filling in the method below.
type Server struct {
	channels *channel.Service
}

// New creates the API server.
func New(channels *channel.Service) *Server {
	return &Server{channels: channels}
}

var _ gen.ServerInterface = (*Server)(nil)

func notImplemented(w http.ResponseWriter, operation string) {
	WriteError(w, apperr.NotImplemented(operation+" is not implemented yet"))
}

// ListChannels (GET /v1/channels).
func (s *Server) ListChannels(w http.ResponseWriter, r *http.Request) {
	channels, err := s.channels.List(r.Context())
	if err != nil {
		WriteError(w, err)
		return
	}
	dtos := make([]gen.Channel, len(channels))
	for i, ch := range channels {
		dto, err := channelToAPI(ch)
		if err != nil {
			WriteError(w, err)
			return
		}
		dtos[i] = dto
	}
	WriteJSON(w, http.StatusOK, gen.ChannelList{Channels: dtos})
}

// CreateChannel (POST /v1/channels).
func (s *Server) CreateChannel(w http.ResponseWriter, r *http.Request) {
	var dto gen.ChannelDefinition
	if err := decodeJSON(r, &dto); err != nil {
		WriteError(w, err)
		return
	}
	def, err := definitionFromAPI(dto)
	if err != nil {
		WriteError(w, err)
		return
	}
	created, err := s.channels.Create(r.Context(), def)
	if err != nil {
		WriteError(w, err)
		return
	}
	body, err := channelToAPI(created)
	if err != nil {
		WriteError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/channels/"+created.ID)
	WriteJSON(w, http.StatusCreated, body)
}

// DeleteChannel (DELETE /v1/channels/{channelId}).
func (s *Server) DeleteChannel(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	if err := s.channels.Delete(r.Context(), channelId.String()); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetChannel (GET /v1/channels/{channelId}).
func (s *Server) GetChannel(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	ch, err := s.channels.Get(r.Context(), channelId.String())
	if err != nil {
		WriteError(w, err)
		return
	}
	dto, err := channelToAPI(ch)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, dto)
}

// ReplaceChannel (PUT /v1/channels/{channelId}).
func (s *Server) ReplaceChannel(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	var dto gen.ChannelDefinition
	if err := decodeJSON(r, &dto); err != nil {
		WriteError(w, err)
		return
	}
	def, err := definitionFromAPI(dto)
	if err != nil {
		WriteError(w, err)
		return
	}
	updated, err := s.channels.Replace(r.Context(), channelId.String(), def)
	if err != nil {
		WriteError(w, err)
		return
	}
	body, err := channelToAPI(updated)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, body)
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
