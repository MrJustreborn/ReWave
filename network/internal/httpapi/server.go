package httpapi

import (
	"net/http"

	"rewave/network/internal/apperr"
	"rewave/network/internal/channel"
	"rewave/network/internal/httpapi/gen"
	"rewave/network/internal/media"
	"rewave/network/internal/scheduling"
	"rewave/network/internal/watching"
)

// Server implements the generated OpenAPI ServerInterface.
type Server struct {
	channels   *channel.Service
	videos     *media.Service
	scheduling *scheduling.Service
	watching   *watching.Service
}

// New creates the API server.
func New(channels *channel.Service, videos *media.Service, scheduling *scheduling.Service, watching *watching.Service) *Server {
	return &Server{channels: channels, videos: videos, scheduling: scheduling, watching: watching}
}

var _ gen.ServerInterface = (*Server)(nil)

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
func (s *Server) GetChannelPlaylist(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	playlist, err := s.scheduling.Stored(r.Context(), channelId.String())
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, playlistToAPI(playlist))
}

// StartChannel (POST /v1/channels/{channelId}/start).
func (s *Server) StartChannel(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	ch, err := s.channels.Start(r.Context(), channelId.String())
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

// StopChannel (POST /v1/channels/{channelId}/stop).
func (s *Server) StopChannel(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	ch, err := s.channels.Stop(r.Context(), channelId.String())
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

// WatchChannel (POST /v1/channels/{channelId}/watch).
func (s *Server) WatchChannel(w http.ResponseWriter, r *http.Request, channelId gen.ChannelId) {
	var request gen.WatchRequest
	if err := decodeJSON(r, &request); err != nil {
		WriteError(w, err)
		return
	}
	if request.ViewerId == "" {
		WriteError(w, apperr.Validation("viewerId must be a non-empty string"))
		return
	}
	result, err := s.watching.Watch(r.Context(), channelId.String(), request.ViewerId)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, watchResultToAPI(result))
}

// ListVideos (GET /v1/videos).
func (s *Server) ListVideos(w http.ResponseWriter, r *http.Request, params gen.ListVideosParams) {
	var filter *media.Filter
	if params.Processed != nil {
		filter = &media.Filter{Processed: *params.Processed}
	}
	videos, err := s.videos.List(r.Context(), filter)
	if err != nil {
		WriteError(w, err)
		return
	}
	dtos := make([]gen.Video, len(videos))
	for i, v := range videos {
		dto, err := videoToAPI(v)
		if err != nil {
			WriteError(w, err)
			return
		}
		dtos[i] = dto
	}
	WriteJSON(w, http.StatusOK, gen.VideoList{Videos: dtos})
}

// AddVideo (POST /v1/videos).
func (s *Server) AddVideo(w http.ResponseWriter, r *http.Request) {
	var dto gen.NewVideo
	if err := decodeJSON(r, &dto); err != nil {
		WriteError(w, err)
		return
	}
	added, err := s.videos.Add(r.Context(), addVideoCommandFromAPI(dto))
	if err != nil {
		WriteError(w, err)
		return
	}
	body, err := videoToAPI(added)
	if err != nil {
		WriteError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/videos/"+added.ID)
	WriteJSON(w, http.StatusCreated, body)
}

// DeleteVideo (DELETE /v1/videos/{videoId}).
func (s *Server) DeleteVideo(w http.ResponseWriter, r *http.Request, videoId gen.VideoId) {
	if err := s.videos.Delete(r.Context(), videoId.String()); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetVideo (GET /v1/videos/{videoId}).
func (s *Server) GetVideo(w http.ResponseWriter, r *http.Request, videoId gen.VideoId) {
	v, err := s.videos.Get(r.Context(), videoId.String())
	if err != nil {
		WriteError(w, err)
		return
	}
	dto, err := videoToAPI(v)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, dto)
}
