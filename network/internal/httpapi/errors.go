package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"rewave/network/internal/apperr"
)

// WriteJSON writes v as a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError maps err to an HTTP response following the openapi Error
// schema: {code, message}. Application errors keep their message; any
// other error becomes a 500 with a generic body (details are logged).
func WriteError(w http.ResponseWriter, err error) {
	status := apperr.HTTPStatus(err)
	message := apperr.Message(err)
	if status == 500 {
		slog.Error("unhandled error while serving request", "error", err.Error())
	}
	WriteJSON(w, status, struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: status, Message: message})
}
