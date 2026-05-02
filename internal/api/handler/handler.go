package handler

import (
	"encoding/json"
	"net/http"

	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// Handlers holds all HTTP handler dependencies.
// Constructed once in main.go and shared across all handler functions.
type Handlers struct {
	store  *store.Store
	logger *zap.Logger
}

// New creates a Handlers instance with its dependencies injected.
func New(s *store.Store, logger *zap.Logger) *Handlers {
	return &Handlers{store: s, logger: logger}
}

// respondJSON writes a JSON response with the given status code.
func respondJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// respondError writes an RFC 7807-style Problem Details error response.
func respondError(w http.ResponseWriter, status int, title, detail string) {
	respondJSON(w, status, map[string]any{
		"type":   "https://etchflow.dev/errors/" + slugify(title),
		"title":  title,
		"status": status,
		"detail": detail,
	})
}

// slugify converts a title to a URL-safe slug for the error type field.
func slugify(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result = append(result, c+32) // lowercase
		} else if c == ' ' {
			result = append(result, '-')
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			result = append(result, c)
		}
	}
	return string(result)
}
