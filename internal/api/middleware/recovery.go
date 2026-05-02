package middleware

import (
	"encoding/json"
	"net/http"
	"runtime/debug"

	"go.uber.org/zap"
)

// Recovery returns a middleware that catches panics in handlers,
// logs the stack trace, and returns a 500 JSON error response.
// Without this, a panic in any handler kills the whole server.
func Recovery(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic recovered",
						zap.Any("panic", rec),
						zap.String("stack", string(debug.Stack())),
					)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"type":   "https://etchflow.dev/errors/internal-error",
						"title":  "Internal Server Error",
						"status": 500,
						"detail": "An unexpected error occurred",
					})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
