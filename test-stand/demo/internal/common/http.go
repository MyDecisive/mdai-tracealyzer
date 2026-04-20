package common

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	ddtrace "gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type RequestMeta struct {
	RequestID string
	Scenario  string
	Method    string
	Route     string
}

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string {
	return e.Message
}

type HandlerFunc func(ctx context.Context, r *http.Request, meta RequestMeta) (any, error)

type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func RegisterJSONRoute(mux *http.ServeMux, service string, logger *Logger, method, path string, handler HandlerFunc) {
	base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = NewRequestID()
		}
		meta := RequestMeta{
			RequestID: requestID,
			Scenario:  r.URL.Query().Get("scenario"),
			Method:    method,
			Route:     path,
		}

		ctx := r.Context()
		AnnotateHTTPSpan(ctx, method, path, meta.RequestID, meta.Scenario)

		sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		sw.Header().Set("Content-Type", "application/json")
		sw.Header().Set("X-Request-ID", meta.RequestID)

		logger.Info(ctx, "request received", map[string]any{
			"event":      "request_received",
			"request_id": meta.RequestID,
			"scenario":   meta.Scenario,
			"route":      path,
			"transport":  "http",
		})

		payload, err := handler(ctx, r, meta)
		statusCode := sw.statusCode
		if err != nil {
			statusCode = http.StatusInternalServerError
			var httpErr *HTTPError
			if errors.As(err, &httpErr) {
				statusCode = httpErr.Status
			}
			sw.WriteHeader(statusCode)
			_ = json.NewEncoder(sw).Encode(map[string]any{
				"request_id": meta.RequestID,
				"scenario":   meta.Scenario,
				"success":    false,
				"message":    err.Error(),
			})
		} else {
			if statusCode == 0 {
				statusCode = http.StatusOK
			}
			_ = json.NewEncoder(sw).Encode(payload)
		}

		logger.Info(ctx, "request completed", map[string]any{
			"event":       "request_completed",
			"request_id":  meta.RequestID,
			"scenario":    meta.Scenario,
			"route":       path,
			"transport":   "http",
			"status_code": statusCode,
		})
	})

	resource := method + " " + path
	mux.Handle(path, httptrace.WrapHandler(
		base,
		service,
		resource,
		httptrace.WithSpanOptions(
			ddtracer.Tag(ext.SpanKind, ext.SpanKindServer),
			ddtracer.Tag(ext.SpanType, ext.SpanTypeWeb),
		),
	))
}

func NewTracedHTTPClient(service string) *http.Client {
	return httptrace.WrapClient(
		&http.Client{Timeout: 5 * time.Second},
		httptrace.RTWithServiceName(service),
		httptrace.RTWithResourceNamer(func(req *http.Request) string {
			return req.Method + " " + req.URL.Path
		}),
		httptrace.RTWithSpanOptions(
			ddtracer.Tag(ext.SpanKind, ext.SpanKindClient),
			ddtracer.Tag(ext.SpanType, ext.SpanTypeWeb),
		),
		httptrace.WithBefore(func(req *http.Request, span ddtrace.Span) {
			span.SetTag("http.route", req.URL.Path)
			span.SetTag("request.id", req.Header.Get("X-Request-ID"))
			if scenario := req.URL.Query().Get("scenario"); scenario != "" {
				span.SetTag("scenario", scenario)
			}
		}),
	)
}

func JSONRequest(ctx context.Context, client *http.Client, logger *Logger, method, target, operation string, meta RequestMeta, extraQuery map[string]string, requestBody any) (map[string]any, error) {
	parsedURL, err := url.Parse(target)
	if err != nil {
		return nil, err
	}

	query := parsedURL.Query()
	query.Set("scenario", meta.Scenario)
	for key, value := range extraQuery {
		query.Set(key, value)
	}
	parsedURL.RawQuery = query.Encode()

	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, parsedURL.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Request-ID", meta.RequestID)
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, &HTTPError{
			Status:  http.StatusBadGateway,
			Message: fmt.Sprintf("%s returned %d", parsedURL.Path, resp.StatusCode),
		}
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	logger.Info(ctx, "downstream call completed", map[string]any{
		"event":      "downstream_call_completed",
		"request_id": meta.RequestID,
		"scenario":   meta.Scenario,
		"operation":  operation,
		"transport":  "http",
	})

	return payload, nil
}
