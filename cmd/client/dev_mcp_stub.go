package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	defaultDevMCPStubListenAddr = "127.0.0.1:0"
	defaultDevMCPStubName       = "mcp-stub"
	defaultDevMCPStubVersion    = "0.1.0"
)

type devMCPStubOptions struct {
	ListenAddr    string
	ServerName    string
	ServerVersion string
}

type devMCPStubInstance struct {
	BaseURL  *url.URL
	listener net.Listener
	server   *http.Server
	errCh    chan error
}

type devStubEchoArgs struct {
	Input string `json:"input"`
}

type devStubEchoResult struct {
	Echoed string `json:"echoed"`
}

type devStubUppercaseArgs struct {
	Input string `json:"input"`
}

type devStubUppercaseResult struct {
	Uppercase string `json:"uppercase"`
}

type devStubServerInfoResult struct {
	Name          string   `json:"name"`
	Version       string   `json:"version"`
	Available     []string `json:"available_tools"`
	SamplePrompts []string `json:"sample_prompts"`
}

func startDevMCPStub(opts devMCPStubOptions) (*devMCPStubInstance, error) {
	listenAddr := strings.TrimSpace(opts.ListenAddr)
	if listenAddr == "" {
		listenAddr = defaultDevMCPStubListenAddr
	}
	serverName := strings.TrimSpace(opts.ServerName)
	if serverName == "" {
		serverName = defaultDevMCPStubName
	}
	serverVersion := strings.TrimSpace(opts.ServerVersion)
	if serverVersion == "" {
		serverVersion = defaultDevMCPStubVersion
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	instance := &devMCPStubInstance{
		BaseURL: &url.URL{
			Scheme: "http",
			Host:   listener.Addr().String(),
		},
		listener: listener,
		server: &http.Server{
			Handler:           newDevMCPStubHandler(serverName, serverVersion),
			ReadHeaderTimeout: 5 * time.Second,
		},
		errCh: make(chan error, 1),
	}
	go func() {
		err := instance.server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			instance.errCh <- err
			return
		}
		instance.errCh <- nil
	}()
	return instance, nil
}

func (s *devMCPStubInstance) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_ = s.server.Shutdown(ctx)
	return s.wait()
}

func (s *devMCPStubInstance) wait() error {
	if s == nil || s.errCh == nil {
		return nil
	}
	err := <-s.errCh
	s.errCh = nil
	return err
}

func (s *devMCPStubInstance) MCPURL() string {
	if s == nil || s.BaseURL == nil {
		return ""
	}
	return s.BaseURL.ResolveReference(&url.URL{Path: "/mcp"}).String()
}

func (s *devMCPStubInstance) ProtectedResourceMetadataURL() string {
	if s == nil || s.BaseURL == nil {
		return ""
	}
	return s.BaseURL.ResolveReference(&url.URL{Path: "/.well-known/oauth-protected-resource/mcp"}).String()
}

func (s *devMCPStubInstance) AuthorizationServerMetadataURL() string {
	if s == nil || s.BaseURL == nil {
		return ""
	}
	return s.BaseURL.ResolveReference(&url.URL{Path: "/.well-known/oauth-authorization-server"}).String()
}

func newDevMCPStubHandler(serverName string, serverVersion string) http.Handler {
	mux := http.NewServeMux()
	server := mcp.NewServer(&mcp.Implementation{
		Name:    serverName,
		Version: serverVersion,
	}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "server_info",
		Description: "Describe the demo MCP server and show a few sample prompts to try in ChatGPT",
	}, func(_ context.Context, _ *mcp.CallToolRequest, _ map[string]any) (*mcp.CallToolResult, devStubServerInfoResult, error) {
		result := devStubServerInfoResult{
			Name:      serverName,
			Version:   serverVersion,
			Available: []string{"server_info", "echo", "uppercase"},
			SamplePrompts: []string{
				"Use the echo tool with input 'hello from tunnel-client'.",
				"Use the uppercase tool on 'openai tunnel'.",
				"Call server_info and summarize the available demo tools.",
			},
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("%s %s demo tools: server_info, echo, uppercase", serverName, serverVersion)},
			},
		}, result, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "echo",
		Description: "Echo a string back to the caller",
	}, func(_ context.Context, _ *mcp.CallToolRequest, args devStubEchoArgs) (*mcp.CallToolResult, devStubEchoResult, error) {
		result := devStubEchoResult{Echoed: args.Input}
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: args.Input},
			},
		}, result, nil
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:        "uppercase",
		Description: "Return an uppercased copy of the provided input string",
	}, func(_ context.Context, _ *mcp.CallToolRequest, args devStubUppercaseArgs) (*mcp.CallToolResult, devStubUppercaseResult, error) {
		text := strings.ToUpper(args.Input)
		result := devStubUppercaseResult{Uppercase: text}
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, result, nil
	})

	mux.Handle("/mcp", newDevMCPStubStreamableHandler(server))
	mux.HandleFunc("/.well-known/oauth-protected-resource", func(w http.ResponseWriter, r *http.Request) {
		writeDevMCPStubProtectedResourceMetadata(w, r)
	})
	mux.HandleFunc("/.well-known/oauth-protected-resource/mcp", func(w http.ResponseWriter, r *http.Request) {
		writeDevMCPStubProtectedResourceMetadata(w, r)
	})
	mux.HandleFunc("/.well-known/oauth-authorization-server", func(w http.ResponseWriter, r *http.Request) {
		writeDevMCPStubAuthorizationServerMetadata(w, r)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		writeDevMCPStubJSON(w, map[string]any{"keys": []any{}})
	})
	return mux
}

func newDevMCPStubStreamableHandler(server *mcp.Server) http.Handler {
	getServer := func(*http.Request) *mcp.Server { return server }
	statefulHandler := mcp.NewStreamableHTTPHandler(getServer, nil)
	statelessHandler := mcp.NewStreamableHTTPHandler(getServer, &mcp.StreamableHTTPOptions{
		// Modern MCP requests are self-contained. The demo tools do not need
		// session state, but legacy clients still need their session lifecycle.
		Stateless: true,
	})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		legacy, err := isLegacyDevMCPStubRequest(req)
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		if legacy {
			statefulHandler.ServeHTTP(w, req)
			return
		}
		statelessHandler.ServeHTTP(w, req)
	})
}

func isLegacyDevMCPStubRequest(req *http.Request) (bool, error) {
	if req.Method != http.MethodPost || req.Header.Get("Mcp-Session-Id") != "" {
		return true, nil
	}
	// Only explicit modern protocol requests opt into the new handler. The
	// SDK still validates the version against the unchanged request metadata.
	if req.Header.Get("Mcp-Protocol-Version") < "2026-07-28" {
		return true, nil
	}

	// Inspect the method within the same body limit used by the SDK handlers,
	// then restore the body for the selected handler to validate and process.
	body, readErr := io.ReadAll(io.LimitReader(req.Body, mcp.DefaultMaxRequestBodyBytes+1))
	closeErr := req.Body.Close()
	if readErr != nil {
		return false, readErr
	}
	if len(body) > mcp.DefaultMaxRequestBodyBytes {
		return false, &http.MaxBytesError{Limit: mcp.DefaultMaxRequestBodyBytes}
	}
	if closeErr != nil {
		return false, closeErr
	}
	req.Body = io.NopCloser(bytes.NewReader(body))

	type methodEnvelope struct {
		Method string `json:"method"`
	}
	isLegacy := func(request methodEnvelope) bool {
		return request.Method == "initialize" || request.Method == "notifications/initialized"
	}
	var request methodEnvelope
	if err := json.Unmarshal(body, &request); err == nil {
		return isLegacy(request), nil
	}
	var batch []methodEnvelope
	if err := json.Unmarshal(body, &batch); err == nil {
		return slices.ContainsFunc(batch, isLegacy), nil
	}
	return false, nil
}

func writeDevMCPStubProtectedResourceMetadata(w http.ResponseWriter, r *http.Request) {
	base := devMCPStubBaseURL(r)
	writeDevMCPStubJSON(w, map[string]any{
		"resource":              base.ResolveReference(&url.URL{Path: "/mcp"}).String(),
		"authorization_servers": []string{base.String()},
		"scopes_supported":      []string{"read", "write"},
	})
}

func writeDevMCPStubAuthorizationServerMetadata(w http.ResponseWriter, r *http.Request) {
	base := devMCPStubBaseURL(r)
	writeDevMCPStubJSON(w, map[string]any{
		"issuer":                                base.String(),
		"authorization_endpoint":                base.ResolveReference(&url.URL{Path: "/authorize"}).String(),
		"token_endpoint":                        base.ResolveReference(&url.URL{Path: "/token"}).String(),
		"jwks_uri":                              base.ResolveReference(&url.URL{Path: "/jwks"}).String(),
		"registration_endpoint":                 base.ResolveReference(&url.URL{Path: "/register"}).String(),
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
		"token_endpoint_auth_methods_supported": []string{"none", "client_secret_post"},
		"code_challenge_methods_supported":      []string{"S256"},
	})
}

func writeDevMCPStubJSON(w http.ResponseWriter, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func devMCPStubBaseURL(r *http.Request) *url.URL {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return &url.URL{
		Scheme: scheme,
		Host:   r.Host,
	}
}
