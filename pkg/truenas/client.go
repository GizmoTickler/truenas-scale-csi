// Package truenas provides a client for the TrueNAS Scale API.
package truenas

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"k8s.io/klog/v2"
)

// APIError represents an error from the TrueNAS API.
type APIError struct {
	Code    int
	Message string
	Data    interface{}
}

func (e *APIError) Error() string {
	return fmt.Sprintf("TrueNAS API error [%d]: %s", e.Code, e.Message)
}

// IsNotFoundError returns true if the error indicates a resource was not found.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(*APIError); ok {
		// Common "not found" error codes from TrueNAS
		return apiErr.Code == -1 || strings.Contains(strings.ToLower(apiErr.Message), "not found") ||
			strings.Contains(strings.ToLower(apiErr.Message), "does not exist")
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist")
}

// IsAlreadyExistsError returns true if the error indicates a resource already exists.
func IsAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(*APIError); ok {
		return strings.Contains(strings.ToLower(apiErr.Message), "already exists")
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// IsConnectionError returns true if the error indicates a connection problem.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "refused") ||
		strings.Contains(errStr, "connection lost")
}

// ClientConfig holds the configuration for the TrueNAS client.
type ClientConfig struct {
	Host           string
	Port           int
	Protocol       string
	APIKey         string
	AllowInsecure  bool
	Timeout        time.Duration
	ConnectTimeout time.Duration
	MaxRetries     int           // Maximum number of connection retries (default: 3)
	RetryInterval  time.Duration // Initial retry interval (default: 1s, exponential backoff applied)
}

// Client is a TrueNAS API client using WebSocket JSON-RPC 2.0.
type Client struct {
	config        *ClientConfig
	conn          *websocket.Conn
	mu            sync.Mutex
	messageID     int64
	pending       map[int64]chan *rpcResponse
	authenticated bool
	closed        bool
}

// rpcRequest is a JSON-RPC 2.0 request.
type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int64         `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params,omitempty"`
}

// rpcResponse is a JSON-RPC 2.0 response.
type rpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int64       `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
}

// rpcError is a JSON-RPC 2.0 error.
type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// NewClient creates a new TrueNAS API client.
func NewClient(cfg *ClientConfig) (*Client, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("api key is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 1 * time.Second
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "https"
	}
	if cfg.Port == 0 {
		if cfg.Protocol == "https" {
			cfg.Port = 443
		} else {
			cfg.Port = 80
		}
	}

	client := &Client{
		config:  cfg,
		pending: make(map[int64]chan *rpcResponse),
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

// connect establishes the WebSocket connection and authenticates.
func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	return c.connectWithRetry()
}

// connectWithRetry attempts to connect with exponential backoff retry.
// Must be called with mutex held.
func (c *Client) connectWithRetry() error {
	// Build WebSocket URL
	wsScheme := "ws"
	if c.config.Protocol == "https" {
		wsScheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s:%d/api/current", wsScheme, c.config.Host, c.config.Port)

	// Configure dialer
	dialer := websocket.Dialer{
		HandshakeTimeout: c.config.ConnectTimeout,
	}
	if c.config.AllowInsecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	headers := http.Header{}
	headers.Set("User-Agent", "truenas-scale-csi")

	var lastErr error
	retryInterval := c.config.RetryInterval

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			klog.V(2).Infof("Retrying TrueNAS connection (attempt %d/%d) after %v", attempt, c.config.MaxRetries, retryInterval)
			// Unlock during sleep to allow other operations
			c.mu.Unlock()
			time.Sleep(retryInterval)
			c.mu.Lock()
			// Check if connection was established while we were sleeping
			if c.conn != nil {
				return nil
			}
			// Exponential backoff (double the interval, max 30s)
			retryInterval *= 2
			if retryInterval > 30*time.Second {
				retryInterval = 30 * time.Second
			}
		}

		klog.V(2).Infof("Connecting to TrueNAS WebSocket API: %s (attempt %d)", wsURL, attempt+1)

		conn, _, err := dialer.Dial(wsURL, headers)
		if err != nil {
			lastErr = fmt.Errorf("failed to connect to TrueNAS: %w", err)
			klog.Warningf("TrueNAS connection attempt %d failed: %v", attempt+1, err)
			continue
		}

		c.conn = conn
		c.closed = false

		// Start message reader
		go c.readMessages()

		// Authenticate
		if err := c.authenticate(); err != nil {
			_ = c.conn.Close()
			c.conn = nil
			lastErr = fmt.Errorf("authentication failed: %w", err)
			klog.Warningf("TrueNAS authentication attempt %d failed: %v", attempt+1, err)
			continue
		}

		c.authenticated = true
		klog.Infof("Successfully connected and authenticated to TrueNAS (attempt %d)", attempt+1)
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// authenticate performs API key authentication.
func (c *Client) authenticate() error {
	result, err := c.callLocked("auth.login_with_api_key", c.config.APIKey)
	if err != nil {
		return err
	}

	success, ok := result.(bool)
	if !ok || !success {
		return fmt.Errorf("authentication returned unexpected result: %v", result)
	}

	return nil
}

// readMessages reads incoming WebSocket messages.
func (c *Client) readMessages() {
	for {
		var resp rpcResponse
		if err := c.conn.ReadJSON(&resp); err != nil {
			if !c.closed {
				klog.Errorf("WebSocket read error: %v", err)
			}
			c.handleDisconnect()
			return
		}

		// Route response to waiting caller
		c.mu.Lock()
		if ch, ok := c.pending[resp.ID]; ok {
			ch <- &resp
			delete(c.pending, resp.ID)
		}
		c.mu.Unlock()
	}
}

// handleDisconnect handles WebSocket disconnection.
func (c *Client) handleDisconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.authenticated = false
	c.conn = nil

	// Cancel all pending requests
	for id, ch := range c.pending {
		ch <- &rpcResponse{
			ID: id,
			Error: &rpcError{
				Code:    -1,
				Message: "connection lost",
			},
		}
		delete(c.pending, id)
	}
}

// Call makes a JSON-RPC call to the TrueNAS API.
func (c *Client) Call(method string, params ...interface{}) (interface{}, error) {
	// Ensure connected
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	c.messageID++
	id := c.messageID

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	// Create response channel
	respChan := make(chan *rpcResponse, 1)
	c.pending[id] = respChan

	// Send request
	if err := c.conn.WriteJSON(req); err != nil {
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	c.mu.Unlock()

	klog.V(4).Infof("TrueNAS API call: %s", method)

	// Wait for response with timeout
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, &APIError{
				Code:    resp.Error.Code,
				Message: resp.Error.Message,
				Data:    resp.Error.Data,
			}
		}
		return resp.Result, nil
	case <-time.After(c.config.Timeout):
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("request timeout: %s", method)
	}
}

// callLocked makes a call while already holding the mutex (for authentication).
func (c *Client) callLocked(method string, params ...interface{}) (interface{}, error) {
	c.messageID++
	id := c.messageID

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	// Create response channel
	respChan := make(chan *rpcResponse, 1)
	c.pending[id] = respChan

	// Send request
	if err := c.conn.WriteJSON(req); err != nil {
		delete(c.pending, id)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Temporarily unlock to allow message reader to work
	c.mu.Unlock()

	// Wait for response with timeout
	var resp *rpcResponse
	var timedOut bool
	select {
	case resp = <-respChan:
		// Response received
	case <-time.After(c.config.Timeout):
		timedOut = true
	}

	// Re-acquire lock before returning (caller expects lock to be held)
	c.mu.Lock()

	if timedOut {
		delete(c.pending, id)
		return nil, fmt.Errorf("request timeout: %s", method)
	}

	if resp.Error != nil {
		return nil, &APIError{
			Code:    resp.Error.Code,
			Message: resp.Error.Message,
			Data:    resp.Error.Data,
		}
	}
	return resp.Result, nil
}

// Close closes the WebSocket connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// IsConnected returns true if the client is connected and authenticated.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && c.authenticated
}
