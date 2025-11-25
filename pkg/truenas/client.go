// Package truenas provides a client for the TrueNAS Scale API.
package truenas

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"k8s.io/klog/v2"
)

// ClientConfig holds the configuration for the TrueNAS client.
type ClientConfig struct {
	Host          string
	Port          int
	Protocol      string
	APIKey        string
	AllowInsecure bool
	Timeout       time.Duration
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
	JSONRPC string           `json:"jsonrpc"`
	ID      int64            `json:"id"`
	Result  interface{}      `json:"result,omitempty"`
	Error   *rpcError        `json:"error,omitempty"`
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

	// Build WebSocket URL
	wsScheme := "ws"
	if c.config.Protocol == "https" {
		wsScheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s:%d/api/current", wsScheme, c.config.Host, c.config.Port)

	klog.V(2).Infof("Connecting to TrueNAS WebSocket API: %s", wsURL)

	// Configure dialer
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	if c.config.AllowInsecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Connect
	headers := http.Header{}
	headers.Set("User-Agent", "truenas-scale-csi")

	conn, _, err := dialer.Dial(wsURL, headers)
	if err != nil {
		return fmt.Errorf("failed to connect to TrueNAS: %w", err)
	}

	c.conn = conn
	c.closed = false

	// Start message reader
	go c.readMessages()

	// Authenticate
	if err := c.authenticate(); err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return fmt.Errorf("authentication failed: %w", err)
	}

	c.authenticated = true
	klog.Info("Successfully connected and authenticated to TrueNAS")

	return nil
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
			return nil, fmt.Errorf("TrueNAS API error [%d]: %s", resp.Error.Code, resp.Error.Message)
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
	defer c.mu.Lock()

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, fmt.Errorf("TrueNAS API error [%d]: %s", resp.Error.Code, resp.Error.Message)
		}
		return resp.Result, nil
	case <-time.After(c.config.Timeout):
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("request timeout: %s", method)
	}
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
