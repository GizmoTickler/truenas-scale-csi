// Package truenas provides a client for the TrueNAS Scale API.
package truenas

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	Host              string
	Port              int
	Protocol          string
	APIKey            string
	AllowInsecure     bool
	Timeout           time.Duration
	ConnectTimeout    time.Duration
	MaxRetries        int           // Maximum number of connection retries (default: 3)
	RetryInterval     time.Duration // Initial retry interval (default: 1s, exponential backoff applied)
	HeartbeatInterval time.Duration // Interval for WebSocket heartbeat (default: 30s)
}

// writeRequest represents a request to be written to the WebSocket.
type writeRequest struct {
	data     interface{}
	resultCh chan error
}

// connectionState represents the current state of the connection.
type connectionState int32

const (
	stateDisconnected connectionState = iota
	stateConnecting
	stateConnected
)

// Client is a TrueNAS API client using WebSocket JSON-RPC 2.0.
type Client struct {
	config        *ClientConfig
	conn          *websocket.Conn
	mu            sync.RWMutex
	messageID     int64
	pending       map[int64]chan *rpcResponse
	pendingMu     sync.RWMutex // Separate lock for pending map to reduce contention
	authenticated bool
	closed        bool

	// Connection state management (fixes thundering herd)
	connState int32 // atomic connectionState
	connCond  *sync.Cond
	connMu    sync.Mutex

	// Write loop channel (fixes API client locking)
	writeCh       chan writeRequest
	writeLoopDone chan struct{}

	// Heartbeat management (fixes silent disconnects)
	heartbeatDone chan struct{}
	lastPong      int64 // atomic unix timestamp

	// Safe channel closure (BUG-001 fix: prevents race condition on channel close)
	closeMu             sync.Mutex
	writeLoopDoneClosed bool
	heartbeatDoneClosed bool
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
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
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
		config:        cfg,
		pending:       make(map[int64]chan *rpcResponse),
		writeCh:       make(chan writeRequest, 100), // Buffered channel for write requests
		writeLoopDone: make(chan struct{}),
		heartbeatDone: make(chan struct{}),
	}
	client.connCond = sync.NewCond(&client.connMu)

	if err := client.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

// connect establishes the WebSocket connection and authenticates.
// Uses atomic state to prevent thundering herd on reconnection.
func (c *Client) connect() error {
	// Fast path: already connected
	if atomic.LoadInt32(&c.connState) == int32(stateConnected) && c.conn != nil {
		return nil
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	// Double-check after acquiring lock
	currentState := connectionState(atomic.LoadInt32(&c.connState))

	switch currentState {
	case stateConnected:
		if c.conn != nil {
			return nil
		}
		// Connection was lost, proceed to reconnect
	case stateConnecting:
		// Another goroutine is connecting, wait for it
		for atomic.LoadInt32(&c.connState) == int32(stateConnecting) {
			c.connCond.Wait()
		}
		// Check result
		if atomic.LoadInt32(&c.connState) == int32(stateConnected) && c.conn != nil {
			return nil
		}
		// Connection failed, try again
	}

	// Mark as connecting
	atomic.StoreInt32(&c.connState, int32(stateConnecting))

	err := c.connectWithRetry()

	if err != nil {
		atomic.StoreInt32(&c.connState, int32(stateDisconnected))
	} else {
		atomic.StoreInt32(&c.connState, int32(stateConnected))
	}

	// Wake up all waiters
	c.connCond.Broadcast()

	return err
}

// connectWithRetry attempts to connect with exponential backoff retry.
// Must be called with connMu held.
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
			// Sleep without holding the connMu lock
			c.connMu.Unlock()
			time.Sleep(retryInterval)
			c.connMu.Lock()
			// Check if closed during sleep
			if c.closed {
				return fmt.Errorf("client closed during reconnection")
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

		c.mu.Lock()
		c.conn = conn
		c.closed = false
		// Reinitialize channels for new connection
		c.writeCh = make(chan writeRequest, 100)
		c.writeLoopDone = make(chan struct{})
		c.heartbeatDone = make(chan struct{})
		c.mu.Unlock()

		// Reset channel closure flags for new connection (BUG-001 fix)
		c.closeMu.Lock()
		c.writeLoopDoneClosed = false
		c.heartbeatDoneClosed = false
		c.closeMu.Unlock()

		// Start message reader goroutine
		go c.readMessages()

		// Start write loop goroutine (non-blocking writes)
		go c.writeLoop()

		// Authenticate using direct write (before write loop processes requests)
		if err := c.authenticateDirect(); err != nil {
			c.cleanupConnection()
			lastErr = fmt.Errorf("authentication failed: %w", err)
			klog.Warningf("TrueNAS authentication attempt %d failed: %v", attempt+1, err)
			continue
		}

		c.mu.Lock()
		c.authenticated = true
		atomic.StoreInt64(&c.lastPong, time.Now().Unix())
		c.mu.Unlock()

		// Start heartbeat goroutine after successful auth
		go c.heartbeatLoop()

		klog.Infof("Successfully connected and authenticated to TrueNAS (attempt %d)", attempt+1)
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// cleanupConnection closes the connection and stops goroutines.
func (c *Client) cleanupConnection() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.authenticated = false
	c.mu.Unlock()

	// Signal goroutines to stop (BUG-001 fix: safe channel closure with mutex)
	c.closeMu.Lock()
	if !c.writeLoopDoneClosed {
		close(c.writeLoopDone)
		c.writeLoopDoneClosed = true
	}
	if !c.heartbeatDoneClosed {
		close(c.heartbeatDone)
		c.heartbeatDoneClosed = true
	}
	c.closeMu.Unlock()
}

// authenticateDirect performs API key authentication using direct write.
// This is used during initial connection before the write loop is active.
func (c *Client) authenticateDirect() error {
	c.mu.Lock()
	c.messageID++
	id := c.messageID
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("no connection")
	}

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  "auth.login_with_api_key",
		Params:  []interface{}{c.config.APIKey},
	}

	// Create response channel
	respChan := make(chan *rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[id] = respChan
	c.pendingMu.Unlock()

	// Write directly (write loop not yet processing)
	if err := conn.WriteJSON(req); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return fmt.Errorf("failed to send auth request: %w", err)
	}

	// Wait for response with timeout
	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return &APIError{
				Code:    resp.Error.Code,
				Message: resp.Error.Message,
				Data:    resp.Error.Data,
			}
		}
		success, ok := resp.Result.(bool)
		if !ok || !success {
			return fmt.Errorf("authentication returned unexpected result: %v", resp.Result)
		}
		return nil
	case <-time.After(c.config.Timeout):
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return fmt.Errorf("authentication timeout")
	}
}

// writeLoop handles all WebSocket writes in a dedicated goroutine.
// This prevents blocking Call() when the network is slow.
func (c *Client) writeLoop() {
	for {
		select {
		case <-c.writeLoopDone:
			return
		case req, ok := <-c.writeCh:
			if !ok {
				return
			}

			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			var err error
			if conn == nil {
				err = fmt.Errorf("no connection")
			} else {
				err = conn.WriteJSON(req.data)
			}

			// Send result back to caller
			select {
			case req.resultCh <- err:
			default:
			}
		}
	}
}

// heartbeatLoop sends periodic pings to detect connection issues.
func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatDone:
			return
		case <-ticker.C:
			// Check if connection is still valid
			c.mu.RLock()
			conn := c.conn
			closed := c.closed
			c.mu.RUnlock()

			if closed || conn == nil {
				return
			}

			// Send a lightweight ping via core.ping API call
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := c.CallWithContext(ctx, "core.ping")
			cancel()

			if err != nil {
				klog.Warningf("Heartbeat ping failed: %v", err)
				// Check if we've missed too many heartbeats
				lastPong := atomic.LoadInt64(&c.lastPong)
				if time.Since(time.Unix(lastPong, 0)) > c.config.HeartbeatInterval*3 {
					klog.Errorf("Connection appears dead (no response for %v), triggering reconnect", time.Since(time.Unix(lastPong, 0)))
					c.handleDisconnect()
					return
				}
			} else {
				atomic.StoreInt64(&c.lastPong, time.Now().Unix())
				klog.V(5).Info("Heartbeat ping successful")
			}
		}
	}
}

// readMessages reads incoming WebSocket messages.
func (c *Client) readMessages() {
	for {
		c.mu.RLock()
		conn := c.conn
		closed := c.closed
		c.mu.RUnlock()

		if closed || conn == nil {
			return
		}

		var resp rpcResponse
		if err := conn.ReadJSON(&resp); err != nil {
			c.mu.RLock()
			wasClosed := c.closed
			c.mu.RUnlock()
			if !wasClosed {
				klog.Errorf("WebSocket read error: %v", err)
			}
			c.handleDisconnect()
			return
		}

		// Route response to waiting caller
		c.pendingMu.Lock()
		if ch, ok := c.pending[resp.ID]; ok {
			ch <- &resp
			delete(c.pending, resp.ID)
		}
		c.pendingMu.Unlock()
	}
}

// handleDisconnect handles WebSocket disconnection.
func (c *Client) handleDisconnect() {
	// Update connection state
	atomic.StoreInt32(&c.connState, int32(stateDisconnected))

	c.mu.Lock()
	c.authenticated = false
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	// Close the connection if still open
	if conn != nil {
		_ = conn.Close()
	}

	// Stop write loop and heartbeat (BUG-001 fix: safe channel closure with mutex)
	c.closeMu.Lock()
	if !c.writeLoopDoneClosed {
		close(c.writeLoopDone)
		c.writeLoopDoneClosed = true
	}
	if !c.heartbeatDoneClosed {
		close(c.heartbeatDone)
		c.heartbeatDoneClosed = true
	}
	c.closeMu.Unlock()

	// Cancel all pending requests
	c.pendingMu.Lock()
	for id, ch := range c.pending {
		select {
		case ch <- &rpcResponse{
			ID: id,
			Error: &rpcError{
				Code:    -1,
				Message: "connection lost",
			},
		}:
		default:
		}
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	// Notify waiters that connection state changed
	c.connCond.Broadcast()
}

// Call makes a JSON-RPC call to the TrueNAS API.
func (c *Client) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return c.CallWithContext(ctx, method, params...)
}

// CallWithContext makes a JSON-RPC call with a context for cancellation/timeout.
func (c *Client) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	// Ensure connected
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		if err := c.connect(); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	c.messageID++
	id := c.messageID
	writeCh := c.writeCh
	c.mu.Unlock()

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	// Create response channel
	respChan := make(chan *rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[id] = respChan
	c.pendingMu.Unlock()

	// Send request through write channel (non-blocking)
	writeReq := writeRequest{
		data:     req,
		resultCh: make(chan error, 1),
	}

	select {
	case writeCh <- writeReq:
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()
	}

	// Wait for write result
	select {
	case err := <-writeReq.resultCh:
		if err != nil {
			c.pendingMu.Lock()
			delete(c.pending, id)
			c.pendingMu.Unlock()
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()
	}

	klog.V(4).Infof("TrueNAS API call: %s", method)

	// Wait for response
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
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("request timeout: %s", method)
	}
}

// Close closes the WebSocket connection.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	// Update state
	atomic.StoreInt32(&c.connState, int32(stateDisconnected))

	// Stop goroutines (BUG-001 fix: safe channel closure with mutex)
	c.closeMu.Lock()
	if !c.writeLoopDoneClosed {
		close(c.writeLoopDone)
		c.writeLoopDoneClosed = true
	}
	if !c.heartbeatDoneClosed {
		close(c.heartbeatDone)
		c.heartbeatDoneClosed = true
	}
	c.closeMu.Unlock()

	// Notify waiters
	c.connCond.Broadcast()

	if conn != nil {
		return conn.Close()
	}
	return nil
}

// IsConnected returns true if the client is connected and authenticated.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && c.authenticated && atomic.LoadInt32(&c.connState) == int32(stateConnected)
}
