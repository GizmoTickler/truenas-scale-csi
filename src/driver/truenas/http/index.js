const _ = require("lodash");
const WebSocket = require("ws");
const { stringify } = require("../../../utils/general");
const USER_AGENT = "democratic-csi-driver";

/**
 * TrueNAS SCALE 25.04+ WebSocket JSON-RPC 2.0 Client
 *
 * This client implements the JSON-RPC 2.0 protocol over WebSocket
 * for communication with TrueNAS SCALE 25.04 and later versions.
 *
 * References:
 * - https://api.truenas.com/v25.04.2/jsonrpc.html
 * - https://github.com/truenas/api_client
 */
class Client {
  constructor(options = {}) {
    this.options = JSON.parse(JSON.stringify(options));
    this.logger = console;
    this.ws = null;
    this.authenticated = false;
    this.messageId = 0;
    this.pendingRequests = new Map();
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 2000; // Start with 2 seconds
    this.eventSubscriptions = new Map();
    this.connectPromise = null;
  }

  /**
   * Get WebSocket URL for TrueNAS SCALE 25.04+
   * Format: ws://host:port/api/current or wss://host:port/api/current
   */
  getWebSocketURL() {
    const server = this.options;

    // Determine protocol
    let protocol = server.protocol || "http";
    if (server.port) {
      if (String(server.port).includes("443")) {
        protocol = "https";
      } else if (String(server.port).includes("80")) {
        protocol = "http";
      }
    }

    // Convert http/https to ws/wss
    const wsProtocol = protocol === "https" ? "wss" : "ws";

    // Build WebSocket URL - use /api/current for versioned JSON-RPC API
    const port = server.port ? `:${server.port}` : "";
    return `${wsProtocol}://${server.host}${port}/api/current`;
  }

  /**
   * Connect to TrueNAS WebSocket API and authenticate
   */
  async connect() {
    // Return existing connection promise if already connecting
    if (this.connectPromise) {
      return this.connectPromise;
    }

    // Return immediately if already connected and authenticated
    if (this.ws && this.ws.readyState === WebSocket.OPEN && this.authenticated) {
      return Promise.resolve();
    }

    this.connectPromise = this._doConnect();

    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  async _doConnect() {
    const url = this.getWebSocketURL();
    this.logger.debug(`Connecting to TrueNAS WebSocket API: ${url}`);

    return new Promise((resolve, reject) => {
      try {
        const wsOptions = {
          headers: {
            "User-Agent": USER_AGENT,
          },
        };

        // Handle insecure TLS
        if (this.options.allowInsecure) {
          wsOptions.rejectUnauthorized = false;
        }

        this.ws = new WebSocket(url, wsOptions);

        this.ws.on("open", async () => {
          this.logger.debug("WebSocket connection established");
          this.reconnectAttempts = 0;

          try {
            // Authenticate immediately after connection
            await this._authenticate();
            this.authenticated = true;
            resolve();
          } catch (error) {
            this.logger.error("Authentication failed:", error);
            reject(error);
          }
        });

        this.ws.on("message", (data) => {
          this._handleMessage(data);
        });

        this.ws.on("error", (error) => {
          this.logger.error("WebSocket error:", error);
          if (!this.authenticated) {
            reject(error);
          }
        });

        this.ws.on("close", (code, reason) => {
          this.logger.warn(`WebSocket closed: ${code} - ${reason}`);
          this.authenticated = false;

          // Reject all pending requests
          this.pendingRequests.forEach((pending) => {
            pending.reject(new Error("WebSocket connection closed"));
          });
          this.pendingRequests.clear();

          // Attempt to reconnect
          this._scheduleReconnect();
        });

      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Authenticate with TrueNAS API
   */
  async _authenticate() {
    if (this.options.apiKey) {
      // Authenticate with API key
      this.logger.debug("Authenticating with API key");
      await this.call("auth.login_with_api_key", [this.options.apiKey]);
    } else if (this.options.username && this.options.password) {
      // Authenticate with username/password
      this.logger.debug(`Authenticating with username: ${this.options.username}`);
      await this.call("auth.login", [this.options.username, this.options.password]);
    } else {
      throw new Error("No authentication credentials provided (apiKey or username/password required)");
    }
    this.logger.debug("Authentication successful");
  }

  /**
   * Schedule reconnection with exponential backoff
   */
  _scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.logger.error("Max reconnection attempts reached");
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);

    this.logger.info(`Scheduling reconnection attempt ${this.reconnectAttempts} in ${delay}ms`);

    setTimeout(() => {
      this.connect().catch((error) => {
        this.logger.error("Reconnection failed:", error);
      });
    }, delay);
  }

  /**
   * Handle incoming WebSocket messages
   */
  _handleMessage(data) {
    try {
      const message = JSON.parse(data.toString());

      this.logger.debug("Received message:", stringify(message));

      // Handle JSON-RPC response
      if (message.id !== undefined && this.pendingRequests.has(message.id)) {
        const pending = this.pendingRequests.get(message.id);
        this.pendingRequests.delete(message.id);

        if (message.error) {
          pending.reject(this._createError(message.error));
        } else {
          pending.resolve(message.result);
        }
      }
      // Handle JSON-RPC notification (event)
      else if (message.method && message.id === undefined) {
        this._handleEvent(message.method, message.params);
      }
    } catch (error) {
      this.logger.error("Error parsing WebSocket message:", error);
    }
  }

  /**
   * Handle event notifications
   */
  _handleEvent(method, params) {
    const handlers = this.eventSubscriptions.get(method) || [];
    handlers.forEach((handler) => {
      try {
        handler(params);
      } catch (error) {
        this.logger.error(`Error in event handler for ${method}:`, error);
      }
    });
  }

  /**
   * Create error object from JSON-RPC error
   */
  _createError(error) {
    const err = new Error(error.message || "TrueNAS API Error");
    err.code = error.code;
    err.data = error.data;
    return err;
  }

  /**
   * Make a JSON-RPC 2.0 method call
   *
   * @param {string} method - The API method to call (e.g., "pool.dataset.query")
   * @param {array} params - Array of parameters for the method
   * @param {object} options - Additional options (timeout, etc.)
   * @returns {Promise} - Promise that resolves with the result
   */
  async call(method, params = [], options = {}) {
    // Ensure connection is established
    if (!this.authenticated) {
      await this.connect();
    }

    const messageId = ++this.messageId;
    const request = {
      jsonrpc: "2.0",
      id: messageId,
      method: method,
      params: params,
    };

    this.logger.debug(`Calling method ${method} with params:`, stringify(params));

    return new Promise((resolve, reject) => {
      // Set timeout
      const timeout = options.timeout || 60000;
      const timeoutHandle = setTimeout(() => {
        if (this.pendingRequests.has(messageId)) {
          this.pendingRequests.delete(messageId);
          reject(new Error(`Request timeout: ${method}`));
        }
      }, timeout);

      // Store pending request
      this.pendingRequests.set(messageId, {
        resolve: (result) => {
          clearTimeout(timeoutHandle);
          resolve(result);
        },
        reject: (error) => {
          clearTimeout(timeoutHandle);
          reject(error);
        },
      });

      // Send request
      try {
        this.ws.send(JSON.stringify(request));
      } catch (error) {
        this.pendingRequests.delete(messageId);
        clearTimeout(timeoutHandle);
        reject(error);
      }
    });
  }

  /**
   * Subscribe to an event
   *
   * @param {string} event - Event name to subscribe to
   * @param {function} handler - Handler function to call when event is received
   */
  subscribe(event, handler) {
    if (!this.eventSubscriptions.has(event)) {
      this.eventSubscriptions.set(event, []);
    }
    this.eventSubscriptions.get(event).push(handler);
  }

  /**
   * Unsubscribe from an event
   */
  unsubscribe(event, handler) {
    if (!this.eventSubscriptions.has(event)) {
      return;
    }
    const handlers = this.eventSubscriptions.get(event);
    const index = handlers.indexOf(handler);
    if (index > -1) {
      handlers.splice(index, 1);
    }
  }

  /**
   * Close the WebSocket connection
   */
  async close() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
      this.authenticated = false;
    }
  }

  // Legacy compatibility methods (will be removed after full migration)
  async get() {
    throw new Error("HTTP GET is not supported. Use call() method with appropriate JSON-RPC method.");
  }

  async post() {
    throw new Error("HTTP POST is not supported. Use call() method with appropriate JSON-RPC method.");
  }

  async put() {
    throw new Error("HTTP PUT is not supported. Use call() method with appropriate JSON-RPC method.");
  }

  async delete() {
    throw new Error("HTTP DELETE is not supported. Use call() method with appropriate JSON-RPC method.");
  }
}

module.exports.Client = Client;
