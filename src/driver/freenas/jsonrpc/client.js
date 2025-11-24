const WebSocket = require("ws");
const ReconnectingWebSocket = require("reconnecting-websocket");
const { v4: uuidv4 } = require("uuid");
const { stringify } = require("../../../utils/general");

class Client {
  constructor(options = {}) {
    this.options = JSON.parse(JSON.stringify(options));
    this.logger = this.options.logger || console;
    this.requests = new Map();
    this.isConnected = false;
    this.isAuthenticated = false;
    this.isReconnecting = false;
    this.defaultTimeout = this.options.timeout || 60000; // 60 seconds default

    // Construct URL
    const protocol = this.options.protocol === "https" ? "wss" : "ws";
    const host = this.options.host;
    const port = this.options.port || (protocol === "wss" ? 443 : 80);
    this.url = `${protocol}://${host}:${port}/websocket`;

    const allowInsecure = !!this.options.allowInsecure;

    const rwsOptions = {
      WebSocket: class extends WebSocket {
        constructor(url, protocols) {
          super(url, protocols, {
            rejectUnauthorized: !allowInsecure,
          });
        }
      },
      connectionTimeout: 5000,
      maxRetries: Infinity,
    };

    this.socket = new ReconnectingWebSocket(this.url, [], rwsOptions);

    this.connectPromise = new Promise((resolve, reject) => {
      this.connectResolver = resolve;
      this.connectRejector = reject;
    });

    this.socket.addEventListener("open", this.onOpen.bind(this));
    this.socket.addEventListener("message", this.onMessage.bind(this));
    this.socket.addEventListener("close", this.onClose.bind(this));
    this.socket.addEventListener("error", this.onError.bind(this));
  }

  async onOpen() {
    this.logger.info("WebSocket connected");
    this.isConnected = true;
    this.isReconnecting = false;
    try {
      await this.authenticate();
      if (this.connectResolver) {
        this.connectResolver();
        this.connectResolver = null;
        this.connectRejector = null;
      }
    } catch (err) {
      this.logger.error("Authentication failed", err);
      if (this.connectRejector) {
        this.connectRejector(err);
      }
      // If authentication fails, we probably shouldn't keep retrying the same connection immediately
      // but ReconnectingWebSocket will handle reconnects.
      // We might want to close properly if auth fails permanently.
    }
  }

  onClose(event) {
    this.logger.info("WebSocket closed", event.code, event.reason);
    this.isConnected = false;
    this.isAuthenticated = false;
    this.isReconnecting = true;

    // We don't necessarily reject pending requests here, RWS might reconnect.
    // But for this use case, we might want to fail fast?
    // Actually, we should probably recreate the connectPromise so subsequent calls wait.
    if (!this.connectResolver) {
      this.connectPromise = new Promise((resolve, reject) => {
        this.connectResolver = resolve;
        this.connectRejector = reject;
      });
    }
  }

  onError(err) {
    this.logger.error("WebSocket error", err);
  }

  onMessage(event) {
    try {
      const data = JSON.parse(event.data);
      if (data.msg === "result") {
        if (data.id && this.requests.has(data.id)) {
          const { resolve, reject } = this.requests.get(data.id);
          this.requests.delete(data.id);
          if (data.error) {
            reject(data.error);
          } else {
            resolve(data.result);
          }
        }
      } else if (data.msg === "connected") {
        this.logger.debug("Received connected message from server");
      } else if (data.msg === "failed") {
        this.logger.error("Received failed message from server", data);
      }
    } catch (err) {
      this.logger.error("Error parsing message", err);
    }
  }

  async authenticate() {
    this.logger.debug("Authenticating...");
    // We bypass this.call() logic that waits for connectPromise to avoid deadlock
    // since we are inside onOpen (which resolves connectPromise)

    const id = uuidv4();
    let method, params;

    if (this.options.apiKey) {
      method = "auth.login_with_api_key";
      params = [this.options.apiKey];
    } else if (this.options.username && this.options.password) {
      method = "auth.login";
      params = [this.options.username, this.options.password];
    } else {
      this.logger.warn("No authentication credentials provided");
      return;
    }

    const payload = {
      id,
      msg: "method",
      method,
      params,
    };

    return new Promise((resolve, reject) => {
      this.requests.set(id, { resolve, reject });
      this.socket.send(JSON.stringify(payload));
    })
      .then((res) => {
        if (res === true || (typeof res === "string" && res.length > 0)) {
            // auth.login returns true on success
            // auth.login_with_api_key returns boolean? check docs.
            // Docs say: "Returns true if login is successful."
            this.isAuthenticated = true;
            this.logger.info("Authentication successful");
        } else {
             // Handle cases where it returns something else but not error?
             this.isAuthenticated = true;
             this.logger.info("Authentication successful (checked)");
        }
        return res;
      })
      .catch((err) => {
        this.logger.error("Authentication error", err);
        throw err;
      });
  }

  async call(method, params = [], timeout = null) {
    if (!this.isConnected || !this.isAuthenticated) {
      await this.connectPromise;
    }

    const id = uuidv4();
    const payload = {
      id,
      msg: "method",
      method,
      params,
    };

    this.logger.debug(`Sending JSON-RPC call: ${method}`, { id });

    // Use provided timeout or default
    const timeoutMs = timeout !== null ? timeout : this.defaultTimeout;

    const requestPromise = new Promise((resolve, reject) => {
      this.requests.set(id, { resolve, reject });
      this.socket.send(JSON.stringify(payload));
    });

    // Add timeout handling
    if (timeoutMs > 0) {
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          if (this.requests.has(id)) {
            this.requests.delete(id);
            reject(new Error(`Request timeout after ${timeoutMs}ms for method: ${method}`));
          }
        }, timeoutMs);
      });

      return Promise.race([requestPromise, timeoutPromise]);
    }

    return requestPromise;
  }

  close() {
    this.socket.close();
  }
}

module.exports.Client = Client;
