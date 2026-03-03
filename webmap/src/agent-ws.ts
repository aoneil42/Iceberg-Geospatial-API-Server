/**
 * WebSocket client for receiving layer_ready events from the agent.
 *
 * Connects to /ws/agent/{sessionId}. When the agent materializes a query
 * result, the API pushes a LayerReadyEvent through this WebSocket. The
 * webmap then loads the new layer via the existing REST pipeline.
 *
 * Gracefully handles the agent being absent (connection failure → retry
 * with backoff, max 3 attempts, then stop).
 */

export interface LayerReadyEvent {
  type: "layer_ready";
  namespace: string;
  table: string;
  row_count: number;
  bbox: [number, number, number, number] | null;
  description: string;
}

export type LayerReadyCallback = (event: LayerReadyEvent) => void;

export class AgentWebSocket {
  private ws: WebSocket | null = null;
  private pingInterval: ReturnType<typeof setInterval> | null = null;
  private retryTimeout: ReturnType<typeof setTimeout> | null = null;
  private retryCount = 0;
  private maxRetries = 3;
  private disposed = false;

  constructor(
    private sessionId: string,
    private onLayerReady: LayerReadyCallback,
    private onStatusChange?: (connected: boolean) => void,
  ) {}

  connect(): void {
    if (this.disposed) return;

    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${window.location.host}/ws/agent/${this.sessionId}`;

    try {
      this.ws = new WebSocket(url);
    } catch {
      this.onStatusChange?.(false);
      return;
    }

    this.ws.onopen = () => {
      this.retryCount = 0;
      this.onStatusChange?.(true);

      // Client-side keepalive: send ping every 30s
      this.pingInterval = setInterval(() => {
        if (this.ws?.readyState === WebSocket.OPEN) {
          this.ws.send("ping");
        }
      }, 30_000);
    };

    this.ws.onmessage = (msg) => {
      try {
        const event = JSON.parse(msg.data);
        if (event.type === "layer_ready") {
          this.onLayerReady(event as LayerReadyEvent);
        }
      } catch {
        // ignore malformed messages
      }
    };

    this.ws.onclose = () => {
      this.clearPing();
      this.onStatusChange?.(false);
      if (!this.disposed && this.retryCount < this.maxRetries) {
        this.retryCount++;
        const delay = Math.min(1000 * 2 ** this.retryCount, 10_000);
        this.retryTimeout = setTimeout(() => this.connect(), delay);
      }
    };

    this.ws.onerror = () => {
      // onclose will fire next; handle retry there
    };
  }

  disconnect(): void {
    this.disposed = true;
    this.clearPing();
    if (this.retryTimeout) {
      clearTimeout(this.retryTimeout);
      this.retryTimeout = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  private clearPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }
}
