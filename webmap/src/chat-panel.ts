/**
 * Collapsible chat panel for natural-language queries to the spatial agent.
 *
 * Sends messages to POST /api/agent/chat with {session_id, message}.
 * Receives streaming responses. The agent's materialized results appear on
 * the map via the WebSocket layer_ready flow — not through this panel.
 *
 * When the agent is unavailable (502), the panel shows a friendly message
 * and disables input.
 */

export interface ChatMessage {
  role: "user" | "assistant" | "error";
  content: string;
}

export class ChatPanel {
  private container: HTMLDivElement;
  private messageList: HTMLDivElement;
  private input: HTMLInputElement;
  private sendBtn: HTMLButtonElement;
  private messages: ChatMessage[] = [];
  private sessionId: string;
  private loading = false;
  private abortController: AbortController | null = null;

  constructor(sessionId: string) {
    this.sessionId = sessionId;

    // Build DOM
    this.container = document.createElement("div");
    this.container.id = "agent-chat-panel";
    this.container.innerHTML = `
      <div class="chat-header">
        <span class="chat-title">Spatial Agent</span>
        <span class="chat-status" id="chat-status">●</span>
      </div>
      <div class="chat-messages" id="chat-messages"></div>
      <div class="chat-input-row">
        <input type="text" id="chat-input" placeholder="Ask about your data…"
               autocomplete="off" />
        <button id="chat-send">▶</button>
      </div>
    `;

    this.messageList = this.container.querySelector("#chat-messages")!;
    this.input = this.container.querySelector("#chat-input")!;
    this.sendBtn = this.container.querySelector("#chat-send")!;

    this.sendBtn.addEventListener("click", () => this.send());
    this.input.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !this.loading) this.send();
    });
  }

  mount(parent: HTMLElement): void {
    parent.appendChild(this.container);
  }

  unmount(): void {
    this.abortController?.abort();
    this.container.remove();
  }

  setAgentStatus(connected: boolean): void {
    const dot = this.container.querySelector("#chat-status") as HTMLElement;
    if (dot) {
      dot.style.color = connected ? "#4ade80" : "#f87171";
      dot.title = connected ? "Agent connected" : "Agent unavailable";
    }
  }

  private async send(): Promise<void> {
    const text = this.input.value.trim();
    if (!text || this.loading) return;

    this.input.value = "";
    this.appendMessage({ role: "user", content: text });
    this.loading = true;
    this.sendBtn.disabled = true;

    this.abortController = new AbortController();
    const timeoutId = setTimeout(() => this.abortController?.abort(), 60_000);

    try {
      const resp = await fetch("/api/agent/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: this.sessionId,
          message: text,
        }),
        signal: this.abortController.signal,
      });

      if (!resp.ok) {
        if (resp.status === 502) {
          this.appendMessage({
            role: "error",
            content: "Agent is not running. Deploy the spatial-lakehouse-agent to enable chat.",
          });
        } else {
          this.appendMessage({
            role: "error",
            content: `Error: ${resp.status} ${resp.statusText}`,
          });
        }
        return;
      }

      // Stream the response (SSE or chunked text)
      const reader = resp.body?.getReader();
      const decoder = new TextDecoder();
      let assistantText = "";

      if (reader) {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          assistantText += decoder.decode(value, { stream: true });
        }
      } else {
        assistantText = await resp.text();
      }

      if (assistantText) {
        this.appendMessage({ role: "assistant", content: assistantText });
      }
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") {
        this.appendMessage({
          role: "error",
          content: "Request timed out or was cancelled.",
        });
      } else {
        this.appendMessage({
          role: "error",
          content: "Failed to reach agent. Is the agent container running?",
        });
      }
    } finally {
      clearTimeout(timeoutId);
      this.abortController = null;
      this.loading = false;
      this.sendBtn.disabled = false;
    }
  }

  private appendMessage(msg: ChatMessage): void {
    this.messages.push(msg);
    const el = document.createElement("div");
    el.className = `chat-msg chat-msg-${msg.role}`;
    el.textContent = msg.content;
    this.messageList.appendChild(el);
    this.messageList.scrollTop = this.messageList.scrollHeight;
  }
}
