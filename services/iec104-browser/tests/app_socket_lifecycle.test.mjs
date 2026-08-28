import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

class FakeElement {
  constructor() {
    this.children = [];
    this.classList = { toggle() {} };
    this.dataset = {};
    this.disabled = false;
    this.height = 0;
    this.listeners = new Map();
    this.textContent = "";
    this.value = "";
    this.width = 0;
  }

  addEventListener(eventName, listener) {
    const listeners = this.listeners.get(eventName) ?? [];
    listeners.push(listener);
    this.listeners.set(eventName, listeners);
  }

  append(...children) {
    this.children.push(...children);
  }

  getBoundingClientRect() {
    return { height: 240, width: 640 };
  }

  getContext() {
    return {
      clearRect() {},
      setTransform() {},
    };
  }

  replaceChildren(...children) {
    this.children = children;
  }

  setAttribute() {}
}

function createBrowser() {
  const elements = new Map();
  for (const id of [
    "connection-state",
    "detail-content",
    "detail-title",
    "event-cap",
    "event-count",
    "event-filter",
    "event-rows",
    "table-state",
    "trend-canvas",
    "trend-count",
    "trend-series",
    "trend-state",
    "empty-detail-template",
  ]) {
    elements.set(`#${id}`, new FakeElement());
  }
  elements.get("#empty-detail-template").content = {
    cloneNode: () => new FakeElement(),
  };

  const document = {
    createDocumentFragment: () => new FakeElement(),
    createElement: () => new FakeElement(),
    querySelector: (selector) => {
      const element = elements.get(selector);
      if (!element) {
        throw new Error(`Unexpected selector: ${selector}`);
      }
      return element;
    },
  };

  const intervals = new Map();
  const timeouts = new Map();
  const windowListeners = new Map();
  let nextTimerId = 1;
  const window = {
    devicePixelRatio: 1,
    location: { host: "browser.test", protocol: "http:" },
    addEventListener(eventName, listener) {
      const listeners = windowListeners.get(eventName) ?? [];
      listeners.push(listener);
      windowListeners.set(eventName, listeners);
    },
    clearInterval(timerId) {
      intervals.delete(timerId);
    },
    clearTimeout(timerId) {
      timeouts.delete(timerId);
    },
    setInterval(callback) {
      const timerId = nextTimerId;
      nextTimerId += 1;
      intervals.set(timerId, callback);
      return timerId;
    },
    setTimeout(callback, delay) {
      const timerId = nextTimerId;
      nextTimerId += 1;
      timeouts.set(timerId, { callback, delay });
      return timerId;
    },
  };

  const sockets = [];
  class WebSocket {
    static CLOSED = 3;
    static CONNECTING = 0;
    static OPEN = 1;

    constructor(url) {
      this.closeCalls = 0;
      this.listeners = new Map();
      this.readyState = WebSocket.CONNECTING;
      this.sent = [];
      this.url = url;
      sockets.push(this);
    }

    addEventListener(eventName, listener) {
      const listeners = this.listeners.get(eventName) ?? [];
      listeners.push(listener);
      this.listeners.set(eventName, listeners);
    }

    close() {
      this.closeCalls += 1;
      this.readyState = WebSocket.CLOSED;
    }

    emit(eventName, event = {}) {
      if (eventName === "close") {
        this.readyState = WebSocket.CLOSED;
      }
      if (eventName === "open") {
        this.readyState = WebSocket.OPEN;
      }
      for (const listener of this.listeners.get(eventName) ?? []) {
        listener(event);
      }
    }

    send(message) {
      this.sent.push(message);
    }
  }

  return {
    connectionState: elements.get("#connection-state"),
    document,
    dispatchWindow(eventName) {
      for (const listener of windowListeners.get(eventName) ?? []) {
        listener();
      }
    },
    pendingIntervalCount: () => intervals.size,
    pendingTimeoutCount: () => timeouts.size,
    runNextTimeout() {
      const pending = timeouts.entries().next().value;
      assert.ok(pending, "expected a pending reconnect timeout");
      const [timerId, { callback }] = pending;
      timeouts.delete(timerId);
      callback();
    },
    eventCount: elements.get("#event-count"),
    eventRows: elements.get("#event-rows"),
    sockets,
    trendState: elements.get("#trend-state"),
    WebSocket,
    window,
  };
}

async function loadPage() {
  const browser = createBrowser();
  const context = vm.createContext({
    WebSocket: browser.WebSocket,
    document: browser.document,
    window: browser.window,
  });
  const appUrl = new URL("../public/app.js", import.meta.url);
  const liveWindowUrl = new URL("../public/live_window.mjs", import.meta.url);
  const [appSource, liveWindowSource] = await Promise.all([
    readFile(appUrl, "utf8"),
    readFile(liveWindowUrl, "utf8"),
  ]);
  const liveWindowModule = new vm.SourceTextModule(liveWindowSource, {
    context,
    identifier: liveWindowUrl.href,
  });
  await liveWindowModule.link(() => {
    throw new Error("live_window.mjs has no imports");
  });
  const appModule = new vm.SourceTextModule(appSource, {
    context,
    identifier: appUrl.href,
  });
  await appModule.link((specifier) => {
    assert.equal(specifier, "./live_window.mjs");
    return liveWindowModule;
  });
  await appModule.evaluate();
  return browser;
}

function status(socket, state) {
  socket.emit("message", {
    data: JSON.stringify({ kind: "status", state }),
  });
}

test("reconnects one transient live viewer after close", async () => {
  const page = await loadPage();
  const firstSocket = page.sockets[0];

  firstSocket.emit("close");
  assert.equal(page.connectionState.textContent, "Live viewer disconnected; reconnecting");
  assert.equal(page.connectionState.dataset.state, "disconnected");
  assert.equal(page.pendingTimeoutCount(), 1);

  firstSocket.emit("close");
  assert.equal(page.pendingTimeoutCount(), 1);

  page.runNextTimeout();
  assert.equal(page.sockets.length, 2);
  assert.equal(page.pendingTimeoutCount(), 0);
});

test("ignores status events from a replaced viewer socket", async () => {
  const page = await loadPage();
  const firstSocket = page.sockets[0];

  firstSocket.emit("close");
  page.runNextTimeout();
  const currentSocket = page.sockets[1];
  currentSocket.emit("open");
  status(currentSocket, "active");

  const currentEventCount = page.eventCount.textContent;
  const currentRows = page.eventRows.children.length;
  const currentTrendState = page.trendState.textContent;
  const staleMessage = {
    kind: "message",
    cause_code: 3,
    cause_name: "spontaneous",
    common_address: 1,
    information_object_address: 7,
    quality_flags: [],
    quality_value: 0,
    received_at: "2026-08-26T12:00:00.000Z",
    type_id: "M_ME_NC_1",
    value: 999.99,
    value_text: "999.99",
  };
  firstSocket.emit("message", { data: JSON.stringify(staleMessage) });
  assert.equal(page.pendingTimeoutCount(), 0);
  assert.equal(page.eventCount.textContent, currentEventCount);
  assert.equal(page.eventRows.children.length, currentRows);
  assert.equal(page.trendState.textContent, currentTrendState);

  status(firstSocket, "idle");
  assert.equal(page.connectionState.textContent, "Receiving IEC 104");
  assert.equal(page.connectionState.dataset.state, "active");

  firstSocket.emit("close");
  assert.equal(page.pendingTimeoutCount(), 0);
});

test("beforeunload closes the viewer and prevents reconnect", async () => {
  const page = await loadPage();
  const socket = page.sockets[0];

  socket.emit("open");
  assert.equal(page.pendingIntervalCount(), 1);

  page.dispatchWindow("beforeunload");
  assert.equal(socket.closeCalls, 1);
  assert.equal(page.pendingIntervalCount(), 0);

  socket.emit("close");
  assert.equal(page.pendingTimeoutCount(), 0);
  assert.equal(page.sockets.length, 1);
});

test("beforeunload cancels a pending reconnect", async () => {
  const page = await loadPage();
  const socket = page.sockets[0];

  socket.emit("close");
  assert.equal(page.pendingTimeoutCount(), 1);

  page.dispatchWindow("beforeunload");
  assert.equal(page.pendingTimeoutCount(), 0);
  assert.equal(page.sockets.length, 1);
});