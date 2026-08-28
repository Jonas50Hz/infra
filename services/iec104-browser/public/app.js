import {
  DEFAULT_EVENT_CAP,
  LiveWindow,
  MAX_ANALOG_POINTS,
  MAX_ANALOG_SERIES,
} from "./live_window.mjs";

const RENDER_INTERVAL_MS = 100;
const RECONNECT_INITIAL_DELAY_MS = 500;
const RECONNECT_MAX_DELAY_MS = 5000;

const state = {
  liveWindow: new LiveWindow(DEFAULT_EVENT_CAP),
  socket: null,
  keepaliveTimer: null,
  reconnectAttempts: 0,
  reconnectTimer: null,
  renderTimer: null,
  renderedDetailId: undefined,
  seriesSignature: null,
  destroyed: false,
};

const elements = {
  connectionState: document.querySelector("#connection-state"),
  detailContent: document.querySelector("#detail-content"),
  detailTitle: document.querySelector("#detail-title"),
  eventCap: document.querySelector("#event-cap"),
  eventCount: document.querySelector("#event-count"),
  filter: document.querySelector("#event-filter"),
  rows: document.querySelector("#event-rows"),
  tableState: document.querySelector("#table-state"),
  trendCanvas: document.querySelector("#trend-canvas"),
  trendCount: document.querySelector("#trend-count"),
  trendSeries: document.querySelector("#trend-series"),
  trendState: document.querySelector("#trend-state"),
};

function setConnectionState(text, stateName = "connecting") {
  elements.connectionState.textContent = text;
  elements.connectionState.dataset.state = stateName;
}

function setEmptyDetail() {
  elements.detailTitle.textContent = "No Value Selected";
  elements.detailContent.replaceChildren(
    document.querySelector("#empty-detail-template").content.cloneNode(true),
  );
}

function formatTime(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function qualityText(event) {
  return Array.isArray(event.quality_flags) ? event.quality_flags.join(", ") : "";
}

function filteredEvents() {
  const query = elements.filter.value.trim().toLowerCase();
  if (!query) {
    return state.liveWindow.events;
  }
  return state.liveWindow.events
    .filter(({ event }) => [
      event.common_address,
      event.information_object_address,
      event.type_id,
      event.value_text,
      event.cause_name,
      qualityText(event),
    ].join(" ").toLowerCase().includes(query));
}

function renderRows() {
  const entries = filteredEvents();
  elements.rows.replaceChildren();
  for (const { event, id } of entries) {
    const row = document.createElement("tr");
    row.className = "event-row";
    row.tabIndex = 0;
    row.classList.toggle("is-selected", state.liveWindow.selectedId === id);
    row.addEventListener("click", () => selectEvent(id));
    row.addEventListener("keydown", (keyboardEvent) => {
      if (keyboardEvent.key === "Enter" || keyboardEvent.key === " ") {
        keyboardEvent.preventDefault();
        selectEvent(id);
      }
    });
    row.append(
      cell(formatTime(event.received_at)),
      cell(String(event.common_address)),
      cell(String(event.information_object_address)),
      badgeCell(event.type_id, "type-badge"),
      cell(event.value_text, "value-cell"),
      badgeCell(event.cause_name, "cause-badge"),
      badgeCell(qualityText(event), event.quality_value ? "quality-badge is-alert" : "quality-badge"),
    );
    elements.rows.append(row);
  }
  elements.eventCount.textContent = `Showing ${entries.length} visible of ${state.liveWindow.events.length} retained (newest ${state.liveWindow.eventCap}).`;
  elements.tableState.textContent = entries.length
    ? ""
    : state.liveWindow.events.length
      ? "No received values match this filter."
      : "Waiting for IEC 104 values.";
}

function cell(text, className = "") {
  const item = document.createElement("td");
  item.textContent = text;
  if (className) {
    item.className = className;
  }
  return item;
}

function badgeCell(text, className) {
  const item = document.createElement("td");
  const badge = document.createElement("span");
  badge.className = className;
  badge.textContent = text;
  item.append(badge);
  return item;
}

function addDetailValue(list, label, value) {
  const container = document.createElement("div");
  const term = document.createElement("dt");
  term.textContent = label;
  const definition = document.createElement("dd");
  definition.textContent = value;
  container.append(term, definition);
  list.append(container);
}

function renderDetail(event) {
  elements.detailTitle.textContent = `${event.type_id} / IOA ${event.information_object_address}`;
  const grid = document.createElement("dl");
  grid.className = "detail-grid";
  addDetailValue(grid, "Received", formatTime(event.received_at));
  addDetailValue(grid, "Common Address", String(event.common_address));
  addDetailValue(grid, "Information Object", String(event.information_object_address));
  addDetailValue(grid, "ASDU Type", event.type_id);
  addDetailValue(grid, "Value", event.value_text);
  addDetailValue(grid, "Cause", `${event.cause_name} (${event.cause_code})`);
  addDetailValue(grid, "Quality", `${qualityText(event)} (${event.quality_value})`);
  elements.detailContent.replaceChildren(grid);
}

function renderSelectedDetail() {
  if (state.renderedDetailId === state.liveWindow.selectedId) {
    return;
  }
  state.renderedDetailId = state.liveWindow.selectedId;
  const event = state.liveWindow.selectedEvent;
  if (event) {
    renderDetail(event);
    return;
  }
  setEmptyDetail();
}

function selectEvent(eventId) {
  state.liveWindow.selectEvent(eventId);
  renderRows();
  renderSelectedDetail();
}

function syncTrendSeriesSelector() {
  const series = state.liveWindow.series;
  const signature = series.map((item) => item.key).join(",");
  if (state.seriesSignature !== signature) {
    const options = document.createDocumentFragment();
    if (!series.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No numeric series";
      options.append(option);
    }
    for (const item of series) {
      const option = document.createElement("option");
      option.value = item.key;
      option.textContent = `CA ${item.commonAddress} / IOA ${item.informationObjectAddress}`;
      options.append(option);
    }
    elements.trendSeries.replaceChildren(options);
    state.seriesSignature = signature;
  }
  elements.trendSeries.disabled = !series.length;
  elements.trendSeries.value = state.liveWindow.activeSeriesKey ?? "";
}

function formatTrendValue(value) {
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}

function prepareTrendCanvas() {
  const bounds = elements.trendCanvas.getBoundingClientRect();
  const width = Math.max(1, Math.round(bounds.width));
  const height = Math.max(1, Math.round(bounds.height));
  const scale = Math.min(window.devicePixelRatio || 1, 2);
  const canvasWidth = Math.round(width * scale);
  const canvasHeight = Math.round(height * scale);
  if (
    elements.trendCanvas.width !== canvasWidth
    || elements.trendCanvas.height !== canvasHeight
  ) {
    elements.trendCanvas.width = canvasWidth;
    elements.trendCanvas.height = canvasHeight;
  }
  const context = elements.trendCanvas.getContext("2d");
  if (!context) {
    return null;
  }
  context.setTransform(scale, 0, 0, scale, 0, 0);
  context.clearRect(0, 0, width, height);
  return { context, height, width };
}

function renderTrend() {
  syncTrendSeriesSelector();
  const canvas = prepareTrendCanvas();
  const series = state.liveWindow.activeSeries;
  if (!canvas || !series || !series.points.length) {
    elements.trendCanvas.setAttribute("aria-label", "No numeric IEC 104 M_ME_NC_1 values have been received.");
    elements.trendState.textContent = "No numeric M_ME_NC_1 values have been received.";
    elements.trendCount.textContent = `Tracks up to ${MAX_ANALOG_SERIES} CA/IOA series and ${MAX_ANALOG_POINTS} numeric values per series.`;
    return;
  }

  const points = series.points.filter((point) => Number.isFinite(point.value));
  if (!points.length) {
    elements.trendCanvas.setAttribute("aria-label", "No numeric IEC 104 M_ME_NC_1 values have been received.");
    elements.trendState.textContent = "No numeric M_ME_NC_1 values have been received.";
    return;
  }

  const { context, height, width } = canvas;
  const values = points.map((point) => point.value);
  const lowestValue = Math.min(...values);
  const highestValue = Math.max(...values);
  const valuePadding = highestValue === lowestValue
    ? Math.max(Math.abs(highestValue) * 0.1, 1)
    : (highestValue - lowestValue) * 0.1;
  const minimum = lowestValue - valuePadding;
  const maximum = highestValue + valuePadding;
  const left = 58;
  const right = 20;
  const top = 20;
  const bottom = 30;
  const plotWidth = Math.max(1, width - left - right);
  const plotHeight = Math.max(1, height - top - bottom);
  const yFor = (value) => top + ((maximum - value) / (maximum - minimum)) * plotHeight;

  context.font = "11px Avenir Next, Trebuchet MS, sans-serif";
  context.fillStyle = "#5d6d68";
  context.strokeStyle = "rgba(29, 53, 48, 0.16)";
  context.lineWidth = 1;
  for (let index = 0; index < 4; index += 1) {
    const ratio = index / 3;
    const y = top + ratio * plotHeight;
    const value = maximum - ratio * (maximum - minimum);
    context.beginPath();
    context.moveTo(left, y);
    context.lineTo(width - right, y);
    context.stroke();
    context.fillText(formatTrendValue(value), 4, y + 4);
  }

  context.strokeStyle = "#087c78";
  context.lineWidth = 2;
  context.beginPath();
  for (const [index, point] of points.entries()) {
    const x = points.length === 1
      ? left + plotWidth / 2
      : left + (index / (points.length - 1)) * plotWidth;
    const y = yFor(point.value);
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  }
  context.stroke();

  if (points.length === 1) {
    context.fillStyle = "#087c78";
    context.beginPath();
    context.arc(left + plotWidth / 2, yFor(points[0].value), 4, 0, Math.PI * 2);
    context.fill();
  }

  const seriesLabel = `CA ${series.commonAddress} / IOA ${series.informationObjectAddress}`;
  elements.trendCanvas.setAttribute("aria-label", `${seriesLabel} trend with ${points.length} numeric values.`);
  elements.trendState.textContent = `Showing ${seriesLabel}.`;
  elements.trendCount.textContent = `${points.length} retained numeric values; tracks up to ${MAX_ANALOG_SERIES} series and ${MAX_ANALOG_POINTS} values each.`;
}

function renderLiveView() {
  renderRows();
  renderSelectedDetail();
  renderTrend();
}

function renderNow() {
  if (state.renderTimer !== null) {
    window.clearTimeout(state.renderTimer);
    state.renderTimer = null;
  }
  renderLiveView();
}

function scheduleLiveRender() {
  if (state.renderTimer !== null) {
    return;
  }
  state.renderTimer = window.setTimeout(() => {
    state.renderTimer = null;
    renderLiveView();
  }, RENDER_INTERVAL_MS);
}

function handleStatus(payload) {
  if (payload.state === "active") {
    setConnectionState("Receiving IEC 104", "active");
    return;
  }
  if (payload.state === "idle") {
    setConnectionState("Monitor idle", "idle");
    return;
  }
  setConnectionState("Connecting IEC 104", "connecting");
}

function clearKeepaliveTimer() {
  if (state.keepaliveTimer === null) {
    return;
  }
  window.clearInterval(state.keepaliveTimer);
  state.keepaliveTimer = null;
}

function clearReconnectTimer() {
  if (state.reconnectTimer === null) {
    return;
  }
  window.clearTimeout(state.reconnectTimer);
  state.reconnectTimer = null;
}

function isCurrentSocket(socket) {
  return !state.destroyed && state.socket === socket;
}

function scheduleReconnect() {
  if (state.destroyed || state.socket !== null || state.reconnectTimer !== null) {
    return;
  }
  const delay = Math.min(
    RECONNECT_INITIAL_DELAY_MS * (2 ** state.reconnectAttempts),
    RECONNECT_MAX_DELAY_MS,
  );
  state.reconnectAttempts += 1;
  state.reconnectTimer = window.setTimeout(() => {
    state.reconnectTimer = null;
    connect();
  }, delay);
}

function connect() {
  if (state.destroyed || state.socket !== null) {
    return;
  }
  clearReconnectTimer();
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  let socket;
  try {
    socket = new WebSocket(`${protocol}//${window.location.host}/v1/iec104/live`);
  } catch {
    setConnectionState("Live viewer unavailable; reconnecting", "error");
    scheduleReconnect();
    return;
  }
  state.socket = socket;

  socket.addEventListener("open", () => {
    if (!isCurrentSocket(socket)) {
      return;
    }
    setConnectionState("Connecting IEC 104", "connecting");
    state.reconnectAttempts = 0;
    clearKeepaliveTimer();
    state.keepaliveTimer = window.setInterval(() => {
      if (isCurrentSocket(socket) && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ kind: "keepalive" }));
      }
    }, 5000);
  });

  socket.addEventListener("message", (messageEvent) => {
    if (!isCurrentSocket(socket)) {
      return;
    }
    let payload;
    try {
      payload = JSON.parse(messageEvent.data);
    } catch {
      return;
    }
    if (payload.kind === "status") {
      handleStatus(payload);
      return;
    }
    if (payload.kind === "message") {
      state.liveWindow.ingest(payload);
      scheduleLiveRender();
    }
  });

  socket.addEventListener("close", () => {
    if (!isCurrentSocket(socket)) {
      return;
    }
    clearKeepaliveTimer();
    state.socket = null;
    setConnectionState("Live viewer disconnected; reconnecting", "disconnected");
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    if (!isCurrentSocket(socket)) {
      return;
    }
    setConnectionState("Live viewer unavailable", "error");
  });
}

elements.eventCap.addEventListener("change", () => {
  elements.eventCap.value = String(state.liveWindow.setEventCap(elements.eventCap.value));
  renderNow();
});
elements.filter.addEventListener("input", renderNow);
elements.trendSeries.addEventListener("change", () => {
  if (state.liveWindow.selectSeries(elements.trendSeries.value)) {
    renderTrend();
  }
});
window.addEventListener("resize", scheduleLiveRender);
window.addEventListener("beforeunload", () => {
  state.destroyed = true;
  if (state.renderTimer !== null) {
    window.clearTimeout(state.renderTimer);
    state.renderTimer = null;
  }
  clearReconnectTimer();
  clearKeepaliveTimer();
  const socket = state.socket;
  state.socket = null;
  if (socket) {
    socket.close();
  }
});

elements.eventCap.value = String(state.liveWindow.eventCap);
renderLiveView();
connect();