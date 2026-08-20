const state = {
  events: [],
  selectedIndex: null,
  socket: null,
  keepaliveTimer: null,
};

const elements = {
  connectionState: document.querySelector("#connection-state"),
  detailContent: document.querySelector("#detail-content"),
  detailTitle: document.querySelector("#detail-title"),
  eventCount: document.querySelector("#event-count"),
  filter: document.querySelector("#event-filter"),
  rows: document.querySelector("#event-rows"),
  tableState: document.querySelector("#table-state"),
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
  return event.quality_flags.join(", ");
}

function filteredEvents() {
  const query = elements.filter.value.trim().toLowerCase();
  if (!query) {
    return state.events.map((event, index) => ({ event, index }));
  }
  return state.events
    .map((event, index) => ({ event, index }))
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
  for (const { event, index } of entries) {
    const row = document.createElement("tr");
    row.className = "event-row";
    row.tabIndex = 0;
    row.classList.toggle("is-selected", state.selectedIndex === index);
    row.addEventListener("click", () => selectEvent(index));
    row.addEventListener("keydown", (keyboardEvent) => {
      if (keyboardEvent.key === "Enter" || keyboardEvent.key === " ") {
        keyboardEvent.preventDefault();
        selectEvent(index);
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
  elements.eventCount.textContent = `${entries.length} received in this view`;
  elements.tableState.textContent = entries.length
    ? ""
    : state.events.length
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

function selectEvent(index) {
  state.selectedIndex = index;
  renderRows();
  const event = state.events[index];
  if (event) {
    renderDetail(event);
  }
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

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(`${protocol}//${window.location.host}/v1/iec104/live`);
  state.socket = socket;

  socket.addEventListener("open", () => {
    setConnectionState("Connecting IEC 104", "connecting");
    state.keepaliveTimer = window.setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ kind: "keepalive" }));
      }
    }, 5000);
  });

  socket.addEventListener("message", (messageEvent) => {
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
      state.events.unshift(payload);
      renderRows();
    }
  });

  socket.addEventListener("close", () => {
    if (state.keepaliveTimer !== null) {
      window.clearInterval(state.keepaliveTimer);
      state.keepaliveTimer = null;
    }
    if (state.socket === socket) {
      setConnectionState("Monitor disconnected", "disconnected");
    }
  });

  socket.addEventListener("error", () => {
    setConnectionState("Monitor unavailable", "error");
  });
}

elements.filter.addEventListener("input", renderRows);
window.addEventListener("beforeunload", () => {
  if (state.socket && state.socket.readyState === WebSocket.OPEN) {
    state.socket.close();
  }
});

setEmptyDetail();
renderRows();
connect();