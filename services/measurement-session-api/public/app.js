const state = {
  sessionId: createSessionId(),
};

const elements = {
  captureReason: document.querySelector("#capture-reason"),
  endedAt: document.querySelector("#ended-at"),
  form: document.querySelector("#session-form"),
  formStatus: document.querySelector("#form-status"),
  mridCount: document.querySelector("#mrid-count"),
  mrids: document.querySelector("#mrids"),
  rangeReadout: document.querySelector("#range-readout"),
  resultContent: document.querySelector("#result-content"),
  resultTitle: document.querySelector("#result-title"),
  serviceState: document.querySelector("#service-state"),
  startedAt: document.querySelector("#started-at"),
  submitButton: document.querySelector("#submit-button"),
};

function createSessionId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseGrafanaTime(value, fallback) {
  if (!value) {
    return fallback;
  }
  const timestamp = /^\d+$/.test(value) ? Number(value) : Date.parse(value);
  const parsed = new Date(timestamp);
  return Number.isNaN(parsed.getTime()) ? fallback : parsed;
}

function toLocalInputValue(date) {
  const offsetMilliseconds = date.getTimezoneOffset() * 60_000;
  return new Date(date.getTime() - offsetMilliseconds).toISOString().slice(0, 19);
}

function readMrids() {
  return [...new Set(elements.mrids.value.split(/[\n,]+/).map((value) => value.trim()).filter(Boolean))]
    .sort();
}

function updateSelectionSummary() {
  const start = new Date(elements.startedAt.value);
  const end = new Date(elements.endedAt.value);
  const validRange = !Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime());
  elements.rangeReadout.textContent = validRange
    ? `${start.toISOString()} to ${end.toISOString()} UTC`
    : "Choose a start and end time.";
  const count = readMrids().length;
  elements.mridCount.textContent = count === 1 ? "1 measurement ID selected" : `${count} measurement IDs selected`;
}

function setServiceState(text, stateName) {
  elements.serviceState.textContent = text;
  elements.serviceState.dataset.state = stateName;
}

function setFormStatus(text, stateName = "") {
  elements.formStatus.textContent = text;
  elements.formStatus.dataset.state = stateName;
}

function addResultField(container, label, value) {
  const row = document.createElement("div");
  const term = document.createElement("dt");
  const definition = document.createElement("dd");
  term.textContent = label;
  definition.textContent = value;
  row.append(term, definition);
  container.append(row);
}

function renderResult(result) {
  const details = document.createElement("dl");
  details.className = "result-grid";
  addResultField(details, "Session ID", result.session_id);
  addResultField(details, "Artifact ID", result.blob_id);
  addResultField(details, "Submitted UTC", new Date(result.requested_at).toISOString());

  const link = document.createElement("a");
  link.className = "session-link";
  link.href = result.session_dashboard_url;
  link.target = "_blank";
  link.rel = "noreferrer";
  link.textContent = "Open session dashboard";

  elements.resultTitle.textContent = "Submitted";
  elements.resultContent.replaceChildren(details, link);
}

function initializeFromQuery() {
  const query = new URLSearchParams(window.location.search);
  const now = new Date();
  const from = parseGrafanaTime(query.get("from"), new Date(now.getTime() - 15 * 60_000));
  const to = parseGrafanaTime(query.get("to"), now);
  elements.startedAt.value = toLocalInputValue(from);
  elements.endedAt.value = toLocalInputValue(to);
  const mrids = (query.get("mrids") || "").split(",").map((value) => value.trim()).filter(Boolean);
  elements.mrids.value = [...new Set(mrids)].sort().join("\n");
  elements.captureReason.value = "Grafana selection";
  updateSelectionSummary();
}

async function submitSelection(event) {
  event.preventDefault();
  const start = new Date(elements.startedAt.value);
  const end = new Date(elements.endedAt.value);
  const mrids = readMrids();
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    setFormStatus("Start and end are required.", "error");
    return;
  }
  if (mrids.length === 0) {
    setFormStatus("Select at least one measurement ID.", "error");
    return;
  }

  elements.submitButton.disabled = true;
  setServiceState("Submitting request", "pending");
  setFormStatus("Publishing immutable request.");
  try {
    const response = await fetch("/v1/measurement-sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: state.sessionId,
        started_at: start.toISOString(),
        ended_at: end.toISOString(),
        mrids,
        capture_reason: elements.captureReason.value.trim() || null,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(typeof payload.detail === "string" ? payload.detail : "Request was rejected.");
    }
    renderResult(payload);
    setServiceState("Request submitted", "success");
    setFormStatus("The selected interval is now queued for materialization.", "success");
  } catch (error) {
    setServiceState("Submission unavailable", "error");
    setFormStatus(error instanceof Error ? error.message : "Request could not be submitted.", "error");
    elements.submitButton.disabled = false;
  }
}

elements.form.addEventListener("submit", submitSelection);
elements.startedAt.addEventListener("input", updateSelectionSummary);
elements.endedAt.addEventListener("input", updateSelectionSummary);
elements.mrids.addEventListener("input", updateSelectionSummary);

initializeFromQuery();