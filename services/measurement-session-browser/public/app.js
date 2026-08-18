const state = {
  nextCursor: null,
  selectedSessionId: null,
  sessions: [],
};

const elements = {
  catalogState: document.querySelector("#catalog-state"),
  detailContent: document.querySelector("#detail-content"),
  detailTitle: document.querySelector("#detail-title"),
  filter: document.querySelector("#session-filter"),
  loadMore: document.querySelector("#load-more"),
  resultCount: document.querySelector("#result-count"),
  rows: document.querySelector("#session-rows"),
  tableState: document.querySelector("#table-state"),
};

function setCatalogState(text, isError = false) {
  elements.catalogState.textContent = text;
  elements.catalogState.classList.toggle("is-error", isError);
}

function setEmptyDetail() {
  elements.detailTitle.textContent = "No Session Selected";
  elements.detailContent.replaceChildren(
    document.querySelector("#empty-detail-template").content.cloneNode(true),
  );
}

function formatTime(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function formatBytes(value) {
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KiB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

function filteredSessions() {
  const query = elements.filter.value.trim().toLowerCase();
  if (!query) {
    return state.sessions;
  }
  return state.sessions.filter((session) => (
    session.session_id.toLowerCase().includes(query)
    || session.source_mrid.toLowerCase().includes(query)
  ));
}

function renderRows() {
  const sessions = filteredSessions();
  elements.rows.replaceChildren();
  for (const session of sessions) {
    const row = document.createElement("tr");
    row.className = "session-row";
    row.tabIndex = 0;
    row.classList.toggle("is-selected", state.selectedSessionId === session.session_id);
    row.addEventListener("click", () => selectSession(session.session_id));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectSession(session.session_id);
      }
    });
    const finalized = document.createElement("td");
    finalized.textContent = formatTime(session.finalized_at);
    const source = document.createElement("td");
    const sourcePrimary = document.createElement("span");
    sourcePrimary.className = "cell-primary";
    sourcePrimary.textContent = session.source_mrid;
    const sourceSecondary = document.createElement("span");
    sourceSecondary.className = "cell-secondary";
    sourceSecondary.textContent = session.session_id;
    source.append(sourcePrimary, sourceSecondary);
    const measurements = document.createElement("td");
    measurements.textContent = String(session.measurement_count);
    const artifacts = document.createElement("td");
    artifacts.textContent = String(session.artifact_count);
    row.append(finalized, source, measurements, artifacts);
    elements.rows.append(row);
  }
  elements.tableState.textContent = sessions.length ? "" : "No finalized sessions match this filter.";
  elements.resultCount.textContent = `${sessions.length} shown`;
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

function renderDetail(session) {
  elements.detailTitle.textContent = session.source_mrid;
  const grid = document.createElement("dl");
  grid.className = "detail-grid";
  addDetailValue(grid, "Session ID", session.session_id);
  addDetailValue(grid, "Started", formatTime(session.started_at));
  addDetailValue(grid, "Ended", formatTime(session.ended_at));
  addDetailValue(grid, "Finalized", formatTime(session.finalized_at));
  addDetailValue(grid, "Measurements", String(session.measurement_count));

  const metadataSection = document.createElement("section");
  metadataSection.className = "metadata-section";
  const metadataHeading = document.createElement("h3");
  metadataHeading.textContent = "Context";
  const metadata = document.createElement("ul");
  metadata.className = "metadata-list";
  for (const entry of session.metadata) {
    const item = document.createElement("li");
    const key = document.createElement("span");
    key.textContent = entry.key;
    const value = document.createElement("span");
    value.textContent = entry.value;
    item.append(key, value);
    metadata.append(item);
  }
  metadataSection.append(metadataHeading, metadata);

  const artifactSection = document.createElement("section");
  artifactSection.className = "artifact-section";
  const artifactHeading = document.createElement("h3");
  artifactHeading.textContent = "Artifacts";
  const artifacts = document.createElement("ul");
  artifacts.className = "artifact-list";
  for (const artifact of session.artifacts) {
    const item = document.createElement("li");
    const download = document.createElement("a");
    download.href = `/api${artifact.download_url}`;
    download.download = typeof artifact.download_name === "string"
      ? artifact.download_name
      : artifact.id;
    const name = document.createElement("span");
    name.textContent = artifact.id;
    const size = document.createElement("small");
    size.textContent = `${formatBytes(artifact.size_bytes)} | Download`;
    download.append(name, size);
    item.append(download);
    artifacts.append(item);
  }
  artifactSection.append(artifactHeading, artifacts);
  elements.detailContent.replaceChildren(grid, metadataSection, artifactSection);
}

async function selectSession(sessionId) {
  state.selectedSessionId = sessionId;
  renderRows();
  elements.detailTitle.textContent = "Loading Session";
  elements.detailContent.replaceChildren();
  try {
    const response = await fetch(`/api/v1/measurement-sessions/${encodeURIComponent(sessionId)}`);
    if (!response.ok) {
      throw new Error("Session detail request failed");
    }
    renderDetail(await response.json());
  } catch (error) {
    elements.detailTitle.textContent = "Session Unavailable";
    const message = document.createElement("p");
    message.className = "empty-detail";
    message.textContent = "The selected session could not be verified.";
    elements.detailContent.replaceChildren(message);
  }
}

async function loadSessions(cursor = null) {
  elements.tableState.textContent = "Loading finalized sessions...";
  try {
    const query = new URLSearchParams({ limit: "25" });
    if (cursor) {
      query.set("cursor", cursor);
    }
    const response = await fetch(`/api/v1/measurement-sessions?${query}`);
    if (!response.ok) {
      throw new Error("Catalog request failed");
    }
    const payload = await response.json();
    state.sessions.push(...payload.items);
    state.nextCursor = payload.next_cursor;
    elements.loadMore.hidden = !state.nextCursor;
    setCatalogState("Catalog");
    renderRows();
  } catch (error) {
    setCatalogState("Unavailable", true);
    elements.tableState.textContent = "The session catalog is unavailable.";
  }
}

elements.filter.addEventListener("input", renderRows);
elements.loadMore.addEventListener("click", () => loadSessions(state.nextCursor));

setEmptyDetail();
loadSessions();