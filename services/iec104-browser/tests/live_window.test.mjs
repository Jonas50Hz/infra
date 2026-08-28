import assert from "node:assert/strict";
import test from "node:test";

import {
  EVENT_CAP_OPTIONS,
  LiveWindow,
  MAX_ANALOG_POINTS,
  MAX_ANALOG_SERIES,
} from "../public/live_window.mjs";

function message(sequence, seriesIndex = sequence % (MAX_ANALOG_SERIES + 5)) {
  return {
    common_address: seriesIndex,
    information_object_address: 100 + seriesIndex,
    received_at: `2026-08-26T12:00:${String(sequence % 60).padStart(2, "0")}.000Z`,
    sequence,
    type_id: "M_ME_NC_1",
    value: sequence / 10,
    quality_flags: ["valid"],
    quality_value: 0,
  };
}

test("keeps newest events and finite analog series bounded during sustained ingress", () => {
  const window = new LiveWindow(EVENT_CAP_OPTIONS[0]);
  const selected = window.ingest(message(0));
  window.selectEvent(selected.id);

  for (let sequence = 1; sequence < 10_000; sequence += 1) {
    window.ingest(message(sequence));
  }
  window.ingest({ ...message(10_000), value: Number.NaN });
  window.ingest({ ...message(10_001), value: Number.POSITIVE_INFINITY });
  window.ingest({ ...message(10_002), value: "50.01" });
  window.ingest({ ...message(10_003), value: false });
  window.ingest({ ...message(10_004), quality_flags: ["invalid"], quality_value: 128 });

  assert.equal(window.events.length, EVENT_CAP_OPTIONS[0]);
  assert.equal(window.events[0].event.sequence, 10_004);
  assert.equal(window.events.at(-1).event.sequence, 9_973);
  assert.equal(window.selectedId, null);
  assert.equal(window.selectedEvent, null);

  assert.equal(window.series.length, MAX_ANALOG_SERIES);
  for (const series of window.series) {
    assert.ok(series.points.length <= MAX_ANALOG_POINTS);
    assert.ok(series.points.every((point) => Number.isFinite(point.value)));
  }
  assert.ok(window.series.some((series) => series.points.length === MAX_ANALOG_POINTS));
  assert.ok(window.series.every((series) => (
    !series.points.some((point) => point.value === 1000.4)
  )));

  const selectedSeries = window.series.at(-1);
  assert.equal(window.selectSeries(selectedSeries.key), true);
  assert.equal(window.activeSeries?.key, selectedSeries.key);
  assert.equal(window.selectSeries("not-a-series"), false);
});