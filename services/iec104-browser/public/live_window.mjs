export const EVENT_CAP_OPTIONS = Object.freeze([32, 64, 128, 256]);
export const DEFAULT_EVENT_CAP = 128;
export const MAX_EVENT_CAP = EVENT_CAP_OPTIONS.at(-1);
export const MAX_ANALOG_SERIES = 8;
export const MAX_ANALOG_POINTS = 128;

function eventCapFor(value) {
  const cap = Number(value);
  return EVENT_CAP_OPTIONS.includes(cap) ? cap : DEFAULT_EVENT_CAP;
}

function analogSeriesKey(event) {
  if (
    event.type_id !== "M_ME_NC_1"
    || !Number.isSafeInteger(event.common_address)
    || !Number.isSafeInteger(event.information_object_address)
    || !Number.isFinite(event.value)
    || event.quality_flags?.includes("invalid")
    || (Number.isInteger(event.quality_value) && (event.quality_value & 128) !== 0)
  ) {
    return null;
  }
  return `${event.common_address}:${event.information_object_address}`;
}

function analogPoint(event) {
  const key = analogSeriesKey(event);
  if (key === null) {
    return null;
  }
  return {
    key,
    commonAddress: event.common_address,
    informationObjectAddress: event.information_object_address,
    receivedAt: event.received_at,
    value: event.value,
  };
}

/**
 * Keep the monitor's browser-local message and analog-chart windows bounded.
 */
export class LiveWindow {
  constructor(eventCap = DEFAULT_EVENT_CAP) {
    this.eventCap = eventCapFor(eventCap);
    this.events = [];
    this.selectedId = null;
    this.series = [];
    this.activeSeriesKey = null;
    this.nextEventId = 0;
  }

  ingest(event) {
    const record = {
      id: `event-${this.nextEventId}`,
      event,
    };
    this.nextEventId += 1;
    this.events.unshift(record);
    const evicted = this.events.splice(this.eventCap);
    if (evicted.some((item) => item.id === this.selectedId)) {
      this.selectedId = null;
    }
    this.addAnalogPoint(event);
    return record;
  }

  setEventCap(eventCap) {
    this.eventCap = eventCapFor(eventCap);
    const evicted = this.events.splice(this.eventCap);
    if (evicted.some((item) => item.id === this.selectedId)) {
      this.selectedId = null;
    }
    return this.eventCap;
  }

  selectEvent(eventId) {
    this.selectedId = this.events.some((item) => item.id === eventId)
      ? eventId
      : null;
    return this.selectedEvent;
  }

  get selectedEvent() {
    return this.events.find((item) => item.id === this.selectedId)?.event ?? null;
  }

  selectSeries(seriesKey) {
    if (!this.series.some((item) => item.key === seriesKey)) {
      return false;
    }
    this.activeSeriesKey = seriesKey;
    return true;
  }

  get activeSeries() {
    return this.series.find((item) => item.key === this.activeSeriesKey) ?? null;
  }

  addAnalogPoint(event) {
    const point = analogPoint(event);
    if (point === null) {
      return;
    }

    let series = this.series.find((item) => item.key === point.key);
    if (!series) {
      if (this.series.length >= MAX_ANALOG_SERIES) {
        return;
      }
      series = {
        key: point.key,
        commonAddress: point.commonAddress,
        informationObjectAddress: point.informationObjectAddress,
        points: [],
      };
      this.series.push(series);
      if (this.activeSeriesKey === null) {
        this.activeSeriesKey = series.key;
      }
    }

    series.points.push({ receivedAt: point.receivedAt, value: point.value });
    if (series.points.length > MAX_ANALOG_POINTS) {
      series.points.splice(0, series.points.length - MAX_ANALOG_POINTS);
    }
  }
}