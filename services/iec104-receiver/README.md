# IEC 104 Receiver

`iec104-receiver` is a profile-gated, one-shot test control center. It connects
to `iec104-exporter`, uses c104 `Init.NONE` to send only `STARTDT`, registers
three unique monitor points, publishes matching raw-Protobuf fixture records to
Kafka, and verifies the received type, COT, information-object address, quality,
and value. It then verifies that a raw general-interrogation I-frame is closed
without an application response.

Its telemetry flow never sends an IEC command or general interrogation; the raw
general-interrogation frame is a deliberate negative test after telemetry is
complete. Run it through
[`../../scripts/test-iec104-export.sh`](../../scripts/test-iec104-export.sh).

For the browser integration test, `IEC104_RECEIVER_MODE=publish-only` emits the
same unique Kafka fixtures without opening a second IEC control-center
connection. This mode is test-only.