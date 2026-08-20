# IEC 104 Exporter

`iec104-exporter` is the root-owned, one-way IEC 60870-5-104 controlled
station. It consumes raw-Protobuf `Export` records from Kafka and publishes
only monitor-direction `M_SP_NA_1`, `M_DP_NA_1`, and `M_ME_NC_1` ASDUs to one
active control-center connection on TCP port 2404.

The service does not expose command, interrogation, parameterization, or any
other inbound application-ASDU handler. A TCP ingress guard forwards only IEC
U (`STARTDT`, `STOPDT`, `TESTFR`) and structurally valid S acknowledgement
frames to c104. It closes a connection before a malformed transport frame or
application I-frame can reach c104. A Kafka offset is committed only after c104
accepts the outbound batch; reconnects or restarts can therefore resend a
record.

The host mapping defaults to `127.0.0.1:2404`. Override it with
`IEC104_EXPORTER_HOST_PORT` when that port is occupied.

The exporter permits one control center. `iec104-browser` acquires that slot
only while its live page is open; `iec104-receiver` owns it only during the
profile-gated protocol test.