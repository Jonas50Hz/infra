# IEC 104 Browser

`iec104-browser` is the root-owned, read-only web control center at
`http://localhost:3003`. Application startup opens one persistent IEC 104
client with `STARTDT`; it displays only values received on that connection.

The page keeps a bounded newest-message window in browser memory: the default
is 128 messages, with a selectable 32, 64, 128, or 256-message window. The
256-message maximum matches the server queue; a selected value clears when it
falls out of the browser window. The top trend displays only finite numeric
`M_ME_NC_1` values for one selected CA/IOA series at a time. It stores at most
eight series and 128 points per series; non-finite and IEC-quality-invalid
values do not enter the trend.

The service retains the IEC 104 connection with zero UI viewers and broadcasts
values only to connected pages. It does not consume Kafka, persist messages,
send IEC application ASDUs, issue a general interrogation, or expose mutation
routes. Human browser clients are viewer-only.

It shares the exporter’s one control-center connection. Stop the browser before
running the profile-gated `iec104-receiver` test; the receiver workflow must
own that sole control-center slot.