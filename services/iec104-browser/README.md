# IEC 104 Browser

`iec104-browser` is the root-owned, read-only web control center at
`http://localhost:3003`. Opening the page starts an IEC 104 client with
`STARTDT`; it displays only values received on that IEC connection.

The page keeps its own live history in browser memory. The service broadcasts
values only while a page is connected and stops its IEC 104 client when the last
page closes. It does not consume Kafka, persist messages, send IEC application
ASDUs, issue a general interrogation, or expose mutation routes.

It shares the exporter’s one control-center connection. Close the page before
running the profile-gated `iec104-receiver` test.