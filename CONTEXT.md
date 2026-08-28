# WAMA

WAMA owns an alarm system whose workflow is to detect, notify, display, and acknowledge.

## Alarm Workflow

**Alarm**:
A WAMA condition raised after detection for operator handling. MeasurementSession processing does not raise Alarms. An Alarm is identified by its Alarm Rule and exact MRID; that identity remains stable when the rule is revised. It is distinct from a detection rule, which determines conditions, and a notification, which communicates them.
_Avoid_: Detection rule, notification

**Active Alarm**:
An Alarm whose Alarm Rule condition currently holds for a member after a qualifying live measurement. Newer qualifying measurements silently refresh its current evidence without creating a new Alarm Episode or Alarm Notification. A member removed from an Alarm Rule has its Active Alarm cleared. It is distinct from Alarm Acknowledgement, which records operator recognition, and Alarm Clearance, which occurs when the condition no longer holds.

**Alarm Rule**:
A condition that determines when an Alarm is raised, with an explicit catalog scope, and can dynamically include measurements matching its MRID Selector. A dynamically added member becomes an Active Alarm only after a qualifying live measurement occurs.

**Qualifying Measurement**:
A compatible, valid live measurement whose MCCS timestamp is no more than 30 seconds before its evaluation and that may change an Alarm's state. A stale measurement leaves the Alarm's state unchanged. For an exact MRID, a measurement no newer than the latest evaluated measurement cannot change the Alarm's state.

**Threshold Rule**:
An Alarm Rule that compares a measurement using greater-than, greater-than-or-equal, less-than, or less-than-or-equal.

**Threshold Crossing**:
A Threshold Rule crossing takes immediate effect on one qualifying measurement.

**Alarm Rule Revision**:
A change to an Alarm Rule that may change the rule while retaining the same Alarm Identity. It waits for the next qualifying live measurement before changing an Active Alarm.

**MRID Selector**:
A selector used by an Alarm Rule to identify one or more measurements by their MRIDs, including wildcard patterns.

**Alarm Clearance**:
The automatic clearing of an Alarm when its Alarm Rule condition no longer holds; it is distinct from Alarm Acknowledgement, which records operator recognition.

**Alarm Acknowledgement**:
A record of an operator's recognition of an Alarm. It remains associated with its Alarm Episode through a rule-only revision, including a severity change.

**Alarm Episode**:
A period of activation for an Alarm. A new activation after Alarm Clearance is a new Alarm Episode and requires its own acknowledgement.

**Historical Replay**:
A reconstruction of Alarm state from historical measurements. It may reconstruct state but does not issue Alarm Notifications.

**Alarm Severity**:
The operational urgency assigned to an Alarm by its Alarm Rule. In v1, severity is `warning` or `critical`.

**Alarm Notification**:
A communication issued only when an Alarm becomes active. It is not issued for automatic clearance, acknowledgement, or a rule-only update.

## Frequency Capture

**Frequency Capture Episode**:
A bounded period that begins when a source frequency meets a configured capture condition and ends when its clearance condition is met. It is distinct from an Alarm Episode and a MeasurementSession.
_Avoid_: Frequency alarm
