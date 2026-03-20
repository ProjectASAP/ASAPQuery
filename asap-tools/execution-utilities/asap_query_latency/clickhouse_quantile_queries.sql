-- T000: quantile window ending at 2013-07-01 20:00:00
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 19:50:00' AND '2013-07-01 20:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T001: quantile window ending at 2013-07-01 20:13:16
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 20:03:16' AND '2013-07-01 20:13:16' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T002: quantile window ending at 2013-07-01 20:26:32
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 20:16:32' AND '2013-07-01 20:26:32' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T003: quantile window ending at 2013-07-01 20:39:48
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 20:29:48' AND '2013-07-01 20:39:48' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T004: quantile window ending at 2013-07-01 20:53:04
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 20:43:04' AND '2013-07-01 20:53:04' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T005: quantile window ending at 2013-07-01 21:06:20
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 20:56:20' AND '2013-07-01 21:06:20' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T006: quantile window ending at 2013-07-01 21:19:36
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 21:09:36' AND '2013-07-01 21:19:36' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T007: quantile window ending at 2013-07-01 21:32:52
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 21:22:52' AND '2013-07-01 21:32:52' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T008: quantile window ending at 2013-07-01 21:46:08
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 21:36:08' AND '2013-07-01 21:46:08' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T009: quantile window ending at 2013-07-01 21:59:24
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 21:49:24' AND '2013-07-01 21:59:24' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T010: quantile window ending at 2013-07-01 22:12:40
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 22:02:40' AND '2013-07-01 22:12:40' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T011: quantile window ending at 2013-07-01 22:25:56
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 22:15:56' AND '2013-07-01 22:25:56' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T012: quantile window ending at 2013-07-01 22:39:12
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 22:29:12' AND '2013-07-01 22:39:12' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T013: quantile window ending at 2013-07-01 22:52:28
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 22:42:28' AND '2013-07-01 22:52:28' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T014: quantile window ending at 2013-07-01 23:05:44
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 22:55:44' AND '2013-07-01 23:05:44' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T015: quantile window ending at 2013-07-01 23:19:00
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 23:09:00' AND '2013-07-01 23:19:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T016: quantile window ending at 2013-07-01 23:32:16
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 23:22:16' AND '2013-07-01 23:32:16' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T017: quantile window ending at 2013-07-01 23:45:32
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 23:35:32' AND '2013-07-01 23:45:32' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T018: quantile window ending at 2013-07-01 23:58:48
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-01 23:48:48' AND '2013-07-01 23:58:48' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T019: quantile window ending at 2013-07-02 00:12:04
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 00:02:04' AND '2013-07-02 00:12:04' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T020: quantile window ending at 2013-07-02 00:25:20
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 00:15:20' AND '2013-07-02 00:25:20' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T021: quantile window ending at 2013-07-02 00:38:36
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 00:28:36' AND '2013-07-02 00:38:36' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T022: quantile window ending at 2013-07-02 00:51:52
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 00:41:52' AND '2013-07-02 00:51:52' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T023: quantile window ending at 2013-07-02 01:05:08
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 00:55:08' AND '2013-07-02 01:05:08' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T024: quantile window ending at 2013-07-02 01:18:24
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 01:08:24' AND '2013-07-02 01:18:24' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T025: quantile window ending at 2013-07-02 01:31:40
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 01:21:40' AND '2013-07-02 01:31:40' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T026: quantile window ending at 2013-07-02 01:44:56
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 01:34:56' AND '2013-07-02 01:44:56' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T027: quantile window ending at 2013-07-02 01:58:12
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 01:48:12' AND '2013-07-02 01:58:12' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T028: quantile window ending at 2013-07-02 02:11:28
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 02:01:28' AND '2013-07-02 02:11:28' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T029: quantile window ending at 2013-07-02 02:24:45
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 02:14:45' AND '2013-07-02 02:24:45' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T030: quantile window ending at 2013-07-02 02:38:01
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 02:28:01' AND '2013-07-02 02:38:01' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T031: quantile window ending at 2013-07-02 02:51:17
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 02:41:17' AND '2013-07-02 02:51:17' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T032: quantile window ending at 2013-07-02 03:04:33
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 02:54:33' AND '2013-07-02 03:04:33' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T033: quantile window ending at 2013-07-02 03:17:49
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 03:07:49' AND '2013-07-02 03:17:49' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T034: quantile window ending at 2013-07-02 03:31:05
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 03:21:05' AND '2013-07-02 03:31:05' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T035: quantile window ending at 2013-07-02 03:44:21
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 03:34:21' AND '2013-07-02 03:44:21' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T036: quantile window ending at 2013-07-02 03:57:37
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 03:47:37' AND '2013-07-02 03:57:37' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T037: quantile window ending at 2013-07-02 04:10:53
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 04:00:53' AND '2013-07-02 04:10:53' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T038: quantile window ending at 2013-07-02 04:24:09
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 04:14:09' AND '2013-07-02 04:24:09' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T039: quantile window ending at 2013-07-02 04:37:25
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 04:27:25' AND '2013-07-02 04:37:25' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T040: quantile window ending at 2013-07-02 04:50:41
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 04:40:41' AND '2013-07-02 04:50:41' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T041: quantile window ending at 2013-07-02 05:03:57
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 04:53:57' AND '2013-07-02 05:03:57' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T042: quantile window ending at 2013-07-02 05:17:13
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 05:07:13' AND '2013-07-02 05:17:13' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T043: quantile window ending at 2013-07-02 05:30:29
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 05:20:29' AND '2013-07-02 05:30:29' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T044: quantile window ending at 2013-07-02 05:43:45
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 05:33:45' AND '2013-07-02 05:43:45' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T045: quantile window ending at 2013-07-02 05:57:01
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 05:47:01' AND '2013-07-02 05:57:01' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T046: quantile window ending at 2013-07-02 06:10:17
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 06:00:17' AND '2013-07-02 06:10:17' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T047: quantile window ending at 2013-07-02 06:23:33
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 06:13:33' AND '2013-07-02 06:23:33' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T048: quantile window ending at 2013-07-02 06:36:49
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 06:26:49' AND '2013-07-02 06:36:49' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T049: quantile window ending at 2013-07-02 06:50:05
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 06:40:05' AND '2013-07-02 06:50:05' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T050: quantile window ending at 2013-07-02 07:03:21
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 06:53:21' AND '2013-07-02 07:03:21' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T051: quantile window ending at 2013-07-02 07:16:37
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 07:06:37' AND '2013-07-02 07:16:37' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T052: quantile window ending at 2013-07-02 07:29:53
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 07:19:53' AND '2013-07-02 07:29:53' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T053: quantile window ending at 2013-07-02 07:43:09
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 07:33:09' AND '2013-07-02 07:43:09' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T054: quantile window ending at 2013-07-02 07:56:25
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 07:46:25' AND '2013-07-02 07:56:25' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T055: quantile window ending at 2013-07-02 08:09:41
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 07:59:41' AND '2013-07-02 08:09:41' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T056: quantile window ending at 2013-07-02 08:22:57
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 08:12:57' AND '2013-07-02 08:22:57' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T057: quantile window ending at 2013-07-02 08:36:13
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 08:26:13' AND '2013-07-02 08:36:13' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T058: quantile window ending at 2013-07-02 08:49:30
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 08:39:30' AND '2013-07-02 08:49:30' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T059: quantile window ending at 2013-07-02 09:02:46
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 08:52:46' AND '2013-07-02 09:02:46' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T060: quantile window ending at 2013-07-02 09:16:02
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 09:06:02' AND '2013-07-02 09:16:02' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T061: quantile window ending at 2013-07-02 09:29:18
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 09:19:18' AND '2013-07-02 09:29:18' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T062: quantile window ending at 2013-07-02 09:42:34
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 09:32:34' AND '2013-07-02 09:42:34' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T063: quantile window ending at 2013-07-02 09:55:50
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 09:45:50' AND '2013-07-02 09:55:50' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T064: quantile window ending at 2013-07-02 10:09:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 09:59:06' AND '2013-07-02 10:09:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T065: quantile window ending at 2013-07-02 10:22:22
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 10:12:22' AND '2013-07-02 10:22:22' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T066: quantile window ending at 2013-07-02 10:35:38
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 10:25:38' AND '2013-07-02 10:35:38' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T067: quantile window ending at 2013-07-02 10:48:54
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 10:38:54' AND '2013-07-02 10:48:54' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T068: quantile window ending at 2013-07-02 11:02:10
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 10:52:10' AND '2013-07-02 11:02:10' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T069: quantile window ending at 2013-07-02 11:15:26
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 11:05:26' AND '2013-07-02 11:15:26' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T070: quantile window ending at 2013-07-02 11:28:42
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 11:18:42' AND '2013-07-02 11:28:42' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T071: quantile window ending at 2013-07-02 11:41:58
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 11:31:58' AND '2013-07-02 11:41:58' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T072: quantile window ending at 2013-07-02 11:55:14
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 11:45:14' AND '2013-07-02 11:55:14' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T073: quantile window ending at 2013-07-02 12:08:30
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 11:58:30' AND '2013-07-02 12:08:30' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T074: quantile window ending at 2013-07-02 12:21:46
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 12:11:46' AND '2013-07-02 12:21:46' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T075: quantile window ending at 2013-07-02 12:35:02
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 12:25:02' AND '2013-07-02 12:35:02' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T076: quantile window ending at 2013-07-02 12:48:18
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 12:38:18' AND '2013-07-02 12:48:18' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T077: quantile window ending at 2013-07-02 13:01:34
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 12:51:34' AND '2013-07-02 13:01:34' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T078: quantile window ending at 2013-07-02 13:14:50
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 13:04:50' AND '2013-07-02 13:14:50' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T079: quantile window ending at 2013-07-02 13:28:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 13:18:06' AND '2013-07-02 13:28:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T080: quantile window ending at 2013-07-02 13:41:22
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 13:31:22' AND '2013-07-02 13:41:22' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T081: quantile window ending at 2013-07-02 13:54:38
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 13:44:38' AND '2013-07-02 13:54:38' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T082: quantile window ending at 2013-07-02 14:07:54
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 13:57:54' AND '2013-07-02 14:07:54' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T083: quantile window ending at 2013-07-02 14:21:10
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 14:11:10' AND '2013-07-02 14:21:10' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T084: quantile window ending at 2013-07-02 14:34:26
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 14:24:26' AND '2013-07-02 14:34:26' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T085: quantile window ending at 2013-07-02 14:47:42
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 14:37:42' AND '2013-07-02 14:47:42' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T086: quantile window ending at 2013-07-02 15:00:59
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 14:50:59' AND '2013-07-02 15:00:59' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T087: quantile window ending at 2013-07-02 15:14:15
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 15:04:15' AND '2013-07-02 15:14:15' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T088: quantile window ending at 2013-07-02 15:27:31
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 15:17:31' AND '2013-07-02 15:27:31' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T089: quantile window ending at 2013-07-02 15:40:47
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 15:30:47' AND '2013-07-02 15:40:47' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T090: quantile window ending at 2013-07-02 15:54:03
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 15:44:03' AND '2013-07-02 15:54:03' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T091: quantile window ending at 2013-07-02 16:07:19
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 15:57:19' AND '2013-07-02 16:07:19' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T092: quantile window ending at 2013-07-02 16:20:35
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 16:10:35' AND '2013-07-02 16:20:35' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T093: quantile window ending at 2013-07-02 16:33:51
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 16:23:51' AND '2013-07-02 16:33:51' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T094: quantile window ending at 2013-07-02 16:47:07
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 16:37:07' AND '2013-07-02 16:47:07' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T095: quantile window ending at 2013-07-02 17:00:23
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 16:50:23' AND '2013-07-02 17:00:23' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T096: quantile window ending at 2013-07-02 17:13:39
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 17:03:39' AND '2013-07-02 17:13:39' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T097: quantile window ending at 2013-07-02 17:26:55
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 17:16:55' AND '2013-07-02 17:26:55' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T098: quantile window ending at 2013-07-02 17:40:11
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 17:30:11' AND '2013-07-02 17:40:11' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T099: quantile window ending at 2013-07-02 17:53:27
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 17:43:27' AND '2013-07-02 17:53:27' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T100: quantile window ending at 2013-07-02 18:06:43
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 17:56:43' AND '2013-07-02 18:06:43' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T101: quantile window ending at 2013-07-02 18:19:59
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 18:09:59' AND '2013-07-02 18:19:59' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T102: quantile window ending at 2013-07-02 18:33:15
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 18:23:15' AND '2013-07-02 18:33:15' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T103: quantile window ending at 2013-07-02 18:46:31
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 18:36:31' AND '2013-07-02 18:46:31' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T104: quantile window ending at 2013-07-02 18:59:47
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 18:49:47' AND '2013-07-02 18:59:47' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T105: quantile window ending at 2013-07-02 19:13:03
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 19:03:03' AND '2013-07-02 19:13:03' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T106: quantile window ending at 2013-07-02 19:26:19
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 19:16:19' AND '2013-07-02 19:26:19' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T107: quantile window ending at 2013-07-02 19:39:35
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 19:29:35' AND '2013-07-02 19:39:35' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T108: quantile window ending at 2013-07-02 19:52:51
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 19:42:51' AND '2013-07-02 19:52:51' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T109: quantile window ending at 2013-07-02 20:06:07
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 19:56:07' AND '2013-07-02 20:06:07' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T110: quantile window ending at 2013-07-02 20:19:23
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 20:09:23' AND '2013-07-02 20:19:23' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T111: quantile window ending at 2013-07-02 20:32:39
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 20:22:39' AND '2013-07-02 20:32:39' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T112: quantile window ending at 2013-07-02 20:45:55
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 20:35:55' AND '2013-07-02 20:45:55' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T113: quantile window ending at 2013-07-02 20:59:11
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 20:49:11' AND '2013-07-02 20:59:11' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T114: quantile window ending at 2013-07-02 21:12:27
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:02:27' AND '2013-07-02 21:12:27' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T115: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T116: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T117: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T118: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T119: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T120: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T121: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T122: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T123: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T124: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T125: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T126: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T127: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T128: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T129: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T130: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T131: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T132: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T133: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T134: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T135: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T136: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T137: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T138: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T139: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T140: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T141: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T142: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T143: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T144: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T145: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T146: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T147: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T148: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T149: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T150: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T151: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T152: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T153: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T154: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T155: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T156: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T157: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T158: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T159: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T160: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T161: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T162: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T163: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T164: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T165: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T166: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T167: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T168: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T169: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T170: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T171: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T172: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T173: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T174: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T175: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T176: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T177: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T178: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T179: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T180: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T181: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T182: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T183: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T184: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T185: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T186: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T187: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T188: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T189: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T190: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T191: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T192: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T193: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T194: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T195: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T196: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T197: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T198: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T199: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T200: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T201: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T202: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T203: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T204: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T205: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T206: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T207: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T208: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T209: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T210: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T211: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T212: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T213: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T214: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T215: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T216: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T217: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T218: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T219: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T220: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T221: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T222: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T223: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T224: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T225: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T226: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T227: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T228: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T229: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T230: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T231: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T232: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T233: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T234: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T235: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T236: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T237: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T238: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T239: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T240: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T241: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T242: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T243: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T244: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T245: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T246: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T247: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T248: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T249: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T250: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T251: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T252: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T253: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T254: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T255: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T256: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T257: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T258: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T259: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T260: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T261: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T262: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T263: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T264: quantile window ending at 2013-07-02 21:19:06
SELECT quantile(0.95)(ResolutionWidth) FROM hits WHERE EventTime BETWEEN '2013-07-02 21:09:06' AND '2013-07-02 21:19:06' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
