-- T000: quantile window ending at 2013-07-14 20:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 20:00:00') AND '2013-07-14 20:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T001: quantile window ending at 2013-07-14 20:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 20:30:00') AND '2013-07-14 20:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T002: quantile window ending at 2013-07-14 21:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 21:00:00') AND '2013-07-14 21:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T003: quantile window ending at 2013-07-14 21:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 21:30:00') AND '2013-07-14 21:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T004: quantile window ending at 2013-07-14 22:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 22:00:00') AND '2013-07-14 22:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T005: quantile window ending at 2013-07-14 22:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 22:30:00') AND '2013-07-14 22:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T006: quantile window ending at 2013-07-14 23:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 23:00:00') AND '2013-07-14 23:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T007: quantile window ending at 2013-07-14 23:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-14 23:30:00') AND '2013-07-14 23:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T008: quantile window ending at 2013-07-15 00:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 00:00:00') AND '2013-07-15 00:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T009: quantile window ending at 2013-07-15 00:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 00:30:00') AND '2013-07-15 00:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T010: quantile window ending at 2013-07-15 01:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 01:00:00') AND '2013-07-15 01:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T011: quantile window ending at 2013-07-15 01:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 01:30:00') AND '2013-07-15 01:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T012: quantile window ending at 2013-07-15 02:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 02:00:00') AND '2013-07-15 02:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T013: quantile window ending at 2013-07-15 02:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 02:30:00') AND '2013-07-15 02:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T014: quantile window ending at 2013-07-15 03:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 03:00:00') AND '2013-07-15 03:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T015: quantile window ending at 2013-07-15 03:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 03:30:00') AND '2013-07-15 03:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T016: quantile window ending at 2013-07-15 04:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 04:00:00') AND '2013-07-15 04:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T017: quantile window ending at 2013-07-15 04:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 04:30:00') AND '2013-07-15 04:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T018: quantile window ending at 2013-07-15 05:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 05:00:00') AND '2013-07-15 05:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T019: quantile window ending at 2013-07-15 05:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 05:30:00') AND '2013-07-15 05:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T020: quantile window ending at 2013-07-15 06:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 06:00:00') AND '2013-07-15 06:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T021: quantile window ending at 2013-07-15 06:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 06:30:00') AND '2013-07-15 06:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T022: quantile window ending at 2013-07-15 07:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 07:00:00') AND '2013-07-15 07:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T023: quantile window ending at 2013-07-15 07:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 07:30:00') AND '2013-07-15 07:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T024: quantile window ending at 2013-07-15 08:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 08:00:00') AND '2013-07-15 08:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T025: quantile window ending at 2013-07-15 08:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 08:30:00') AND '2013-07-15 08:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T026: quantile window ending at 2013-07-15 09:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 09:00:00') AND '2013-07-15 09:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T027: quantile window ending at 2013-07-15 09:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 09:30:00') AND '2013-07-15 09:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T028: quantile window ending at 2013-07-15 10:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 10:00:00') AND '2013-07-15 10:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T029: quantile window ending at 2013-07-15 10:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 10:30:00') AND '2013-07-15 10:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T030: quantile window ending at 2013-07-15 11:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 11:00:00') AND '2013-07-15 11:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T031: quantile window ending at 2013-07-15 11:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 11:30:00') AND '2013-07-15 11:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T032: quantile window ending at 2013-07-15 12:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 12:00:00') AND '2013-07-15 12:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T033: quantile window ending at 2013-07-15 12:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 12:30:00') AND '2013-07-15 12:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T034: quantile window ending at 2013-07-15 13:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 13:00:00') AND '2013-07-15 13:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T035: quantile window ending at 2013-07-15 13:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 13:30:00') AND '2013-07-15 13:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T036: quantile window ending at 2013-07-15 14:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 14:00:00') AND '2013-07-15 14:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T037: quantile window ending at 2013-07-15 14:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 14:30:00') AND '2013-07-15 14:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T038: quantile window ending at 2013-07-15 15:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 15:00:00') AND '2013-07-15 15:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T039: quantile window ending at 2013-07-15 15:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 15:30:00') AND '2013-07-15 15:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T040: quantile window ending at 2013-07-15 16:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 16:00:00') AND '2013-07-15 16:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T041: quantile window ending at 2013-07-15 16:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 16:30:00') AND '2013-07-15 16:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T042: quantile window ending at 2013-07-15 17:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 17:00:00') AND '2013-07-15 17:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T043: quantile window ending at 2013-07-15 17:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 17:30:00') AND '2013-07-15 17:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T044: quantile window ending at 2013-07-15 18:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 18:00:00') AND '2013-07-15 18:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T045: quantile window ending at 2013-07-15 18:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 18:30:00') AND '2013-07-15 18:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T046: quantile window ending at 2013-07-15 19:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 19:00:00') AND '2013-07-15 19:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T047: quantile window ending at 2013-07-15 19:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 19:30:00') AND '2013-07-15 19:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T048: quantile window ending at 2013-07-15 20:00:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 20:00:00') AND '2013-07-15 20:00:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
-- T049: quantile window ending at 2013-07-15 20:30:00
SELECT QUANTILE(0.95, ResolutionWidth) FROM hits WHERE EventTime BETWEEN DATEADD(s, -10, '2013-07-15 20:30:00') AND '2013-07-15 20:30:00' GROUP BY RegionID, OS, UserAgent, TraficSourceID;
