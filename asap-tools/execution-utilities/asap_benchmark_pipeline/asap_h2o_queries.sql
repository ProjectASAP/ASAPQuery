-- Q1: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:11:10') AND '1971-01-01 00:11:10'
GROUP BY id1, id2, id3;

-- Q2: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:13:10') AND '1971-01-01 00:13:10'
GROUP BY id1, id2, id3;

-- Q3: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:15:10') AND '1971-01-01 00:15:10'
GROUP BY id1, id2, id3;

-- Q4: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:17:10') AND '1971-01-01 00:17:10'
GROUP BY id1, id2, id3;

-- Q5: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:19:10') AND '1971-01-01 00:19:10'
GROUP BY id1, id2, id3;

-- Q6: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:21:10') AND '1971-01-01 00:21:10'
GROUP BY id1, id2, id3;

-- Q7: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:23:10') AND '1971-01-01 00:23:10'
GROUP BY id1, id2, id3;

-- Q8: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:25:10') AND '1971-01-01 00:25:10'
GROUP BY id1, id2, id3;

-- Q9: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:27:10') AND '1971-01-01 00:27:10'
GROUP BY id1, id2, id3;

-- Q10: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:29:10') AND '1971-01-01 00:29:10'
GROUP BY id1, id2, id3;

-- Q11: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:31:10') AND '1971-01-01 00:31:10'
GROUP BY id1, id2, id3;

-- Q12: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:33:10') AND '1971-01-01 00:33:10'
GROUP BY id1, id2, id3;

-- Q13: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:35:10') AND '1971-01-01 00:35:10'
GROUP BY id1, id2, id3;

-- Q14: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:37:10') AND '1971-01-01 00:37:10'
GROUP BY id1, id2, id3;

-- Q15: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:39:10') AND '1971-01-01 00:39:10'
GROUP BY id1, id2, id3;

-- Q16: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:41:10') AND '1971-01-01 00:41:10'
GROUP BY id1, id2, id3;

-- Q17: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:43:10') AND '1971-01-01 00:43:10'
GROUP BY id1, id2, id3;

-- Q18: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:45:10') AND '1971-01-01 00:45:10'
GROUP BY id1, id2, id3;

-- Q19: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:47:10') AND '1971-01-01 00:47:10'
GROUP BY id1, id2, id3;

-- Q20: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:49:10') AND '1971-01-01 00:49:10'
GROUP BY id1, id2, id3;

-- Q21: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:51:10') AND '1971-01-01 00:51:10'
GROUP BY id1, id2, id3;

-- Q22: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:53:10') AND '1971-01-01 00:53:10'
GROUP BY id1, id2, id3;

-- Q23: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:55:10') AND '1971-01-01 00:55:10'
GROUP BY id1, id2, id3;

-- Q24: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:57:10') AND '1971-01-01 00:57:10'
GROUP BY id1, id2, id3;

-- Q25: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 00:59:10') AND '1971-01-01 00:59:10'
GROUP BY id1, id2, id3;

-- Q26: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:01:10') AND '1971-01-01 01:01:10'
GROUP BY id1, id2, id3;

-- Q27: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:03:10') AND '1971-01-01 01:03:10'
GROUP BY id1, id2, id3;

-- Q28: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:05:10') AND '1971-01-01 01:05:10'
GROUP BY id1, id2, id3;

-- Q29: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:07:10') AND '1971-01-01 01:07:10'
GROUP BY id1, id2, id3;

-- Q30: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:09:10') AND '1971-01-01 01:09:10'
GROUP BY id1, id2, id3;

-- Q31: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:11:10') AND '1971-01-01 01:11:10'
GROUP BY id1, id2, id3;

-- Q32: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:13:10') AND '1971-01-01 01:13:10'
GROUP BY id1, id2, id3;

-- Q33: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:15:10') AND '1971-01-01 01:15:10'
GROUP BY id1, id2, id3;

-- Q34: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:17:10') AND '1971-01-01 01:17:10'
GROUP BY id1, id2, id3;

-- Q35: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:19:10') AND '1971-01-01 01:19:10'
GROUP BY id1, id2, id3;

-- Q36: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:21:10') AND '1971-01-01 01:21:10'
GROUP BY id1, id2, id3;

-- Q37: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:23:10') AND '1971-01-01 01:23:10'
GROUP BY id1, id2, id3;

-- Q38: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:25:10') AND '1971-01-01 01:25:10'
GROUP BY id1, id2, id3;

-- Q39: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:27:10') AND '1971-01-01 01:27:10'
GROUP BY id1, id2, id3;

-- Q40: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:29:10') AND '1971-01-01 01:29:10'
GROUP BY id1, id2, id3;

-- Q41: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:31:10') AND '1971-01-01 01:31:10'
GROUP BY id1, id2, id3;

-- Q42: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:33:10') AND '1971-01-01 01:33:10'
GROUP BY id1, id2, id3;

-- Q43: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:35:10') AND '1971-01-01 01:35:10'
GROUP BY id1, id2, id3;

-- Q44: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:37:10') AND '1971-01-01 01:37:10'
GROUP BY id1, id2, id3;

-- Q45: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:39:10') AND '1971-01-01 01:39:10'
GROUP BY id1, id2, id3;

-- Q46: Sum v1 by id1
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:41:10') AND '1971-01-01 01:41:10'
GROUP BY id1, id2, id3;

-- Q47: Sum v1 by id1:id2
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:43:10') AND '1971-01-01 01:43:10'
GROUP BY id1, id2, id3;

-- Q48: Sum v1 mean v3 by id3
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:45:10') AND '1971-01-01 01:45:10'
GROUP BY id1, id2, id3;

-- Q49: Mean v1:v3 by id4
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:47:10') AND '1971-01-01 01:47:10'
GROUP BY id1, id2, id3;

-- Q50: Sum v1:v3 by id6
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN  DATEADD(s, -120, '1971-01-01 01:49:10') AND '1971-01-01 01:49:10'
GROUP BY id1, id2, id3;
