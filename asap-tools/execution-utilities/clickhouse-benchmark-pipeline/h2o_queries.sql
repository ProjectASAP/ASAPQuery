-- Q1: Sum v1 by id1
SELECT id1, sum(v1) AS v1 FROM h2o_groupby GROUP BY id1 ORDER BY id1;

-- Q2: Sum v1 by id1:id2
SELECT id1, id2, sum(v1) AS v1 FROM h2o_groupby GROUP BY id1, id2 ORDER BY id1, id2;

-- Q3: Sum v1 mean v3 by id3
SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM h2o_groupby GROUP BY id3 ORDER BY id3;

-- Q4: Mean v1:v3 by id4
SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM h2o_groupby GROUP BY id4 ORDER BY id4;

-- Q5: Sum v1:v3 by id6
SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM h2o_groupby GROUP BY id6 ORDER BY id6;

-- Q6: Median v3 sd v3 by id4 id5
SELECT id4, id5, median(v3) AS median_v3, stddevSamp(v3) AS sd_v3 FROM h2o_groupby GROUP BY id4, id5 ORDER BY id4, id5;

-- Q7: Max v1 - min v2 by id3
SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM h2o_groupby GROUP BY id3 ORDER BY id3;

-- Q8: Largest two v3 by id6
SELECT id6, v3 FROM h2o_groupby ORDER BY v3 DESC LIMIT 2 BY id6;

-- Q9: Regression v1 v2 by id2 id4 (Approximation using corr for benchmark simplicity or skip)
SELECT id2, id4, corr(v1, v2) FROM h2o_groupby GROUP BY id2, id4 ORDER BY id2, id4;

-- Q10: Sum v3 count by id1:id6
SELECT id1, id6, sum(v3) AS v3, count(*) AS count FROM h2o_groupby GROUP BY id1, id6 ORDER BY id1, id6;