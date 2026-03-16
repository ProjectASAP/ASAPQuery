# (ASAP) Potential Elasticsearch Queries to be Supported

Info dump about what Elasticsearch queries could be supported by ASAP based on my understanding of the query engine.

## Supportable Query Types

Elastic DSL single level aggregation queries that explicitly don't return any records (`"size": 0`), with basic filtering by column label values, seem the most similar to the Prometheus style queries, so they could maybe be translated.

Here is a list of ES aggregation functions that seem to map well to the current sketches available:

- `"percentiles"` (quantiles)  
- `"min"`  
- `"max"`  
- `"sum"`  
- `"avg"`

## Query Templates

Templates outlining the basic structure for various kinds of Elasticsearch queries. For the following examples, we use `${name}` syntax to denote dynamic/user provided variables. In each query, the time range specifier would be optional.

### 1\. Simple Aggregation

Compute summary statistics on all data for one or more data columns (metrics).

```json
{
    "size": 0,
    "query": {
        "range": {
            "@timestamp": {
                "gte": "now-30s",
                "lte": "now"
            }
        }
    },                                          
    "aggs": {
        "${result1}": {
            "${agg_type1}": {
                "field": "${metric_name1}",
            }
        },
        "${result2}": {
            "${agg_type2}": {
                "field": "${metric_name2}",
                "${param1}": "${arg1}"
            }
        }
    }
}
```

This is semantically equivalent to the following SQL.

```sql
SELECT 
    AGG1(metric_name1) AS result1,
    AGG2(metric_name2) AS result2
FROM table_name
WHERE time_created >= NOW() - INTERVAL '30 seconds';
```

### 2\. Filtered Aggregation

Compute summary statistics for metrics over a specific combination of label values.

```json
{
    "size": 0,
    "query": {
        "bool": {
            "filter": {
                "term": { "${field1}.keyword": "${value1}" },
                "range": {
                    "@timestamp": {
                        "gte": "now-30s",
                        "lte": "now"
                    }
                }
            }
        }
    },                                          
    "aggs": {
        "${result1}": {
            "${agg_type1}": {
                "field": "${metric_name1}",
            }
        },
        "${result2}": {
            "${agg_type2}": {
                "field": "${metric_name2}",
                "${param1}": "${arg1}"
            }
        }
    }
}
```

The corresponding SQL is as follows.

```sql
SELECT 
    AGG1(metric_name1) AS result1,
    AGG2(metric_name2) AS result2
FROM table_name
WHERE field1 = value1 AND time_created >= NOW() - INTERVAL '30 seconds';
```

### 3\. Filtered Aggregation (Batched)

Compute summary statistics for a metric, grouping by column labels.

```json
{
    "size": 0,
    "query": {
        "range": {
            "@timestamp": {
                "gte": "now-30s",
                "lte": "now"
            }
        }
    },                                          
    "aggs": {
        "${result1}": {
            "filters": {
                "filters": {
                    "${bucket1}": {
                        "term": { "${field1}.keyword": "${value1}" },
                    },
                    "${bucket2}": {
                        "term": { "${field1}.keyword": "${value2}" },
                    }
                }
            },
            "aggs": {
                "${agg_name1}": {
                    "${agg_type1}": {
                        "field": "${metric_name1}",
                    }
                }
            }
        }
    }
}
```

Here is the corresponding SQL.

```sql
SELECT 
    bucket,
    AGG1(metric_name1) AS agg_name1
FROM (
    SELECT 
        CASE 
            WHEN field1 = @value1 THEN 'bucket1'
            WHEN field1 = @value2 THEN 'bucket2' 
            ELSE 'drop' 
        END AS bucket,
        metric_name1
    FROM table_name
    WHERE time_created >= NOW() - INTERVAL '30 seconds'
)
WHERE bucket != 'drop'
GROUP BY bucket;
```

Of course, if you are bucketing by every unique value, then the above statement reduces to a regular `GROUP BY`.  
