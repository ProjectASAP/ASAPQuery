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

Templates outlining the basic structure for some Elasticsearch queries. For the following examples, we use `${name}` syntax to denote dynamic/user provided variables. In each query, the time range specifier would be optional.

### 1\. Simple Aggregation

Compute summary statistics on all data for one or more data columns (metrics), optionally with a time range/predicate.

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
        "${agg_result1}": {
            "${agg_type1}": {
                "field": "${metric_name1}",
            }
        },
        "${agg_result2}": {
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
    AGG1(metric_name1) AS agg_result1,
    AGG2(metric_name2) AS agg_result2
FROM table_name
WHERE time_created >= NOW() - INTERVAL '30 seconds';
```

### 2\. Groupby Aggregation

Compute one or more summary statistics, grouped by one or more labels, optionally with a time range/predicate.

#### Group By (One Label)
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
        "${grouped_result1}": {
            "terms": {
                "field": "${field1}.keyword",
            },
            "aggs": {
                "${agg_result1}": {
                    "${agg_type1}": {
                        "field": "${metric_name1}",
                    }
                }
            }
        }
    }
}
```

#### Group By (Multi-Label)

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
        "${grouped_result1}": {
            "multi_terms": {
                "terms": [
                    { "field": "${field1}.keyword"},
                    { "field": "${field2}.keyword"}
                ]
            },
            "aggs": {
                "${agg_result1}": {
                    "${agg_type1}": {
                        "field": "${metric_name1}",
                    }
                }
            }
        }
    }
}
```

The corresponding SQL (multi-label case) is as follows.

```sql
SELECT 
    AGG1(metric_name1) AS agg_result1,
    AGG2(metric_name2) AS agg_result2
FROM table_name
WHERE time_created >= NOW() - INTERVAL '30 seconds'
GROUP BY field1, field2;
```
