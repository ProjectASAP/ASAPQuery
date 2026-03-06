import os
import sys
import yaml
import argparse
import numpy as np

from typing import List

# from promql_utilities.query_results.classes import QueryResult, QueryResultAcrossTime

# TODO: make this more robust
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants  # noqa: E402


def correlation(exact, estimate) -> float:
    corr_result = np.corrcoef(exact, estimate)[0, 1]

    if np.isnan(corr_result):
        print(
            f"DEBUG correlation NaN detected - Array lengths: exact={len(exact)}, estimate={len(estimate)}"
        )
        print(f"DEBUG correlation NaN detected - Exact values: {exact}")
        print(f"DEBUG correlation NaN detected - Estimate values: {estimate}")
        print(f"DEBUG correlation NaN detected - Exact variance: {np.var(exact)}")
        print(f"DEBUG correlation NaN detected - Estimate variance: {np.var(estimate)}")
        print(
            f"DEBUG correlation NaN detected - Contains NaN - Exact: {np.isnan(exact).any()}, Estimate: {np.isnan(estimate).any()}"
        )
        print(
            f"DEBUG correlation NaN detected - Contains Inf - Exact: {np.isinf(exact).any()}, Estimate: {np.isinf(estimate).any()}"
        )
        print(f"DEBUG correlation NaN detected - Result: {corr_result}")

    return corr_result


def l1_norm(exact, estimate) -> float:
    return np.sum(np.abs(exact - estimate) / exact)


def l2_norm(exact, estimate) -> float:
    return np.sum(np.square(exact - estimate) / exact)


def mape(exact, estimate) -> float:
    # Mean Absolute Percentage Error
    # Handle division by zero by excluding zero values
    non_zero_mask = exact != 0
    if not np.any(non_zero_mask):
        return float("inf") if not np.array_equal(exact, estimate) else 0.0
    return (
        np.mean(
            np.abs(
                (exact[non_zero_mask] - estimate[non_zero_mask]) / exact[non_zero_mask]
            )
        )
        * 100
    )


def rmse_percentage(exact, estimate) -> float:
    # Root Mean Square Percentage Error
    non_zero_mask = exact != 0
    if not np.any(non_zero_mask):
        return float("inf") if not np.array_equal(exact, estimate) else 0.0
    return (
        np.sqrt(
            np.mean(
                (
                    (exact[non_zero_mask] - estimate[non_zero_mask])
                    / exact[non_zero_mask]
                )
                ** 2
            )
        )
        * 100
    )


# def get_timeseries_similarity_scores(results_across_servers, queries: List[str], similarity_functions):
#     similarity_scores = {f.__name__: {q: 0 for q in queries} for f in similarity_functions}

#     for f in similarity_functions:
#         for query_idx, query in enumerate(queries):
#             prom_results = results_across_servers["prometheus"][query_idx].get_all_timeseries()
#             sketchdb_results = results_across_servers["sketchdb"][query_idx].get_all_timeseries()

#             scores_per_key = {}

#             for timeseries_key in prom_results:
#                 if timeseries_key not in sketchdb_results:
#                     print(f"Skipping timeseries {timeseries_key} because it is not present in SketchDB")
#                     continue

#                 prom_timeseries = prom_results[timeseries_key].values
#                 sketchdb_timeseries = sketchdb_results[timeseries_key].values

#                 score = f(prom_timeseries, sketchdb_timeseries)
#                 scores_per_key[timeseries_key] = score
#                 similarity_scores[f.__name__][query] += score / len(prom_results)

#     return similarity_scores


def get_timeseries_similarity_scores(
    exact_results,
    estimate_results,
    queries: List[str],
    similarity_functions,
    verbose: bool,
):
    similarity_scores = {
        f.__name__: {q: 0 for q in queries} for f in similarity_functions
    }

    for f in similarity_functions:
        for query_idx, query in enumerate(queries):
            print(
                f"Calculating similarity scores for query {query} using function {f.__name__}"
            )
            exact_timeseries_per_key = exact_results[query_idx].get_all_timeseries()
            estimate_timeseries_per_key = estimate_results[
                query_idx
            ].get_all_timeseries()

            scores_per_key = {}

            for timeseries_key in sorted(exact_timeseries_per_key.keys()):
                if timeseries_key not in estimate_timeseries_per_key:
                    print(
                        f"Skipping timeseries {timeseries_key} because it is not present in estimated results"
                    )
                    continue

                exact_timeseries = exact_timeseries_per_key[timeseries_key].values
                estimate_timeseries = estimate_timeseries_per_key[timeseries_key].values

                if len(exact_timeseries) == 0 or len(estimate_timeseries) == 0:
                    print(
                        f"Skipping timeseries {timeseries_key} because exact has {len(exact_timeseries)} data points and estimate has {len(estimate_timeseries)} data points"
                    )
                    continue

                if any([v is None for v in exact_timeseries]):
                    print(
                        f"Skipping timeseries {timeseries_key} because exact_timeseries has None value"
                    )
                    continue
                if any([v is None for v in estimate_timeseries]):
                    print(
                        f"Skipping timeseries {timeseries_key} because estimate_timeseries has None value"
                    )
                    continue

                if verbose:
                    print("Key: {}".format(timeseries_key))
                    print("Exact: {}".format(exact_timeseries))
                    print("Estimate: {}".format(estimate_timeseries))

                score = f(exact=exact_timeseries, estimate=estimate_timeseries)
                scores_per_key[timeseries_key] = score
                similarity_scores[f.__name__][query] += score / len(
                    exact_timeseries_per_key
                )

    return similarity_scores


def main(args):
    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_name)

    exact_results = None
    estimate_results = None

    from results_loader import load_results, get_server_name_for_mode

    exact_results = load_results(
        os.path.join(
            experiment_dir, args.exact_experiment_mode, "prometheus_client_output"
        )
    )
    estimate_results = load_results(
        os.path.join(
            experiment_dir, args.estimate_experiment_mode, "prometheus_client_output"
        )
    )

    # results_across_modes = {}
    if not args.exact_experiment_server_name:
        args.exact_experiment_server_name = get_server_name_for_mode(
            experiment_dir, args.exact_experiment_mode
        )

    # results_across_modes['{}.{}'.format(args.exact_experiment_mode, args.exact_experiment_server_name)] = exact_results[args.exact_experiment_server_name]
    # results_across_modes['{}.{}'.format(args.estimate_experiment_mode, args.estimate_experiment_server_name)] = estimate_results[args.estimate_experiment_server_name]
    print(
        exact_results.keys(),
        estimate_results.keys(),
        args.exact_experiment_server_name,
        args.estimate_experiment_server_name,
    )
    exact_results = exact_results[args.exact_experiment_server_name]
    estimate_results = estimate_results[args.estimate_experiment_server_name]

    # results_across_modes[args.exact_experiment_mode] = exact_results[args.exact_experiment_mode]
    # results_across_modes[args.estimate_experiment_mode] = estimate_results[args.estimate_experiment_mode]

    query_group_config = None
    config_files = os.listdir(os.path.join(experiment_dir, "experiment_config"))
    if len(config_files) != 1:
        raise ValueError(
            f"Expected exactly one config file in {experiment_dir}, but found {len(config_files)}"
        )
    with open(
        os.path.join(experiment_dir, "experiment_config", config_files[0]), "r"
    ) as f:
        config = yaml.safe_load(f)
        query_group_config = config["query_groups"]

    # Flatten queries from all query groups
    all_queries = []
    for query_group in query_group_config:
        all_queries.extend(query_group["queries"])
    # timeseries_similarity_scores = get_timeseries_similarity_scores(results_across_modes, all_queries, [
    timeseries_similarity_scores = get_timeseries_similarity_scores(
        exact_results,
        estimate_results,
        all_queries,
        [correlation, l1_norm, l2_norm, mape, rmse_percentage],
        args.verbose,
    )

    # with open(os.path.join(args.output_dir, args.output_file), "w") as fout:
    for f in timeseries_similarity_scores:
        for query in timeseries_similarity_scores[f]:
            print(f"{f}: {query} = {timeseries_similarity_scores[f][query]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--exact_experiment_mode", type=str, required=True)
    parser.add_argument("--estimate_experiment_mode", type=str, required=True)
    parser.add_argument("--exact_experiment_server_name", type=str, required=True)
    parser.add_argument("--estimate_experiment_server_name", type=str, required=True)
    parser.add_argument("--verbose", action="store_true", default=False)
    args = parser.parse_args()
    main(args)
