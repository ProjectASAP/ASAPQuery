import argparse

from utils import arroyo_utils


def main(args):
    # http_utils.make_api_request(
    #     url=f"{args.arroyo_url}/pipelines/{args.pipeline_id}",
    #     method="patch",
    #     data=json.dumps({"stop": "immediate"}),
    # )
    # http_utils.make_api_request(
    #     url=f"{args.arroyo_url}/pipelines/{args.pipeline_id}",
    #     method="delete",
    # )

    if not args.pipeline_id and not args.all_pipelines:
        raise ValueError("You must specify either --pipeline_id or --all_pipelines.")

    pipeline_ids = []
    if args.pipeline_id:
        pipeline_ids = [args.pipeline_id]
    elif args.all_pipelines:
        pipeline_ids = arroyo_utils.get_all_pipelines(arroyo_url=args.arroyo_url)

    arroyo_utils.stop_and_delete_pipelines(
        arroyo_url=args.arroyo_url, pipeline_ids=pipeline_ids
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete a pipeline.")

    parser.add_argument(
        "--pipeline_id",
        type=str,
        required=False,
        help="The ID of the pipeline to delete.",
    )
    parser.add_argument(
        "--all_pipelines", action="store_true", help="Delete all pipelines."
    )
    parser.add_argument(
        "--arroyo_url",
        default="http://localhost:5115/api/v1",
        help="URL of the Arroyo API server",
    )

    args = parser.parse_args()
    main(args)
