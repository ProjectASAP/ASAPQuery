import os
import json
import argparse
from typing import List

import utils.http_utils as http_utils
import utils.jinja_utils as jinja_utils


def main(args):
    if args.all_udfs and args.udfs:
        raise ValueError(
            "Cannot specify both --all_udfs and --udfs. Use one or the other."
        )
    if not args.all_udfs and not args.udfs:
        raise ValueError("You must specify either --all_udfs or --udfs.")

    udfs: List[str] = []
    if args.udfs:
        udfs = args.udfs.strip().split(",")
        udfs = [udf.strip() for udf in udfs if udf.strip()]
    else:
        udf_templates = os.listdir(os.path.join(args.template_dir, "udfs"))
        udfs = [
            udf.split(".rs")[0]
            for udf in udf_templates
            if udf.endswith(".rs") or udf.endswith(".rs.j2")
        ]

    if not udfs:
        raise ValueError("No UDFs found to validate.")
    udfs = sorted(udfs)

    print(f"Validating UDFs: {', '.join(udfs)}")

    for udf in udfs:
        udf_body = None
        udf_dir = os.path.join(args.template_dir, "udfs")

        # Check if we have a Jinja template version first
        template_path = os.path.join(udf_dir, f"{udf}.rs.j2")
        regular_path = os.path.join(udf_dir, f"{udf}.rs")

        if os.path.exists(template_path):
            # Read template source and parse for variables
            with open(template_path, "r") as file:
                template_source = file.read()

            # Load the template for rendering
            udf_template = jinja_utils.load_template(udf_dir, f"{udf}.rs.j2")

            # Get all template variables and set them to 100
            template_vars = jinja_utils.get_template_variables(
                template_source, udf_template.environment
            )
            params = {var_name: 100 for var_name in template_vars}

            udf_body = udf_template.render(**params)
        elif os.path.exists(regular_path):
            # Use regular file if no template exists
            with open(regular_path, "r") as file:
                udf_body = file.read()
        else:
            raise ValueError(
                f"UDF {udf} not found. Neither {template_path} nor {regular_path} exists."
            )

        if not udf_body:
            raise ValueError(f"UDF {udf} is empty or could not be rendered.")

        data = {"definition": udf_body, "language": "rust"}

        response = http_utils.create_arroyo_resource(
            args.arroyo_url,
            endpoint="udfs/validate",
            data=json.dumps(data),
            resource_type="UDF",
        )
        response = json.loads(response)

        print(f"Validating UDF: {udf}")
        print(response)
        print("-" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate UDFs in a given directory against a template directory."
    )
    parser.add_argument(
        "--template_dir",
        default="./templates",
        help="Directory containing template files",
    )

    parser.add_argument(
        "--arroyo_url",
        default="http://localhost:5115/api/v1",
        help="URL of the Arroyo API server",
    )

    parser.add_argument(
        "--all_udfs",
        action="store_true",
        help="Validate all UDFs in the template directory",
    )
    parser.add_argument(
        "--udfs",
        type=str,
        required=False,
        help="Comma-separated list of UDFs to validate",
    )

    args = parser.parse_args()
    main(args)
