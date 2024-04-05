# import argparse

# from libs.dataops.deploy.job import create_or_update
# from libs.dataops.deploy.readconfig import read_config_json


# def main():
#     parser = argparse.ArgumentParser(description="Create or Update a Databricks job.")
#     parser.add_argument("--host", required=True, help="Databricks host")
#     parser.add_argument("--token", required=True, help="Databricks token")
#     parser.add_argument("--file", required=True, help="Job configuration file")
#     parser.add_argument("--branch-name", required=False, help="Git branch name")
#     parser.add_argument("--release-version", required=False, help="Release version")

#     args = parser.parse_args()

#     cfg = read_config_json(args.file)
#     response = create_or_update(
#         databricks_host=args.host,
#         databricks_token=args.token,
#         job_config=cfg,
#         branch_name=args.branch_name,
#         release_version=args.release_version,
#     )
#     print(f"Job response: {response}")


# if __name__ == "__main__":
#     main()
