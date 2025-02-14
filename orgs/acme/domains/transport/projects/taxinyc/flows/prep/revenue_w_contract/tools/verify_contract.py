from datacontract.data_contract import DataContract


## Must be set in environment:
#
# DATACONTRACT_DATABRICKS_TOKEN (Databricks pat token)
# DATACONTRACT_DATABRICKS_SERVER_HOSTNAME like "dbc-1111112222.cloud.databricks.com"
# DATACONTRACT_DATABRICKS_HTTP_PATH like "/sql/1.0/warehouses/aee0a67411111111"


def run_verify_contract():
    data_contract = DataContract(
        data_contract_file="./contracts/revenue_per_inhabitant.yaml"
    )
    run = data_contract.test()
    print("verify_contract.py:" + repr(16) + ":run:" + repr(run))
    print(run.pretty())
    if not run.has_passed():
        print("Data quality validation failed.")
    else:
        print("Data quality validation successful.")

run_verify_contract()
