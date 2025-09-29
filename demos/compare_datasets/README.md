# Compare Datasets Demo

This folder contains a demo for comparing datasets (tables or views) in Databricks. The purpose of this demo is to provide tools and examples for validating, comparing, and testing the structure and content of different datasets within your Databricks environment.

## What It Does
- **Schema Comparison:** Check if the schemas of two datasets (tables or views) are identical or highlight differences.
- **Data Comparison:** Compare the actual data between two datasets to identify mismatches or missing records.
- **Test Data Generation:** Create sample datasets for testing and demonstration purposes.
- **Entity Extraction:** Retrieve and list entities (tables/views) from your environment for comparison.

## Folder Structure
- `notebooks/`
  - `check_schemas_exists.py`: Checks if schemas exist for the specified datasets.
  - `compare_datasets.py`: Main script for comparing datasets (schema and data).
  - `create_test_data.py`: Generates test data for use in comparisons.
  - `get_entities.py`: Retrieves entities (tables/views) from the environment.
- `bundles/`
  - `compare_datasets.jobs.yml`: Example Databricks job configuration for running the comparison workflows.

## How to Use
1. **Set Up Your Environment:**
   - Ensure you have access to a Databricks workspace and the necessary permissions to read tables/views.
2. **Upload Notebooks:**
   - Import the notebooks from the `notebooks/` folder into your Databricks workspace.
3. **Configure and Run:**
   - Use the provided notebooks to check schemas, generate test data, and compare datasets as needed.
   - Optionally, use the job configuration in `bundles/compare_datasets.jobs.yml` to automate the process.

## Job Failure Conditions
The provided job (`compare_datasets.jobs.yml`) is designed to fail if differences are detected between the compared datasets. This ensures that any schema or data mismatches are caught early and can be addressed before proceeding. The job will:
- **Fail on Schema Differences:** If the schemas of the datasets do not match, the job will stop and report the differences.
- **Fail on Data Differences:** If there are discrepancies in the data (missing, extra, or mismatched records), the job will fail and provide details about the inconsistencies.

This behavior is intentional to support automated validation and regression testing workflows, making it easy to catch issues during data migrations or pipeline updates.

## Example Use Cases
- Validating data migrations between environments.
- Ensuring consistency between production and test datasets.
- Automated regression testing for data pipelines.

---
This demo is provided for educational and demonstration purposes. Adapt the scripts and workflows as needed for your specific use case.
