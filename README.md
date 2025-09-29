# Databricks Project

This repository provides a modular structure for Databricks workflows, utilities, and demos. Each top-level folder serves a specific purpose in the overall data engineering and analytics process.

## General Structure

- Each main folder groups related resources (notebooks, scripts, configuration, jobs) for a specific domain or use case.
- Subfolders like `notebooks/`, `bundles/`, or `docs/` further organize code, job definitions, and documentation.
- All documentation is kept at the main folder level for clarity and maintainability.

## Folder Overview

- [**compare_datasets/**](databricks/compare_datasets/README.md):
  Tools and notebooks for comparing datasets, checking schema existence, creating test data, and retrieving entities. Includes job definitions for dataset comparison workflows.

- [**dbdemos/**](databricks/dbdemos/README.md):
  Demo scripts and resources for Databricks, including installation scripts and documentation.

- [**dim_date_time/**](databricks/dim_date_time/README.md):
  Resources for generating and managing a shared date/time dimension, with scripts for creating and generating date/time data and job definitions for dimension creation.

- [**lineage/**](databricks/lineage/README.md):
  Resources for data lineage tracking and visualization, including job, dashboard, and pipeline definitions, dashboard configs, SQL scripts, documentation, and notebooks.

- [**utilities/**](databricks/utilities/README.md):
  Utility scripts and notebooks for Databricks administration and support tasks.

- **databricks.yml** and **variables.yml**:
  Configuration files for Databricks workflows and environment variables.

---

## Repository Responsibility & Contact

This repository is maintained by **Stefan Koch** and **Severin Lindenmann**. If you have any questions, suggestions, or need support, please feel free to reach out to either of them. They are responsible for the overall structure, code quality, and direction of this project.

## Contributing

We welcome contributions from everyone! If you would like to contribute, you can:
- Create a feature branch for your changes and submit a pull request
- Open an issue to report bugs, request features, or ask questions

Whether it's a small fix, a new feature, or just a suggestion, every contribution is appreciated. Please make sure your code is well-documented and tested where possible.

Let's work together to make this project even better!

---

For more details, see the individual README files in each folder.