# Simple EtL Pipeline

A very lightweight EtL pipeline builder with transformations and scheduling.

Flat Files -> transform -> DuckDB or Motherduck

Test pipeline locally with **DuckDB** then to the cloud with **Motherduck**.

Currently works with excel & csv files.  

Uses pandas for import and Duckdb native connection/SQL for export to database.

Transformation of the data is performed as a SQL select on the loaded DataFrame.


# pipeline.toml

The pipeline.toml configures the whole pipeline.

# secret.toml

To use access MotherDuck locally update config/secret.toml 

MOTHERDUCK_TOKEN = "**Your Motherduck Access Token**"

**Be Safe** and add this file to .gitignore 

## GITHUB ACTIONS

pipeline_workflow.yml github action scheduling

**Repository Secrets**

MOTHERDUCK_TOKEN = **Your Motherduck Access Token**

