# Gravy Train EtL Pipeline

Extract UK MP details from the Houses of Parliament API & expense claims from the Independent Parliamentary Standards Authority.

Uses a version of

# Simple EtL Pipeline

A very lightweight EtL pipeline builder with transformations and scheduling.

Flat Files -> transform -> DuckDB or Motherduck

Test pipeline locally with **DuckDB** then to the cloud with **Motherduck**.

### python src/simple_pipe.py ###

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

