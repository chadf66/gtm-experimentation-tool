# GTM Experimentation Tool (gxt)

**A modern CLI for SQL-based A/B testing and experimentation workflows**

Break free from spreadsheet chaos and manual experiment assignment processes. GTM Experimentation Tool empowers data teams to manage experiments with code, automate user assignments with deterministic SQL, and seamlessly integrate with modern data warehouses.

## Why gxt?

If you're in GTM, you've likely run into a scenario like this one: Your team wants to test two different GTM outreach strategies for an upcoming product launch campaign targeting enterprise accounts that signed up in the last 90 days.

**The Manual Process (😫):**
1. Someone writes a SQL query to find all enterprise accounts from the last 90 days
2. Export results to a Google Sheet with ~10,000 rows  
3. Add a `=RAND()` column and sort by it to "randomize"
4. Manually assign first 5,000 rows to "Subject A" and next 5,000 to "Subject B"
5. Copy assignments to another sheet for the campaign team
6. Cross fingers that no one accidentally sorts the sheet
7. Hope you can reproduce the same assignments next week when the campaign manager asks "wait, which users got which version again?"

**Problems:**
- ❌ Manual and error-prone
- ❌ Not reproducible (different results each time)
- ❌ No version control or audit trail  
- ❌ Doesn't scale across multiple experiments
- ❌ Easy to accidentally corrupt assignments

**The gxt Way (✅):**
```bash
# One-time setup
gxt new-experiment email-subject-test
```

### Define your audience in SQL in experiments/email-subject-test/audience.sql (version controlled!)
```sql 
SELECT user_id 
FROM analytics.users 
WHERE account_type = 'enterprise' 
  AND signup_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
```


### Configure your test in experiments/email-subject-test/config.yml
```yaml
experiment_id: email-subject-test
status: active
randomization_unit: user_id
assignments_table: gxt_assignments
variants:
  - variant1
    exposure: 0.5
  - variant2
    exposure: 0.5
```

### Generate assignments (same results every time!)
```bash
gxt run email-subject-test --no-dry-run
```

The gxt_assignments table will contain deterministic, reproducible user assignments that your application can query to serve the right variant to each user.

### Automate

Automate with your preferred orchestration tool (Airflow, cron, etc.). **Done!**

**Result:** Deterministic, reproducible randomized assignments stored in your data warehouse that any team member can query. No more spreadsheet chaos!


**For GTM Teams:**
- Replace error-prone Google Sheets with reliable, version-controlled experiment management
- Automate experiment assignments with simple SQL logic
- Scale experiments without manual intervention
- Maintain experiment history and reproducibility

**For Data Teams:**
- SQL-first approach using familiar tools and patterns
- Built-in integration with extensible adapter architecture (currently works with BigQuery, more adapters coming soon!)
- Dry-run mode for safe testing and validation
- Clean project structure with reusable templates

## Quick Start

Install via pip:
```bash
pip install gtm-experiments-tool
```

Initialize a new project:
```bash
gxt init my-experiments
cd my-experiments
```

Create your first experiment:
```bash
gxt new-experiment welcome-banner
```

Run assignments (dry-run by default):
```bash
gxt run welcome-banner
```

## Key Features

- **SQL-Based Logic**: Define experiment audiences and assignments using familiar SQL
- **Deterministic Assignments**: Reproducible user bucketing using hash-based randomization
- **BigQuery Integration**: Native support with automatic table management
- **Version Control Ready**: All configuration stored in YAML and SQL files
- **Safe by Default**: Dry-run mode prevents accidental data writes
- **Template System**: Quick scaffolding for new projects and experiments

## Project Structure

A gxt project follows a simple, organized layout:

```
my-experiments/
├── gxt_project.yml      # Project configuration
├── profiles.yml         # Database connection settings  
├── experiments/
│   ├── welcome-banner/
│   │   ├── audience.sql     # SQL defining experiment audience
│   │   └── config.yml       # Experiment configuration
│   └── pricing-test/
│       ├── audience.sql
│       └── config.yml
└── target/              # Compiled manifests (auto-generated)
    └── manifest.json    # Compiled experiment metadata and SQL
```

### Configuration Files

**gxt_project.yml** - Project settings:
```yaml
# Example
project_name: my_project_name
version: 0.1.0
profile: gxt_profile
dataset: experiments
assignments_table: gxt_assignments
```

**profiles.yml** - Database connections:
```yaml
# Example
gxt_profile:
  target: dev
  outputs:
    dev:
      type: bigquery
      project: my-gcp-project-id 
```

**experiments/[name]/config.yml** - Experiment settings:
```yaml
experiment_id: customer-onboarding-calls
status: active
randomization_unit: user_id
assignments_table: gxt_assignments
variants:
  - treatment
    exposure: 0.6
  - control
    exposure: 0.4
```

## CLI Reference

All commands support `--project-path` to specify the project directory (defaults to current directory).

### `gxt init <project-name>`
Create a new gxt project with sample files and templates.

```bash
gxt init my-experiments
```

### `gxt new-experiment <experiment-name>`
Scaffold a new experiment with template files.

```bash
gxt new-experiment --project-path ./my-experiments pricing-test
```

### `gxt list`
Display all experiments in the project.

```bash
gxt list --project-path ./my-experiments
```

### `gxt validate`
Check experiment configurations and SQL syntax.

```bash
gxt validate --project-path ./my-experiments
```

### `gxt compile [experiment-name]`
Compile experiment manifests, resolving `{{ source() }}` references to full table names.

```bash
# Compile all experiments
gxt compile --project-path ./my-experiments

# Compile specific experiment
gxt compile welcome-banner --project-path ./my-experiments
```

### `gxt run <experiment-name>`
Generate and optionally execute assignment SQL.

```bash
# Dry run (default) - prints SQL without executing
gxt run welcome-banner --project-path ./my-experiments

# Execute against BigQuery
gxt run welcome-banner --project-path ./my-experiments --no-dry-run

# Create assignments table if it doesn't exist
gxt run welcome-banner --project-path ./my-experiments --no-dry-run --create-assignments-table
```

**Options:**
- `--no-dry-run`: Execute SQL against the database (default is dry-run)
- `--create-assignments-table`: Create the assignments table if it doesn't exist
- `--adapter`: Specify adapter (currently only `bigquery` supported)

## BigQuery Setup

1. **Install dependencies:**
   ```bash
   pip install google-cloud-bigquery
   ```

2. **Set up authentication:**
   - Service account: Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
   - Or use Application Default Credentials: `gcloud auth application-default login`

3. **Ensure billing is enabled** on your GCP project for DML operations


## Contributing

This project welcomes contributions! Please see the [GitHub repository](https://github.com/chadf66/gtm-experimentation-tool) for issues and development guidelines.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.

