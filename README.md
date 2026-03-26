# Fabric CI/CD Examples

End-to-end CI/CD pipeline for Microsoft Fabric using [Fabric Automation Bundles](https://github.com/dereknguyenio/fabric-automation-bundles).

Deploys a medallion lakehouse architecture (bronze/silver/gold) across dev, test, and prod environments with automated quality gates.

## Architecture

```
PR opened → validate → plan (all targets)
                ↓
        merge to main
                ↓
        deploy to dev → deploy to test → run ETL → data quality checks → deploy to prod (manual approval)
```

## What Gets Deployed

| Resource | Type | Description |
|----------|------|-------------|
| bronze | Lakehouse | Raw data landing zone |
| silver | Lakehouse | Cleaned, validated data |
| gold | Lakehouse | Business-ready aggregates |
| spark_env | Environment | Spark runtime + libraries |
| ingest_to_bronze | Notebook | Source → bronze ingestion |
| bronze_to_silver | Notebook | Bronze → silver cleaning |
| silver_to_gold | Notebook | Silver → gold aggregation |
| data_quality_checks | Notebook | Quality validation across layers |
| daily_etl | Pipeline | Scheduled ETL: ingest → clean → aggregate |
| data_quality | Pipeline | Quality check pipeline |
| analytics_warehouse | Warehouse | SQL views over gold |
| analytics_agent | Data Agent | Natural language data access |

## Workflows

| Workflow | Trigger | What it does |
|----------|---------|-------------|
| [CI](.github/workflows/ci.yml) | PR to main | Validate + plan all targets |
| [CD](.github/workflows/deploy.yml) | Push to main | Deploy dev → test → prod |
| [Destroy](.github/workflows/destroy.yml) | Manual | Tear down dev or test |
| [Drift Check](.github/workflows/drift-check.yml) | Daily (weekdays) | Detect portal changes, create issues |

## Setup

### 1. Prerequisites

- Microsoft Fabric capacity (F2+ for dev, F8+ for prod)
- Azure service principal with Fabric API permissions
- GitHub repository (fork or clone this repo)

### 2. Create Service Principal

```bash
# Create the service principal
az ad sp create-for-rbac --name "sp-fabric-cicd" --role "Contributor"

# Note the output:
# - appId → AZURE_CLIENT_ID
# - password → AZURE_CLIENT_SECRET
# - tenant → AZURE_TENANT_ID
```

Grant the service principal access to your Fabric workspaces in the Fabric Admin Portal.

### 3. Configure GitHub Secrets

Go to your repo → Settings → Secrets and variables → Actions.

**Secrets (required):**

| Secret | Description |
|--------|-------------|
| `AZURE_TENANT_ID` | Azure AD tenant GUID |
| `AZURE_CLIENT_ID` | Service principal app ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |

**Variables (required):**

| Variable | Description |
|----------|-------------|
| `FAB_CAPACITY_ID` | Fabric capacity GUID |

### 4. Configure GitHub Environments

Go to Settings → Environments and create:

- **dev** — no protection rules (auto-deploy)
- **test** — no protection rules (auto-deploy after dev)
- **production** — add required reviewers (manual approval before prod deploy)

### 5. Update fabric.yml

Replace the placeholder GUIDs in `fabric.yml`:

```yaml
variables:
  capacity_id:
    description: "Fabric capacity GUID"
    # Set via GitHub Actions variable FAB_CAPACITY_ID
```

Update the Entra ID group GUIDs in the security section:

```yaml
security:
  roles:
    - name: data_engineers
      entra_group: "YOUR-ENTRA-GROUP-GUID"  # Replace
```

### 6. Push and Deploy

```bash
# First deploy (creates workspaces + all resources)
git push origin main

# Subsequent changes via PR
git checkout -b feature/update-etl
# ... make changes to notebooks/fabric.yml ...
git push origin feature/update-etl
# Open PR → CI validates → merge → CD deploys
```

## Local Development

```bash
# Install
pip install fabric-automation-bundles

# Authenticate
az login

# Validate
fab-bundle validate

# Plan (see what would change)
fab-bundle plan -t dev

# Deploy to dev
fab-bundle deploy -t dev

# Run a notebook
fab-bundle run ingest_to_bronze -t dev

# Check status
fab-bundle status -t dev

# Detect drift
fab-bundle drift -t dev

# Tear down dev
fab-bundle destroy -t dev
```

## Pipeline Flow

```
daily_etl pipeline:
  ingest_to_bronze → bronze_to_silver → silver_to_gold
                                                  ↓
data_quality pipeline:                    data_quality_checks
```

The `daily_etl` pipeline runs on a cron schedule (`0 6 * * *` — 6am daily).
In the test environment, `data_quality_checks` runs automatically after deployment as a post-deploy validation.

## License

MIT
