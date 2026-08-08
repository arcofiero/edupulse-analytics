# Superset Dashboard Provisioning

EduPulse exposes three stakeholder dashboards:

- Advisor Risk Monitor
- Faculty Content Engagement
- Department Adoption Weekly

Run the local pipeline and prepare dashboard datasets:

```bash
make dashboards-local
```

Generated CSV datasets, a SQLite database, and the manifest are written to
`.local/superset`.

The dashboard chart definitions live in `superset/dashboard_specs.yml`. These
specifications map the Gold outputs to Superset datasets and charts. The SQLite
database is mounted into the Superset container at:

```text
/app/edupulse_superset_data/edupulse_dashboards.db
```

Use this SQLAlchemy URI when registering the local analytics database in Superset:

```text
sqlite:////app/edupulse_superset_data/edupulse_dashboards.db
```

The local Superset container provisions the analytics database, six datasets,
six charts, and three dashboards on startup via `superset/provision_assets.py`.
The provisioned datasets are:

- `advisor_at_risk_students`
- `advisor_engagement_signals`
- `faculty_content_engagement`
- `faculty_content_difficulty`
- `admin_adoption_weekly`
- `admin_cohort_engagement_summary`
