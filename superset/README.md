# Superset Dashboard Provisioning

EduPulse exposes three stakeholder dashboards:

- Advisor Risk Monitor
- Faculty Content Engagement
- Department Adoption Weekly

Run the local pipeline and prepare dashboard datasets:

```bash
make dashboards-local
```

Generated CSV datasets and the manifest are written to `.local/superset`.

The dashboard chart definitions live in `superset/dashboard_specs.yml`. These
specifications map the Gold outputs to Superset datasets and charts; once Superset
is running, the CSV outputs can be registered as datasets for dashboard assembly.
