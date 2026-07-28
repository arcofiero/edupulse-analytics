# Project Process

This project moves in small, reviewable milestones. Each milestone should leave the
repository in a runnable state and make the next layer of the analytics platform
easier to build.

## Branch Flow

1. Keep `main` stable.
2. Create a focused branch for each milestone.
3. Make scoped commits that explain the intent of the change.
4. Open the branch for review.
5. Merge into `main` only after review and validation pass.

## Milestone Shape

Each milestone should have one clear outcome. Good milestone examples:

- Simulator foundation
- Avro schema contracts
- Kafka producer integration
- Bronze Delta writer
- Flink session stitching
- dbt Silver models
- Soda quality checks
- Airflow orchestration
- Superset dashboard provisioning

Avoid mixing unrelated layers in one branch. For example, do not combine dashboard
work with simulator behavior unless the dashboard change depends directly on that
simulator output.

## Definition of Ready

A milestone is ready to start when it has:

- A short goal statement
- The expected files or components to touch
- The command that will verify the change
- Any external dependency clearly named, such as Confluent Cloud, S3, or Docker

## Definition of Done

A branch is ready for review when:

- The implementation matches the milestone goal
- Relevant tests or smoke checks pass
- Configuration examples are updated when new settings are introduced
- No secrets or generated local artifacts are committed
- README or docs are updated when behavior changes

## Validation Commands

Use the narrowest command that proves the change:

```bash
pytest tests/ -v
```

```bash
make simulate
```

```bash
make simulate-backfill
```

```bash
docker compose up -d
```

If a command cannot run because credentials or external services are missing, note
that in the review summary and include the closest local substitute.

## Review Checklist

Before merging, review for:

- Correct behavior for the stated milestone
- Tests for core paths and expected failures
- Clear handling of malformed or late data
- Settings read from environment variables, not hardcoded secrets
- Small enough scope to understand confidently
- Documentation that matches the actual commands

## Local Configuration

Runtime credentials should live in `.env` and stay out of version control. When a
new environment variable is required, add it to `.env.example` with an empty or
safe placeholder value.

## Commit Guidance

Prefer commits that describe the user-facing or system-facing change:

```text
Add simulator event models
```

```text
Validate student events against Avro schemas
```

```text
Route malformed events to DLQ topic
```

Keep formatting-only changes separate from behavioral changes when practical.
