select
    event_id,
    event_ts,
    produced_ts,
    cast(substr(event_ts, 1, 10) as date) as event_date,
    student_id,
    event_type,
    course_id,
    year_cohort,
    persona,
    source,
    properties
from {{ source('bronze', 'bronze_offline_events') }}
where is_malformed = false
