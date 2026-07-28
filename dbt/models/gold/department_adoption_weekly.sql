select
    year_cohort,
    date_trunc('week', event_date) as week_start_date,
    count(distinct student_id) as active_students
from (
    select year_cohort, event_date, student_id from {{ ref('silver_student_events') }}
    union all
    select year_cohort, event_date, student_id from {{ ref('silver_offline_events') }}
) activity
group by year_cohort, date_trunc('week', event_date)
