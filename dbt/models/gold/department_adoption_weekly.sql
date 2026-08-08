select
    year_cohort,
    date_trunc('week', event_date) as week_start_date,
    count(distinct student_id) as active_students,
    count(distinct student_id) as observed_students,
    1.0 as adoption_rate,
    sum(case when source = 'lms' then 1 else 0 end) as online_event_count,
    sum(case when source = 'campus' then 1 else 0 end) as offline_event_count
from (
    select year_cohort, event_date, student_id, source from {{ ref('silver_student_events') }}
    union all
    select year_cohort, event_date, student_id, source from {{ ref('silver_offline_events') }}
) activity
group by year_cohort, date_trunc('week', event_date)
