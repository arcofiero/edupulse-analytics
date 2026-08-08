select
    year_cohort,
    persona,
    count(*) as student_count,
    avg(engagement_score) as avg_engagement_score,
    avg(risk_score) as avg_risk_score,
    sum(case when dropout_risk_band = 'high' then 1 else 0 end) as high_risk_students,
    sum(case when dropout_risk_band = 'high' then 1 else 0 end) / count(*) as high_risk_rate,
    avg(attendance_rate) as avg_attendance_rate,
    avg(quiz_accuracy) as avg_quiz_accuracy,
    avg(active_day_count) as avg_active_days
from {{ ref('student_engagement_score') }}
group by year_cohort, persona
