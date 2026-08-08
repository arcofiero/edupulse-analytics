select
    student_id,
    year_cohort,
    persona,
    risk_score,
    dropout_risk_band,
    engagement_score,
    active_day_count,
    attendance_rate,
    quiz_accuracy,
    assignment_submit_count,
    forum_event_count,
    case
        when risk_score >= 55 then 'Advisor outreach'
        when risk_score >= 30 then 'Monitor and nudge'
        else 'Monitor'
    end as recommended_action
from {{ ref('student_engagement_score') }}
