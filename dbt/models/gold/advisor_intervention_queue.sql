select
    row_number() over (
        order by risk_score desc, engagement_score asc, student_id
    ) as queue_rank,
    student_id,
    year_cohort,
    persona,
    risk_score,
    dropout_risk_band,
    engagement_score,
    recommended_action
from {{ ref('student_risk_signals') }}
where dropout_risk_band in ('high', 'medium')
