with online as (
    select
        student_id,
        year_cohort,
        persona,
        count(*) as online_event_count,
        sum(
            case event_type
                when 'page_view' then 1
                when 'video_play' then 2
                when 'video_pause' then 0.5
                when 'video_seek' then 0.5
                when 'quiz_attempt' then 4
                when 'quiz_answer' then 2
                when 'assignment_submit' then 8
                when 'forum_post' then 5
                when 'forum_reply' then 3
                else 1
            end
        ) as online_score
    from {{ ref('silver_student_events') }}
    group by student_id, year_cohort, persona
),
attendance as (
    select
        student_id,
        count(*) as attendance_count
    from {{ ref('silver_offline_events') }}
    where event_type = 'attendance'
    group by student_id
)
select
    online.student_id,
    online.year_cohort,
    online.persona,
    online.online_event_count,
    coalesce(attendance.attendance_count, 0) as attendance_count,
    online.online_score + (coalesce(attendance.attendance_count, 0) * 2) as engagement_score,
    case
        when online.online_score < 12 or online.online_event_count < 4 then 'high'
        when online.online_score < 35 then 'medium'
        else 'low'
    end as dropout_risk_band
from online
left join attendance on online.student_id = attendance.student_id
