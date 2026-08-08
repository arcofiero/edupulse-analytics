with online as (
    select
        student_id,
        year_cohort,
        persona,
        count(*) as online_event_count,
        count(distinct event_date) as active_day_count,
        sum(case when event_type = 'assignment_submit' then 1 else 0 end) as assignment_submit_count,
        sum(case when event_type = 'forum_post' or event_type = 'forum_reply' then 1 else 0 end) as forum_event_count,
        sum(case when event_type = 'quiz_answer' then 1 else 0 end) as quiz_answer_count,
        avg(case when event_type = 'quiz_answer' then cast(properties['correct'] as int) end) as quiz_accuracy,
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
        count(*) as attendance_total,
        sum(case when cast(properties['present'] as boolean) then 1 else 0 end) as attendance_count
    from {{ ref('silver_offline_events') }}
    where event_type = 'attendance'
    group by student_id
)
select
    online.student_id,
    online.year_cohort,
    online.persona,
    online.online_event_count,
    online.active_day_count,
    coalesce(attendance.attendance_count, 0) as attendance_count,
    coalesce(attendance.attendance_count, 0) / nullif(attendance.attendance_total, 0) as attendance_rate,
    coalesce(online.quiz_accuracy, 0) as quiz_accuracy,
    online.assignment_submit_count,
    online.forum_event_count,
    online.online_score + (coalesce(attendance.attendance_count, 0) * 2) as engagement_score,
    case
        when online.online_event_count < 4 then 60
        when coalesce(attendance.attendance_count, 0) / nullif(attendance.attendance_total, 1) < 0.7 then 55
        when online.online_score < 35 then 35
        else 15
    end as risk_score,
    case
        when online.online_event_count < 4 then 'high'
        when online.online_score < 35 then 'medium'
        else 'low'
    end as dropout_risk_band
from online
left join attendance on online.student_id = attendance.student_id
