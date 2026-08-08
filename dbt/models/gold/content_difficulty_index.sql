select
    course_id,
    count(distinct student_id) as unique_students,
    count(*) as total_events,
    sum(case when event_type = 'quiz_attempt' then 1 else 0 end) as quiz_attempts,
    sum(case when event_type = 'quiz_answer' then 1 else 0 end) as quiz_answers,
    avg(case when event_type = 'quiz_answer' then cast(properties['correct'] as int) end) as quiz_accuracy,
    sum(case when event_type like 'video_%' then 1 else 0 end) as video_events,
    sum(case when event_type = 'assignment_submit' then 1 else 0 end) as assignment_submits,
    sum(case when event_type in ('forum_post', 'forum_reply') then 1 else 0 end) as forum_events,
    case
        when avg(case when event_type = 'quiz_answer' then cast(properties['correct'] as int) end) < 0.55 then 'needs_review'
        when avg(case when event_type = 'quiz_answer' then cast(properties['correct'] as int) end) < 0.75 then 'watch'
        else 'healthy'
    end as content_health_band
from {{ ref('silver_student_events') }}
group by course_id
