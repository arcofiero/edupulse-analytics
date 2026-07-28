select
    course_id,
    event_type,
    count(*) as event_count,
    count(distinct student_id) as unique_students
from {{ ref('silver_student_events') }}
group by course_id, event_type
