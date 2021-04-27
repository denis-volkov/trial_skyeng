-- user_id, page, happened_at
-- 1 rooms.homework-showcase
-- 2 rooms.view.step.content
-- 3 rooms.lesson.rev.step.content

select row_number() over() as id, user_id, start_session, end_session, datediff(minute, start_session, end_session) as duration, date_part(hour, start_session) as start_session_in_hour,
    avg(duration) over() as avg_duration, avg(duration) over(partition by start_session_in_hour) as avg_duration_in_hour
from(
select any_value(user_id) as user_id, any_value(start_session) as start_session, any_value(end_session) as end_session, count(*) as count_target_page
from (
    select *,
        case
            when page = 'rooms.view.step.content' and sum(num3) over(partition by num_session order by happened_at rows between current row and unbounded following) >= 1
            and sum(num1) over(partition by num_session order by happened_at rows unbounded preceding) >= 1 then 1
        end as num2
    from (
        select *, 
            case
                when page = 'rooms.lesson.rev.step.content' then row_number() over(partition by num_session,page order by happened_at desc)
            end as num3,
            case
                when page = 'rooms.homework-showcase' then row_number() over(partition by num_session,page order by happened_at)
            end as num1,
            min(happened_at) over(partition by num_session) as start_session,
            dateadd(m, 59, max(happened_at) over(partition by num_session)) as end_session
            from (
                select *, user_id + sum(session) over(partition by user_id order by happened_at desc rows unbounded preceding) as num_session
                from (
                    select *,
                        case
                            when datediff(minute, happened_at, lag(happened_at) over(partition by user_id order by happened_at desc)) < 60 then 0
                            else 1
                        end as session
                    from test.vimbox_pages))))
where num1 = 1 or num2 = 1 or num3 = 1
group by num_session
having count_target_page >= 3
)
