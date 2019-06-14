--log_analysis1为Hive同步HBase外部表
--./hive下执行
--统计http_method
select
    http_method,
    count(1) as count
from log_analysis1
group by  http_method;

--统计http_status
select
    http_status,
    count(1) as count
from log_analysis1
group by http_status;

--热门请求资源
select
    resource,
    count(resource) as sum
from log_analysis1
group by resource
order by 2 desc
limit 10;

--非正常响应资源
select
    resource,
    http_status,
    count(resource) as sum
from log_analysis1
where http_status <> '200'
group by resource,http_status
order by 3 desc
limit 10;