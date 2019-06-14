--各区域PV
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  sum(count) as value,
  provice as metric
FROM ip
WHERE 1 = 1
group by provice
ORDER BY value desc
limit 20

--总PV:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  sum(count) as value
FROM ip
WHERE 1 = 1

--总UV：
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count as value
FROM uv
WHERE 1 = 1


--网络运营商统计:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  sum(count) as value,
  isp as metric
FROM ip
WHERE  1 = 1
group by isp
ORDER BY 2 desc
limit 10

--客户端分布:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count as value,
  name as metric
FROM client
WHERE  1 = 1

--联通运营商区域分布:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count as value,
  provice as metric
FROM ip
WHERE isp = '联通'
ORDER BY 2

--联通运营商区域分布:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count as value,
  provice as metric
FROM ip
WHERE isp = '电信'
ORDER BY 2

--一月中PV排名前20时间点：
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count(time_stamp) as value,
  substring(time_stamp,12) as metric
from time
group by time_stamp
order by value desc
limit 20

--一天中PV排名前20时间点：
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count(time_stamp) as value,
  substring(time_stamp,12) as metric
from time2
group by time_stamp
order by value desc

--http响应状态码统计:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count as value,
  status_code as metric
FROM http_status
WHERE  1 = 1
ORDER BY 2 desc
limit 20

----http请求方式统计:
SELECT
  UNIX_TIMESTAMP(now()) as time_sec,
  count as value,
  method_name as metric
FROM http_method
WHERE  1 = 1
ORDER BY 2 desc

--热门请求资源：
select
  name as 资源地址,
  count as 请求次数
from resource
where 1 = 1

--非正常响应资源：
select
   name as 非正常响应资源,
   state_code as 状态码,
   count as 非正常响应次数
from unusual_resource