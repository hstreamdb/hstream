-- Some examples for admin sql

-- Find the top 5 streams that have had the highest throughput in the last 10 minutes.
SELECT streams.name, sum(append_throughput.throughput_10min) AS total_throughput
FROM append_throughput
LEFT JOIN streams ON streams.name = append_throughput.stream_name 
GROUP BY stream_name
ORDER BY total_throughput DESC
LIMIT 0, 5;
