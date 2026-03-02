INSERT INTO arroyo_output_sink
SELECT COUNT(*) as count, labels.hostname as hostname, TUMBLE(INTERVAL '5 seconds') as window
FROM arroyo_input_source
GROUP BY hostname, window
