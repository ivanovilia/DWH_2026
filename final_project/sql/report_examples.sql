-- Execute with: docker compose exec clickhouse clickhouse-client --user cinema --password ... --database cinema
-- Both layers combined without overlap: batch time < T, live time >= T.
SELECT * FROM final_minute ORDER BY minute, content_id LIMIT 30;

SELECT * FROM live_minute ORDER BY minute, content_id LIMIT 30;
SELECT * FROM published_minute ORDER BY minute, content_id LIMIT 30;
SELECT * FROM published_daily_country_tier ORDER BY day,country,subscription_tier;
SELECT * FROM published_top5 ORDER BY day,rank;
SELECT * FROM published_search_words ORDER BY day,frequency DESC LIMIT 30;
SELECT t FROM report_cutover FINAL WHERE id = 1;
