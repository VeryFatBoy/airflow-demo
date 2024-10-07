USE airflow_demo;

DROP TABLE IF EXISTS event_stats_staging;

CREATE TABLE event_stats_staging AS
    SELECT date, user_id, SUM(spend_amt) AS total_spend_amt
    FROM event
    WHERE date = '{{ ds }}' AND user_id <> {{ params.test_user_id }}
    GROUP BY date, user_id;

INSERT INTO event_stats (date, user_id, total_spend_amt)
    SELECT date, user_id, total_spend_amt
    FROM event_stats_staging
    ON DUPLICATE KEY UPDATE total_spend_amt = VALUES(total_spend_amt);

DROP TABLE IF EXISTS event_stats_staging;
