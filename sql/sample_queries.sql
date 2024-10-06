SELECT userID, LENGTH(post) as post_length FROM user_posts_info ORDER BY post_length DESC LIMIT 5;
SELECT userID, AVG(LENGTH(title) + LENGTH(body)) AS avg_post_length FROM user_posts_info WHERE userID IN (SELECT userID FROM user_posts_info ORDER BY LENGTH(body) DESC LIMIT 5) GROUP BY userID;
SELECT EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(1609459200 + (id * 86400))) AS day_of_week, COUNT(*) AS post_count FROM user_posts_info WHERE status = 'lengthy' GROUP BY day_of_week ORDER BY post_count DESC LIMIT 1;
