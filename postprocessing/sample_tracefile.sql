SELECT comments.* FROM comments WHERE comments.story_id = 5 AND comments.short_id = 10
SELECT comments.* FROM comments WHERE comments.story_id = 15 AND comments.short_id = 20
SELECT 1 AS `one` FROM users WHERE users.PII_username = 'asdf'
SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER BY hotness ASC LIMIT 51
SELECT tags.id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON stories.id = taggings.story_id WHERE tags.inactive = 0 AND stories.user_id = 99 GROUP BY tags.id ORDER BY `count` DESC LIMIT 1
SELECT tags.* FROM tags WHERE tags.inactive = 0 AND tags.tag IN ('test')
