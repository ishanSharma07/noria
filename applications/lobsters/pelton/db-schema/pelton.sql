CREATE TABLE users ( id int NOT NULL PRIMARY KEY, PII_username varchar(50), email varchar(100), password_digest varchar(75), created_at datetime, is_admin int, password_reset_token varchar(75), session_token varchar(75) NOT NULL, about text, invited_by_user_id int, is_moderator int, pushover_mentions int, rss_token varchar(75), mailing_list_token varchar(75), mailing_list_mode int, karma int NOT NULL, banned_at datetime, banned_by_user_id int, banned_reason varchar(200), deleted_at datetime, disabled_invite_at datetime, disabled_invite_by_user_id int, disabled_invite_reason varchar(200), settings text, FOREIGN KEY (banned_by_user_id) REFERENCES users(id), FOREIGN KEY (invited_by_user_id) REFERENCES users(id), FOREIGN KEY (disabled_invite_by_user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE comments ( id int NOT NULL PRIMARY KEY, created_at datetime NOT NULL, updated_at datetime, short_id varchar(10) NOT NULL, story_id int NOT NULL, user_id int NOT NULL, parent_comment_id int, thread_id int, comment text NOT NULL, upvotes int NOT NULL, downvotes int NOT NULL, confidence int NOT NULL, markeddown_comment text, is_deleted int, is_moderated int, is_from_email int, hat_id int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX comments_short_index ON comments(short_id);
CREATE TABLE hat_requests ( id int NOT NULL PRIMARY KEY, created_at datetime, updated_at datetime, user_id int, hat varchar(255), link varchar(255), comment text) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE hats ( id int NOT NULL PRIMARY KEY, created_at datetime, updated_at datetime, OWNER_user_id int, OWNER_granted_by_user_id int, hat varchar(255) NOT NULL, link varchar(255), modlog_use int, doffed_at datetime, FOREIGN KEY (OWNER_user_id) REFERENCES users(id), FOREIGN KEY (OWNER_granted_by_user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE hidden_stories ( id int NOT NULL PRIMARY KEY, user_id int, story_id int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE invitation_requests ( id int NOT NULL PRIMARY KEY, code varchar(255), is_verified int, PII_email varchar(255), name varchar(255), memo text, ip_address varchar(255), created_at datetime NOT NULL, updated_at datetime NOT NULL) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE invitations ( id int NOT NULL PRIMARY KEY, OWNER_user_id int, OWNER_email varchar(255), code varchar(255), created_at datetime NOT NULL, updated_at datetime NOT NULL, memo text, FOREIGN KEY (OWNER_user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE keystores ( keyX varchar(50) NOT NULL PRIMARY KEY, valueX int) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE messages ( id int NOT NULL PRIMARY KEY, created_at datetime, OWNER_author_user_id int, OWNER_recipient_user_id int, has_been_read int, subject varchar(100), body text, short_id varchar(30), deleted_by_author int, deleted_by_recipient int, FOREIGN KEY (OWNER_author_user_id) REFERENCES users(id), FOREIGN KEY (OWNER_recipient_user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE moderations ( id int NOT NULL PRIMARY KEY, created_at datetime NOT NULL, updated_at datetime NOT NULL, OWNER_moderator_user_id int, story_id int, comment_id int, OWNER_user_id int, `action` text, reason text, is_from_suggestions int, FOREIGN KEY (OWNER_user_id) REFERENCES users(id), FOREIGN KEY (OWNER_moderator_user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE read_ribbons ( id int NOT NULL PRIMARY KEY, is_following int, created_at datetime NOT NULL, updated_at datetime NOT NULL, user_id int, story_id int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE saved_stories ( id int NOT NULL PRIMARY KEY, created_at datetime NOT NULL, updated_at datetime NOT NULL, user_id int, story_id int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE stories ( id int NOT NULL PRIMARY KEY, created_at datetime, user_id int, url varchar(250), title varchar(150) NOT NULL, description text, short_id varchar(6) NOT NULL, is_expired int NOT NULL, upvotes int NOT NULL, downvotes int NOT NULL, is_moderated int NOT NULL, hotness int NOT NULL, markeddown_description text, story_cache text, comments_count int NOT NULL, merged_story_id int, unavailable_at datetime, twitter_id varchar(20), user_is_author int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
-- Need this index for transitive sharding.
CREATE INDEX storiespk ON stories(id);
CREATE INDEX stories_short_index ON stories(short_id);
CREATE TABLE tags ( id int NOT NULL PRIMARY KEY, tag varchar(25) NOT NULL, description varchar(100), privileged int, is_media int, inactive int, hotness_mod int) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE suggested_taggings ( id int NOT NULL PRIMARY KEY, story_id int, tag_id int, user_id int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE suggested_titles ( id int NOT NULL PRIMARY KEY, story_id int, user_id int, title varchar(150) NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE tag_filters ( id int NOT NULL PRIMARY KEY, created_at datetime NOT NULL, updated_at datetime NOT NULL, user_id int, tag_id int, FOREIGN KEY (user_id) REFERENCES users(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE taggings ( id int NOT NULL PRIMARY KEY, story_id int NOT NULL, tag_id int NOT NULL, FOREIGN KEY (tag_id) REFERENCES tags(id), FOREIGN KEY (story_id) REFERENCES stories(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE votes ( id int NOT NULL PRIMARY KEY, OWNER_user_id int NOT NULL, story_id int NOT NULL, comment_id int, vote int NOT NULL, reason varchar(1), FOREIGN KEY (OWNER_user_id) REFERENCES users(id), FOREIGN KEY (story_id) REFERENCES stories(id), FOREIGN KEY (comment_id) REFERENCES comments(id)) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
-- Pelton specific views that the queries in endpoints are hard coded against.
CREATE VIEW q6 AS '"SELECT comments.upvotes, comments.downvotes, comments.story_id FROM comments JOIN stories ON comments.story_id = stories.id WHERE comments.story_id = ? AND comments.user_id != stories.user_id"';
CREATE VIEW q11 AS '"SELECT stories.id, stories.merged_story_id FROM stories WHERE stories.merged_story_id = ?"';
CREATE VIEW q12 AS '"SELECT comments.*, comments.upvotes - comments.downvotes AS saldo FROM comments WHERE comments.story_id = ? ORDER BY saldo ASC, confidence DESC"';
CREATE VIEW q13 AS '"SELECT tags.*, taggings.story_id FROM tags INNER JOIN taggings ON tags.id = taggings.tag_id WHERE taggings.story_id = ?"';
-- rewrote net_votes as arith expr in the WHERE clause
CREATE VIEW q16 AS '"SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER BY hotness ASC LIMIT 51"';
CREATE VIEW q17 AS '"SELECT votes.* FROM votes WHERE votes.comment_id = ?"';
CREATE VIEW q22 AS '"SELECT tags.id, stories.user_id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON taggings.story_id = stories.id WHERE tags.inactive = 0 AND stories.user_id = ? GROUP BY tags.id, stories.user_id ORDER BY `count` DESC LIMIT 1"';
CREATE VIEW q25 AS '"SELECT suggested_titles.* FROM suggested_titles WHERE suggested_titles.story_id = ?"';
CREATE VIEW q26 AS '"SELECT taggings.* FROM taggings WHERE taggings.story_id = ?"';
CREATE VIEW q27 AS '"SELECT 1 AS `one`, hats.OWNER_user_id FROM hats WHERE hats.OWNER_user_id = ? LIMIT 1"';
CREATE VIEW q28 AS '"SELECT suggested_taggings.* FROM suggested_taggings WHERE suggested_taggings.story_id = ?"';
CREATE VIEW q29 AS '"SELECT tags.* FROM tags WHERE tags.id = ?"';
CREATE VIEW q30 AS '"SELECT comments.* FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id DESC LIMIT 40"';
CREATE VIEW q32 AS '"SELECT stories.* FROM stories WHERE stories.id = ?"';
CREATE VIEW q35 AS '"SELECT stories.*, upvotes - downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51"';
-- Needs nested views to support the query as is without flattening views:
-- CREATE VIEW q36 AS '"SELECT COUNT(*) FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = ?"';
CREATE VIEW q36 AS \
	'"SELECT read_ribbons.user_id, COUNT(*) \
	FROM read_ribbons \
	JOIN stories ON (read_ribbons.story_id = stories.id) \
	JOIN comments ON (read_ribbons.story_id = comments.story_id) \
	LEFT JOIN comments AS parent_comments \
	ON (comments.parent_comment_id = parent_comments.id) \
	WHERE read_ribbons.is_following = 1 \
	AND comments.user_id <> read_ribbons.user_id \
	AND comments.is_deleted = 0 \
	AND comments.is_moderated = 0 \
	AND ( comments.upvotes - comments.downvotes ) >= 0 \
	AND read_ribbons.updated_at < comments.created_at \
	AND ( \
     ( \
            parent_comments.user_id = read_ribbons.user_id \
            AND \
            ( parent_comments.upvotes - parent_comments.downvotes ) >= 0 \
     ) \
     OR \
     ( \
            parent_comments.id IS NULL \
            AND \
            stories.user_id = read_ribbons.user_id \
     ) \
     ) GROUP BY read_ribbons.user_id"';
