use chrono;
use my;
use my::prelude::*;
use std::future::Future;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
    priming: bool,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    // check that tags are active
    let select_tags = "SELECT  `tags`.* FROM `tags` \
     WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test')";
    println!("{}", select_tags);
    let (mut c, tag) = c
        .first::<_, my::Row>(
            select_tags,
        )
        .await?;
    let tag = tag.unwrap().get::<u32, _>("id");

    if !priming {
        // check that story id isn't already assigned
        let select_stories = "SELECT  1 AS one FROM `stories` \
         WHERE `stories`.`short_id` = ?";
        let log_query = select_stories.replace("?",&format!("'{}'", ::std::str::from_utf8(&id[..]).unwrap()));
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_stories,
                (::std::str::from_utf8(&id[..]).unwrap(),),
            )
            .await?;
    }

    // TODO: check for similar stories if there's a url
    // SELECT  `stories`.*
    // FROM `stories`
    // WHERE `stories`.`url` IN (
    //  'https://google.com/test',
    //  'http://google.com/test',
    //  'https://google.com/test/',
    //  'http://google.com/test/',
    //  ... etc
    // )
    // AND (is_expired = 0 OR is_moderated = 1)

    // TODO
    // real impl queries `tags` and `users` again here..?

    // TODO: real impl checks *new* short_id and duplicate urls *again*
    // TODO: sometimes submit url

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let insert_stories = "INSERT INTO `stories` \
     (`created_at`, `user_id`, `title`, \
     `description`, `short_id`, `upvotes`, `hotness`, \
     `markeddown_description`,\
     `url`, `is_expired`, `downvotes`, `is_moderated`, `comments_count`,\
     `story_cache`, `merged_story_id`, `unavailable_at`, `twitter_id`, `user_is_author`) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)";
    let log_query = insert_stories
    .replacen("?", &format!("'{}'", &(chrono::Local::now().naive_local()).to_string()), 1)
    .replacen("?", &user.to_string(), 1)
    .replacen("?", &format!("'{}'", &title), 1)
    .replacen("?", "'to infinity'", 1)
    .replacen("?", &format!("'{}'", ::std::str::from_utf8(&id[..]).unwrap()), 1)
    .replacen("?", "1", 1)
    .replacen("?", "-19217", 1)
    .replacen("?", "'<p>to infinity</p>\\n'", 1);
    println!("{}", log_query);
    let q = c
        .prep_exec(
            insert_stories,
            (
                chrono::Local::now().naive_local(),
                user,
                title,
                "to infinity", // lorem ipsum?
                ::std::str::from_utf8(&id[..]).unwrap(),
                1,
                -19217,
                "<p>to infinity</p>\n",
            ),
        )
        .await?;
    let story = q.last_insert_id().unwrap();
    let mut c = q.drop_result().await?;

    let insert_taggings = "INSERT INTO `taggings` (`story_id`, `tag_id`) \
     VALUES (?, ?)";
    let mut log_query = insert_taggings
    .replacen("?", &story.to_string(), 1)
    .replacen("?", &tag.unwrap().to_string(), 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            insert_taggings,
            (story, tag),
        )
        .await?;

    let key = format!("'user:{}:stories_submitted'", user);
    let insert_keystore = "INSERT INTO keystores (`key`, `value`) \
     VALUES (?, ?) \
     ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1";
    log_query = insert_keystore
    .replacen("?", &key, 1)
    .replacen("?", "1", 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            insert_keystore,
            (key, 1),
        )
        .await?;

    if !priming {
        let key = format!("user:{}:stories_submitted", user);
        let select_keystore = "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = ?";
        log_query = select_keystore.replace("?", &key);
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_keystore,
                (key,),
            )
            .await?;

        let select_votes = "SELECT  `votes`.* FROM `votes` \
         WHERE `votes`.`user_id` = ? \
         AND `votes`.`story_id` = ? \
         AND `votes`.`comment_id` IS NULL";
        log_query = select_votes
        .replacen("?", &user.to_string(), 1)
        .replacen("?", &story.to_string(), 1);
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_votes,
                (user, story),
            )
            .await?;
    }

    let insert_votes = "INSERT INTO `votes` \
     (`user_id`, `story_id`, `vote`, `comment_id`, `reason`) \
     VALUES (?, ?, ?, NULL, NULL)";
    log_query = insert_votes
    .replacen("?", &user.to_string(), 1)
    .replacen("?", &story.to_string(), 1)
    .replacen("?", "1", 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            insert_votes,
            (user, story, 1),
        )
        .await?;

    if !priming {
        let select_comments = "SELECT \
         `comments`.`upvotes`, \
         `comments`.`downvotes` \
         FROM `comments` \
         JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
         WHERE `comments`.`story_id` = ? \
         AND `comments`.`user_id` <> `stories`.`user_id`";
        log_query = select_comments.replace("?", &story.to_string());
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_comments,
                (story,),
            )
            .await?;

        // why oh why is story hotness *updated* here?!
        let update_hotness = "UPDATE `stories` \
         SET `hotness` = ? \
         WHERE `stories`.`id` = ?";
        log_query = update_hotness
        .replacen("?", "-19217", 1)
        .replacen("?", &story.to_string(), 1);
        println!("{}", log_query);
        c = c
            .drop_exec(
                update_hotness,
                (-19217, story),
            )
            .await?;
    }

    Ok((c, false))
}
