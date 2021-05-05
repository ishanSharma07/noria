use chrono;
use my;
use my::prelude::*;
use std::future::Future;
use trawler::{CommentId, StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: CommentId,
    story: StoryId,
    parent: Option<CommentId>,
    priming: bool,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();
    let select_stories = "SELECT stories.* \
     FROM stories \
     WHERE stories.short_id = ?";
    let mut log_query = select_stories.replace("?",&format!("'{}'", ::std::str::from_utf8(&story[..]).unwrap()));
    println!("{}", log_query);
    let (mut c, story) = c
        .first_exec::<_, _, my::Row>(
            select_stories,
            (::std::str::from_utf8(&story[..]).unwrap(),),
        )
        .await?;
    let story = story.unwrap();
    let author = story.get::<u32, _>("user_id").unwrap();
    let hotness = story.get::<i64, _>("hotness").unwrap();
    let story = story.get::<u32, _>("id").unwrap();

    if !priming {
        let select_users = "SELECT users.* FROM users WHERE users.id = ?";
        log_query = select_users.replace("?", &author.to_string());
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_users,
                (author,),
            )
            .await?;
    }

    let parent = if let Some(parent) = parent {
        // check that parent exists
        let select_comments = "SELECT  comments.* FROM comments \
         WHERE comments.story_id = ? \
         AND comments.short_id = ?";
        log_query = select_comments.replacen("?", &story.to_string(), 1);
        log_query = log_query.replacen("?", &format!("'{}'", ::std::str::from_utf8(&parent[..]).unwrap()), 1);
        println!("{}", log_query);
        let (x, p) = c
            .first_exec::<_, _, my::Row>(
                select_comments,
                (story, ::std::str::from_utf8(&parent[..]).unwrap()),
            )
            .await?;
        c = x;

        if let Some(p) = p {
            Some((
                p.get::<u32, _>("id").unwrap(),
                p.get::<Option<u32>, _>("thread_id").unwrap(),
            ))
        } else {
            eprintln!(
                "failed to find parent comment {} in story {}",
                ::std::str::from_utf8(&parent[..]).unwrap(),
                story
            );
            None
        }
    } else {
        None
    };

    // TODO: real site checks for recent comments by same author with same
    // parent to ensure we don't double-post accidentally

    if !priming {
        let select_one = "SELECT  1 AS `one` FROM comments \
         WHERE comments.short_id = ?";
        log_query = select_one.replace("?", &format!("'{}'", ::std::str::from_utf8(&id[..]).unwrap()));
        println!("{}", log_query);
        // check that short id is available
        c = c
            .drop_exec(
                select_one,
                (::std::str::from_utf8(&id[..]).unwrap(),),
            )
            .await?;
    }

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let q = if let Some((parent, thread)) = parent {
        let insert_comments = "INSERT INTO comments \
         (created_at, updated_at, short_id, story_id, \
         user_id, parent_comment_id, thread_id, \
         comment, upvotes, confidence, \
         markeddown_comment,\
         downvotes, is_deleted, is_moderated, is_from_email, hat_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, NULL)";
        let r = c.prep_exec(
            insert_comments,
            (
                now,
                now,
                ::std::str::from_utf8(&id[..]).unwrap(),
                story,
                user,
                parent,
                thread,
                "moar benchmarking", // lorem ipsum?
                1,
                1,
                "<p>moar benchmarking</p>\n",
            ),
        )
        .await?;
        let comment_id = r.last_insert_id().unwrap();
        log_query = format!("INSERT INTO comments \
         (id, created_at, updated_at, short_id, story_id, \
         user_id, parent_comment_id, thread_id, \
         comment, upvotes, confidence, \
         markeddown_comment,\
         downvotes, is_deleted, is_moderated, is_from_email, hat_id) \
         VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {}, '{}', {}, '{}', '{}', 0, 0, 0, 0, NULL)",
         comment_id, now, now, ::std::str::from_utf8(&id[..]).unwrap(), story, user, parent,
         thread.map(|x| x.to_string()).unwrap_or("NULL".to_string()), "moar benchmarking", 1, 1, "<p>moar benchmarking</p>");
        println!("{}", log_query);
        r
    } else {
        let insert_comments = "INSERT INTO comments \
         (created_at, updated_at, short_id, story_id, \
         user_id, comment, upvotes, confidence, \
         markeddown_comment, downvotes, is_deleted, is_moderated, \
         is_from_email, hat_id, parent_comment_id, thread_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, NULL, NULL, NULL)";
        let r = c.prep_exec(
            insert_comments,
            (
                now,
                now,
                ::std::str::from_utf8(&id[..]).unwrap(),
                story,
                user,
                "moar benchmarking", // lorem ipsum?
                1,
                1,
                "<p>moar benchmarking</p>\n",
            ),
        )
        .await?;
        let comment_id = r.last_insert_id().unwrap();
        log_query = format!("INSERT INTO comments \
         (id, created_at, updated_at, short_id, story_id, \
         user_id, parent_comment_id, thread_id, \
         comment, upvotes, confidence, \
         markeddown_comment,\
         downvotes, is_deleted, is_moderated, is_from_email, hat_id) \
         VALUES ({}, '{}', '{}', '{}', {}, {}, NULL, NULL, '{}', {}, '{}', '{}', 0, 0, 0, 0, NULL)",
         comment_id, now, now, ::std::str::from_utf8(&id[..]).unwrap(), story, user,
         "moar benchmarking", 1, 1, "<p>moar benchmarking</p>");
        println!("{}", log_query);
        r

    };
    let comment = q.last_insert_id().unwrap();
    let mut c = q.drop_result().await?;

    if !priming {
        // but why?!
        let select_votes = "SELECT  votes.* FROM votes \
         WHERE votes.user_id = ? \
         AND votes.story_id = ? \
         AND votes.comment_id = ?";
        log_query = select_votes.replacen("?", &user.to_string(), 1);
        log_query = log_query.replacen("?", &story.to_string(), 1);
        log_query = log_query.replacen("?", &comment.to_string(), 1);
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_votes,
                (user, story, comment),
            )
            .await?;
    }

    let insert_votes = "INSERT INTO votes \
     (OWNER_user_id, story_id, comment_id, vote, reason) \
     VALUES (?, ?, ?, ?, NULL)";
    c = c
        .drop_exec(
            insert_votes,
            (user, story, comment, 1),
        )
        .await?;
    let vote_insert_id = c.last_insert_id().unwrap();
    log_query = format!("INSERT INTO votes \
     (id, OWNER_user_id, story_id, comment_id, vote, reason) \
     VALUES \
     ({}, {}, {}, {}, {}, NULL)", vote_insert_id, user, story,
     comment, 1);
    println!("{}", log_query);

    let select_storiesv2 = "SELECT stories.id \
     FROM stories \
     WHERE stories.merged_story_id = ?";
    log_query = select_storiesv2.replace("?", &story.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_storiesv2,
            (story,),
        )
        .await?;

    let select_commentsv2 = "SELECT comments.* \
     FROM comments \
     WHERE comments.story_id = ? \
     ORDER BY \
     (upvotes - downvotes) < 0 ASC, \
     confidence DESC";
    log_query = select_commentsv2.replace("?", &story.to_string());
    println!("{}", log_query);
    // why are these ordered?
    let (mut c, count) = c
        .prep_exec(
            select_commentsv2,
            (story,),
        )
        .await?
        .reduce_and_drop(0, |rows, _| rows + 1)
        .await?;

    let udpate_stories = "UPDATE stories SET comments_count = ? WHERE stories.id = ?";
    log_query = udpate_stories.replacen("?", &count.to_string(), 1);
    log_query = log_query.replacen("?", &story.to_string(), 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            udpate_stories,
            (count, story),
        )
        .await?;

    if !priming {
        // get all the stuff needed to compute updated hotness
        let select_tags = "SELECT tags.* \
         FROM tags \
         INNER JOIN taggings \
         ON tags.id = taggings.tag_id \
         WHERE taggings.story_id = ?";
        log_query = select_tags.replace("?", &story.to_string());
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_tags,
                (story,),
            )
            .await?;
        let select_commentsv3 = "SELECT \
         comments.upvotes, \
         comments.downvotes \
         FROM comments \
         JOIN stories ON stories.id = comments.story_id \
         WHERE comments.story_id = ? \
         AND comments.user_id != stories.user_id";
        log_query = select_commentsv3.replace("?", &story.to_string());
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_commentsv3,
                (story,),
            )
            .await?;
        let select_storiesv3 = "SELECT stories.id \
         FROM stories \
         WHERE stories.merged_story_id = ?";
        log_query = select_storiesv3.replace("?", &story.to_string());
        println!("{}", log_query);
        c = c
            .drop_exec(
                select_storiesv3,
                (story,),
            )
            .await?;
    }

    // why oh why is story hotness *updated* here?!
    let update_stories = "UPDATE stories \
     SET hotness = ? \
     WHERE stories.id = ?";
    log_query = update_stories.replacen("?", &(hotness - 1).to_string(), 1);
    log_query = log_query.replacen("?", &story.to_string(), 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            update_stories,
            (hotness - 1, story),
        )
        .await?;

    let key = format!("'user:{}:comments_posted'", user);
    let insert_keystore = "REPLACE INTO keystores (keyX, valueX) \
     VALUES (?, ?)"; // \
//     ON DUPLICATE KEY UPDATE keystores.valueX = keystores.valueX + 1";
    log_query = insert_keystore.replacen("?", &key, 1);
    log_query = log_query.replacen("?", "1", 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            insert_keystore,
            (key, 1),
        )
        .await?;

    Ok((c, false))
}
