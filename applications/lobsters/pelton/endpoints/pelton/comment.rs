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
    let (mut c, story) = c
        .first_exec::<_, _, my::Row>(
            "SELECT stories.* \
             FROM stories \
             WHERE stories.short_id = ?",
            (::std::str::from_utf8(&story[..]).unwrap(),),
        )
        .await?;
    let story = story.unwrap();
    let author = story.get::<u32, _>("user_id").unwrap();
    let hotness = story.get::<i64, _>("hotness").unwrap();
    let story = story.get::<u32, _>("id").unwrap();

    if !priming {
        c = c
            .drop_exec(
                "SELECT users.* FROM users WHERE users.id = ?",
                (author,),
            )
            .await?;
    }

    let parent = if let Some(parent) = parent {
        // check that parent exists
        let (x, p) = c
            .first_exec::<_, _, my::Row>(
                "SELECT comments.* FROM comments \
                 WHERE comments.story_id = ? \
                 AND comments.short_id = ?",
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
        // check that short id is available
        c = c
            .drop_exec(
                "SELECT 1 AS `one`, short_id FROM comments \
                 WHERE comments.short_id = ?",
                (::std::str::from_utf8(&id[..]).unwrap(),),
            )
            .await?;
    }

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let q = if let Some((parent, thread)) = parent {
        c.prep_exec(
            "INSERT INTO comments \
             (created_at, updated_at, short_id, story_id, \
             user_id, parent_comment_id, thread_id, \
             comment, upvotes, confidence, \
             markeddown_comment,\
             downvotes, is_deleted, is_moderated, is_from_email, hat_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, NULL)",
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
        .await?
    } else {
        c.prep_exec(
            "INSERT INTO comments \
             (created_at, updated_at, short_id, story_id, \
             user_id, comment, upvotes, confidence, \
             markeddown_comment, downvotes, is_deleted, is_moderated, \
             is_from_email, hat_id, parent_comment_id, thread_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, NULL, NULL, NULL)",
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
        .await?
    };
    let comment = q.last_insert_id().unwrap();
    let mut c = q.drop_result().await?;

    if !priming {
        // but why?!
        c = c
            .drop_exec(
                "SELECT votes.* FROM votes \
                 WHERE votes.OWNER_user_id = ? \
                 AND votes.story_id = ? \
                 AND votes.comment_id = ?",
                (user, story, comment),
            )
            .await?;
    }

    c = c
        .drop_exec(
            "INSERT INTO votes \
             (OWNER_user_id, story_id, comment_id, vote, reason) \
             VALUES (?, ?, ?, ?, NULL)",
            (user, story, comment, 1),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT * FROM q11 \
             WHERE merged_story_id = ?",
            (story,),
        )
        .await?;

    // why are these ordered?
    let (mut c, count) = c
        .prep_exec(
            "SELECT * \
             FROM q12 \
             WHERE story_id = ?",
            (story,),
        )
        .await?
        .reduce_and_drop(0, |rows, _| rows + 1)
        .await?;

    c = c
        .drop_exec(
            "UPDATE stories SET comments_count = ? WHERE stories.id = ?",
            (count, story),
        )
        .await?;

    if !priming {
        // get all the stuff needed to compute updated hotness
        c = c
            .drop_exec(
                "SELECT * FROM q13 \
                 WHERE story_id = ?",
                (story,),
            )
            .await?;

        c = c
            .drop_exec(
                "SELECT * FROM q6 \
                 WHERE story_id = ?",
                (story,),
            )
            .await?;

        c = c
            .drop_exec(
                "SELECT * FROM q11 \
                 WHERE merged_story_id = ?",
                (story,),
            )
            .await?;
    }

    // why oh why is story hotness *updated* here?!
    c = c
        .drop_exec(
            "UPDATE stories \
             SET hotness = ? \
             WHERE stories.id = ?",
            (hotness - 1, story),
        )
        .await?;

    let key = format!("user:{}:comments_posted", user);
    c = c
        .drop_exec(
            "REPLACE INTO keystores (keyX, valueX) \
             VALUES (?, ?)",
            (key, 1),
        )
        .await?;

    Ok((c, false))
}
