use chrono;
use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use trawler::{StoryId, UserId};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let mut log_query = format!("--start: story");
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = c.await?;
    let select_stories = "SELECT stories.* \
     FROM stories \
     WHERE stories.short_id = ?";
    let lq = select_stories.replace("?",&format!("'{}'", ::std::str::from_utf8(&id[..]).unwrap()));
    log_query.push_str(&format!("\n{}", lq));
    let (mut c, mut story) = c
        .prep_exec(
            select_stories,
            (::std::str::from_utf8(&id[..]).unwrap(),),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);
    let author = story.get::<u32, _>("user_id").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    let select_users = "SELECT users.* FROM users WHERE users.id = ?";
    log_query.push_str(&format!("\n{}", select_users.replace("?", &author.to_string())));
    c = c
        .drop_exec(
            select_users,
            (author,),
        )
        .await?;

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let select_ribbon = "SELECT read_ribbons.* \
             FROM read_ribbons \
             WHERE read_ribbons.user_id = ? \
             AND read_ribbons.story_id = ?";
        let lq = select_ribbon
        .replacen("?", &uid.to_string(), 1)
        .replacen("?", &story.to_string(), 1);
        log_query.push_str(&format!("\n{}", lq));
        let (x, rr) = c
            .first_exec::<_, _, my::Row>(
                select_ribbon,
                (&uid, &story),
            )
            .await?;
        let now = chrono::Local::now().naive_local();
        c = match rr {
            None => {
                let insert_ribbon = "INSERT INTO read_ribbons \
                     (created_at, updated_at, user_id, story_id, is_following) \
                     VALUES (?, ?, ?, ?, 1)";
                let r = x.drop_exec(
                    insert_ribbon,
                    (now, now, uid, story),
                )
                .await?;
                let id = r.last_insert_id().unwrap();
                let lq = format!("INSERT INTO read_ribbons \
                     (id, created_at, updated_at, user_id, story_id, is_following) \
                     VALUES ({}, '{}', '{}', {}, {}, 1)",
                     id, now, now, uid, story);
                log_query.push_str(&format!("\n{}", lq));
                r
            }
            Some(rr) => {
                let update_ribbon = "UPDATE read_ribbons \
                     SET read_ribbons.updated_at = ? \
                     WHERE read_ribbons.id = ?";
                let lq = update_ribbon
                .replacen("?", &format!("'{}'", &now.to_string()), 1)
                .replacen("?", &(rr.get::<u32, _>("id").unwrap()).to_string(), 1);
                log_query.push_str(&format!("\n{}", lq));
                x.drop_exec(
                    update_ribbon,
                    (now, rr.get::<u32, _>("id").unwrap()),
                )
                .await?
            }
        };
    }

    // XXX: probably not drop here, but we know we have no merged stories
    let select_stories = "SELECT id FROM q11 \
     WHERE merged_story_id = ?";
    let lq = select_stories
    .replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            select_stories,
            (story,),
        )
        .await?;

    let select_comments = "SELECT * FROM q12 \
     WHERE story_id = ?";
    let lq = select_comments
    .replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    let comments = c
        .prep_exec(
            select_comments,
            (story,),
        )
        .await?;

    let (mut c, (users, comments)) = comments
        .reduce_and_drop(
            (HashSet::new(), HashSet::new()),
            |(mut users, mut comments), comment| {
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                comments.insert(comment.get::<u32, _>("id").unwrap());
                (users, comments)
            },
        )
        .await?;

    // get user info for all commenters
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    let select_usersv2 = &format!(
        "SELECT users.* FROM users WHERE users.id IN ({})",
        users
    );
    log_query.push_str(&format!("\n{}", select_usersv2));
    c = c
        .drop_query(select_usersv2)
        .await?;

    // get comment votes
    // XXX: why?!
    let comments = comments
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    let select_votes = &format!(
        "SELECT * FROM q17 WHERE comment_id IN ({})",
        comments
    );
    log_query.push_str(&format!("\n{}", select_votes));
    c = c
        .drop_query(select_votes)
        .await?;

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        let select_votesv2 ="SELECT votes.* \
         FROM votes \
         WHERE votes.OWNER_user_id = ? \
         AND votes.story_id = ? \
         AND votes.comment_id IS NULL";
        let lq = select_votesv2
        .replacen("?", &uid.to_string(), 1)
        .replacen("?", &story.to_string(), 1);
        log_query.push_str(&format!("\n{}", lq));
        c = c
            .drop_exec(
                select_votesv2,
                (uid, story),
            )
            .await?;
        let select_hidden ="SELECT hidden_stories.* \
         FROM hidden_stories \
         WHERE hidden_stories.user_id = ? \
         AND hidden_stories.story_id = ?";
        let lq = select_hidden
        .replacen("?", &uid.to_string(), 1)
        .replacen("?", &story.to_string(), 1);
        log_query.push_str(&format!("\n{}", lq));
        c = c
            .drop_exec(
                select_hidden,
                (uid, story),
            )
            .await?;
        let select_saved ="SELECT saved_stories.* \
         FROM saved_stories \
         WHERE saved_stories.user_id = ? \
         AND saved_stories.story_id = ?";
        let lq = select_saved
        .replacen("?", &uid.to_string(), 1)
        .replacen("?", &story.to_string(), 1);
        log_query.push_str(&format!("\n{}", lq));
        c = c
            .drop_exec(
                select_saved,
                (uid, story),
            )
            .await?;
    }

    let select_taggings ="SELECT * FROM q26\
     WHERE story_id = ?";
    let lq = select_taggings
    .replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    let taggings = c
        .prep_exec(
            select_taggings,
            (story,),
        )
        .await?;

    let (c, tags) = taggings
        .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
            tags
        })
        .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");

    let select_tags =&format!(
        "SELECT * FROM q29 WHERE id IN ({})",
        tags
    );
    let lq = select_tags
    .replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    let c = c
        .drop_query(select_tags)
        .await?;

    log_query.push_str("\n--end: story");
    println!("{}", log_query);

    Ok((c, true))
}
