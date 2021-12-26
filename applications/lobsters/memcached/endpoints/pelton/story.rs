use chrono;
use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use trawler::{StoryId, UserId};

use noria_applications::memcached::*;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    read_ribbon_uid: u32,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    // XXX: at the end there are also a bunch of repeated, seemingly superfluous queries
    let c = c.await?;
    // let query = "SELECT stories.* \
    //  FROM stories \
    //  WHERE stories.short_id = ?";
    // let query_id = MemCache(query);
    // let query_id = MemCache(query);
    // let records = MemRead(query_id,  MemCreateKey(vec![MemSetStr(::std::str::from_utf8(&comment[..]).unwrap())]));
    // assert!(records.len() == 1);
    let (mut c, mut story) = c
        .prep_exec(
            "SELECT stories.* \
             FROM stories \
             WHERE stories.short_id = ?",
            (format!{"{}", ::std::str::from_utf8(&id[..]).unwrap()},),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);
    let author = story.get::<u32, _>("user_id").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    c = c
        .drop_exec(
            "SELECT users.* FROM users WHERE users.id = ?",
            (author,),
        )
        .await?;

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let (x, rr) = c
            .first_exec::<_, _, my::Row>(
                "SELECT read_ribbons.* \
                     FROM read_ribbons \
                     WHERE read_ribbons.user_id = ? \
                     AND read_ribbons.story_id = ?",
                (&uid, &story),
            )
            .await?;
        let now = chrono::Local::now().naive_local();
        c = match rr {
            None => {
                x.drop_exec(
                    "INSERT INTO read_ribbons \
                         (id, created_at, updated_at, user_id, story_id, is_following) \
                         VALUES (?, ?, ?, ?, ?, 1)",
                    (read_ribbon_uid, format!{"{}", now}, format!{"{}", now}, uid, story),
                )
                .await?
            }
            Some(rr) => {
                x.drop_exec(
                    "UPDATE read_ribbons \
                         SET read_ribbons.updated_at = ? \
                         WHERE read_ribbons.id = ?",
                    (format!{"{}", now}, rr.get::<u32, _>("id").unwrap()),
                )
                .await?
            }
        };
        MemUpdate("read_ribbons");
    }

    // XXX: probably not drop here, but we know we have no merged stories
    let query = "SELECT stories.id, stories.merged_story_id \
     FROM stories \
     WHERE stories.merged_story_id = ?";
    let query_id = MemCache(query);
    let _records = MemRead(query_id,  MemCreateKey(vec![MemSetUInt(story as u64)]));
    // c = c
    //     .drop_exec(
    //         "SELECT stories.id, stories.merged_story_id \
    //          FROM stories \
    //          WHERE stories.merged_story_id = ?",
    //         (story,),
    //     )
    //     .await?;


    let query = "SELECT comments.*, comments.upvotes - comments.downvotes AS saldo \
     FROM comments \
     WHERE comments.story_id = ? \
     ORDER BY \
     saldo ASC, \
     confidence DESC";
    let query_id = MemCache(query);
    let records = MemRead(query_id, MemCreateKey(vec![MemSetUInt(story as u64)]));
    let mut comments: Vec<u32> = Vec::new();
    let mut users: Vec<u32> = Vec::new();
    for i in 0..records.len(){
        let record: Vec<&str> = records[i].split("|").collect();
        let record = &record[1..];
        comments.push(record[0].parse().unwrap());
        users.push(record[5].parse().unwrap());
    }
    // let comments = c
    //     .prep_exec(
    //         ,
    //         (story,),
    //     )
    //     .await?;

    // let (mut c, (users, comments)) = comments
    //     .reduce_and_drop(
    //         (HashSet::new(), HashSet::new()),
    //         |(mut users, mut comments), comment| {
    //             users.insert(comment.get::<u32, _>("user_id").unwrap());
    //             comments.insert(comment.get::<u32, _>("id").unwrap());
    //             (users, comments)
    //         },
    //     )
    //     .await?;

    // get user info for all commenters
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    c = c
        .drop_query(&format!(
            "SELECT users.* FROM users WHERE users.id IN ({})",
            users
        ))
        .await?;

    // get comment votes
    // XXX: why?!
    // let comments = comments
    //     .into_iter()
    //     .map(|id| format!("{}", id))
    //     .collect::<Vec<_>>()
    //     .join(", ");
    let comments_params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let comments: Vec<&u32> = comments.iter().map(|s| s as &_).collect();
    let query = "SELECT votes.* FROM votes WHERE votes.comment_id = ?";
    let query_id = MemCache(query);
    // TODO(Ishan): Memcache
    for i in 0..comments.len(){
        let _record = MemRead(query_id,  MemCreateKey(vec![MemSetUInt(comments[i].clone() as u64)]));
    }
    // c = c
    //     .drop_exec(&format!(
    //         "SELECT votes.* FROM votes WHERE votes.comment_id IN ({})",
    //         comments_params
    //     ), comments)
    //     .await?;

    // NOTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        c = c
            .drop_exec(
                "SELECT votes.* \
                 FROM votes \
                 WHERE votes.OWNER_user_id = ? \
                 AND votes.story_id = ? \
                 AND votes.comment_id IS NULL",
                (uid, story),
            )
            .await?;

        c = c
            .drop_exec(
                "SELECT hidden_stories.* \
                 FROM hidden_stories \
                 WHERE hidden_stories.user_id = ? \
                 AND hidden_stories.story_id = ?",
                (uid, story),
            )
            .await?;

        c = c
            .drop_exec(
                "SELECT saved_stories.* \
                 FROM saved_stories \
                 WHERE saved_stories.user_id = ? \
                 AND saved_stories.story_id = ?",
                (uid, story),
            )
            .await?;
    }

    let query = "SELECT taggings.* \
     FROM taggings \
     WHERE taggings.story_id = ?";
    let query_id = MemCache(query);
    let records = MemRead(query_id, MemCreateKey(vec![MemSetUInt(story as u64)]));
    let mut tags: Vec<u32> = Vec::new();
    for i in 0..records.len(){
        let record:Vec<&str> = records[i].split("|").collect();
        tags.push(record[2].parse().unwrap());
    }
    // let taggings = c
    //     .prep_exec(
    //         "SELECT taggings.* \
    //          FROM taggings \
    //          WHERE taggings.story_id = ?",
    //         (story,),
    //     )
    //     .await?;

    // let (c, tags) = taggings
    //     .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
    //         tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
    //         tags
    //     })
    //     .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(", ");
    let c = c
        .drop_query(&format!(
            "SELECT tags.* FROM tags WHERE tags.id IN ({})",
            tags
        ))
        .await?;

    Ok((c, true))
}
