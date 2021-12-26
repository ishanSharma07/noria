use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

use noria_applications::memcached::*;


pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let mut c = c.await?;
    // /recent is a little weird:
    // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
    // but it *basically* just looks for stories in the past few days
    // because all our stories are for the same day, we add a LIMIT
    // also note the NOW() hack to support dbs primed a while ago
    let query = "SELECT stories.* FROM stories \
     WHERE stories.merged_story_id IS NULL \
     AND stories.is_expired = 0 \
     AND stories.upvotes - stories.downvotes <= 5 \
     ORDER BY stories.id DESC LIMIT 51";
    let query_id = MemCache(query);
    let records = MemRead(query_id,  MemCreateKey(vec![]));
    let mut users: Vec<u32> = Vec::new();
    let mut stories: Vec<u32> = Vec::new();
    for i in 0..records.len(){
        let record: Vec<&str> = records[i].split("|").collect();
        let record = &record[1..];
        users.push(record[2].parse().unwrap());
        stories.push(record[0].parse().unwrap());
    }
    // let stories = c
    //     .prep_exec(
    //         , (),
    //     )
    //     .await?;
    // let (mut c, (users, stories)) = stories
    //     .reduce_and_drop(
    //         (HashSet::new(), HashSet::new()),
    //         |(mut users, mut stories), story| {
    //             users.insert(story.get::<u32, _>("user_id").unwrap());
    //             stories.insert(story.get::<u32, _>("id").unwrap());
    //             (users, stories)
    //         },
    //     )
    //     .await?;

    assert!(!stories.is_empty(), "got no stories from /recent");

    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    let stories_in_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let stories_in_values: Vec<&u32> = stories.iter().map(|s| s as &_).collect();

    if let Some(uid) = acting_as {
        let x = c
            .drop_exec(
                "SELECT hidden_stories.story_id \
                 FROM hidden_stories \
                 WHERE hidden_stories.user_id = ?",
                (uid,),
            )
            .await?;

        let tags = x
            .prep_exec(
                "SELECT tag_filters.* FROM tag_filters \
                 WHERE tag_filters.user_id = ?",
                (uid,),
            )
            .await?;
        let (x, tags) = tags
            .reduce_and_drop(Vec::new(), |mut tags, tag| {
                tags.push(tag.get::<u32, _>("tag_id").unwrap());
                tags
            })
            .await?;
        c = x;

        if !tags.is_empty() {
            // let s = stories
            //     .iter()
            //     .map(|id| format!("{}", id))
            //     .collect::<Vec<_>>()
            //     .join(",");
            let s_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let s_values: Vec<&u32> = stories.iter().map(|s| s as &_).collect();
            // let tags = tags
            //     .into_iter()
            //     .map(|id| format!("{}", id))
            //     .collect::<Vec<_>>()
            //     .join(",");
            let tags_params = tags.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let all_values: Vec<&u32> = tags.iter().map(|s| s as &_).chain(s_values.iter().map(|s| s as &_)).collect();

            let mut query = "SELECT taggings.story_id \
             FROM taggings \
             WHERE taggings.story_id = ? \
             AND taggings.tag_id = ?";
            let query_id = MemCache(query);
            // TODO: Memread
            // c = c
            //     .drop_exec(&format!(
            //         "SELECT taggings.story_id \
            //          FROM taggings \
            //          WHERE taggings.story_id IN ({}) \
            //          AND taggings.tag_id IN ({})",
            //         s_params, tags_params
            //     ), all_values)
            //     .await?;
        }
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    // println!("[recent] users: {:?}", users);
    c = c
        .drop_query(&format!(
            "SELECT users.* FROM users WHERE users.id IN ({})",
            users,
        ))
        .await?;


    let query = "SELECT suggested_titles.* \
     FROM suggested_titles \
     WHERE suggested_titles.story_id = ?";
    let query_id = MemCache(query);
    // TODO: Memread
    for i in 0..stories_in_values.len(){
        let _record = MemRead(query_id,  MemCreateKey(vec![MemSetUInt(stories[i].clone() as u64)]));
    }
    // c = c
    //     .drop_exec(&format!(
    //         "SELECT suggested_titles.* \
    //          FROM suggested_titles \
    //          WHERE suggested_titles.story_id IN ({})",
    //         stories_in_params
    //     ), &stories_in_values)
    //     .await?;

    let query = "SELECT suggested_taggings.* \
     FROM suggested_taggings \
     WHERE suggested_taggings.story_id = ?";
    let query_id = MemCache(query);
    // TODO: Memread
    for i in 0..stories_in_values.len(){
        let _record = MemRead(query_id,  MemCreateKey(vec![MemSetUInt(stories[i].clone() as u64)]));
    }
    // c = c
    //     .drop_exec(&format!(
    //         "SELECT suggested_taggings.* \
    //          FROM suggested_taggings \
    //          WHERE suggested_taggings.story_id IN ({})",
    //         stories_in_params
    //     ), &stories_in_values)
    //     .await?;


    let query = "SELECT taggings.* FROM taggings \
     WHERE taggings.story_id = ?";
    let query_id = MemCache(query);
    // TODO(Ishan): MemRead
    let mut tags: Vec<u32> = Vec::new();
    for i in 0..stories_in_values.len(){
        let record = MemRead(query_id,  MemCreateKey(vec![MemSetUInt(stories_in_values[i].clone() as u64)]));
        // println!("[recent] records: {:?}", record);
        let record: Vec<&str> = record[0].split("|").collect();
        let record = &record[1..];
        tags.push(record[2].parse().unwrap());
    }
    // let taggings = c
    //     .prep_exec(&format!(
    //         "SELECT taggings.* FROM taggings \
    //          WHERE taggings.story_id IN ({})",
    //         stories_in_params
    //     ), stories_in_values)
    //     .await?;
    //
    // let (mut c, tags) = taggings
    //     .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
    //         tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
    //         tags
    //     })
    //     .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c = c
        .drop_query(&format!(
            "SELECT tags.* FROM tags WHERE tags.id IN ({})",
            tags
        ))
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT votes.* FROM votes \
                     WHERE votes.OWNER_user_id = ? \
                     AND votes.story_id IN ({}) \
                     AND votes.comment_id IS NULL",
                    story_params
                ),
                values,
            )
            .await?;

        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT hidden_stories.* \
                     FROM hidden_stories \
                     WHERE hidden_stories.user_id = ? \
                     AND hidden_stories.story_id IN ({})",
                    story_params
                ),
                values,
            )
            .await?;

        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT saved_stories.* \
                     FROM saved_stories \
                     WHERE saved_stories.user_id = ? \
                     AND saved_stories.story_id IN ({})",
                    story_params
                ),
                values,
            )
            .await?;
    }

    Ok((c, true))
}
