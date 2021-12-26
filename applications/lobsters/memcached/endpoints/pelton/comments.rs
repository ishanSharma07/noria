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
    let query = "SELECT comments.* \
     FROM comments \
     WHERE comments.is_deleted = 0 \
     AND comments.is_moderated = 0 \
     ORDER BY id DESC \
     LIMIT 40";
    let query_id = MemCache(query);
    let records = MemRead(query_id,  MemCreateKey(vec![]));
    // let comments = c
    //     .prep_exec("SELECT comments.* \
    //      FROM comments \
    //      WHERE comments.is_deleted = 0 \
    //      AND comments.is_moderated = 0 \
    //      ORDER BY id DESC \
    //      LIMIT 40", (), )
    //     .await?;

    let mut comments: Vec<u32> = Vec::new();
    let mut users: Vec<u32> = Vec::new();
    let mut stories: Vec<u32> = Vec::new();
    for i in 0..records.len(){
        let record: Vec<&str> = records[i].split("|").collect();
        let record = &record[1..];
        // println!("[comment, comments] record: {:?}", record);
        comments.push(record[0].parse().unwrap());
        users.push(record[5].parse().unwrap());
        stories.push(record[4].parse().unwrap());
    }
    // let (mut c, (comments, users, stories)) = comments
    //     .reduce_and_drop(
    //         (Vec::new(), HashSet::new(), HashSet::new()),
    //         |(mut comments, mut users, mut stories), comment| {
    //             comments.push(comment.get::<u32, _>("id").unwrap());
    //             users.insert(comment.get::<u32, _>("user_id").unwrap());
    //             stories.insert(comment.get::<u32, _>("story_id").unwrap());
    //             (comments, users, stories)
    //         },
    //     )
    //     .await?;

    if let Some(uid) = acting_as {
        let params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let args: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|c| c as &UserId))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT 1, user_id, story_id FROM hidden_stories \
                     WHERE hidden_stories.user_id = ? \
                     AND hidden_stories.story_id IN ({})",
                    params
                ),
                args,
            )
            .await?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    // println!("[comments1] users: {:?}", users);
    c = c
        .drop_query(&format!(
            "SELECT users.* FROM users \
             WHERE users.id IN ({})",
            users
        ))
        .await?;

    // let stories = stories
    //     .into_iter()
    //     .map(|id| format!("{}", id))
    //     .collect::<Vec<_>>()
    //     .join(",");

    let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let stories: Vec<&u32> = stories.iter().map(|s| s as &_).collect();


    // let key_values: Vec<memcached::Value> = Vec::new();
    // let tvec = stories.iter().map(|s| MemSetUInt(s.clone() as u64)).collect();
    // for i in 0..stories.len(){
    //     key_values.push(MemSetUInt(stories[i].clone() as u64));
    // }
    // let records = MemRead(query_id, MemCreateKey(key_values));
    let query = "SELECT stories.* FROM stories \
         WHERE stories.id = ?";
    let query_id = MemCache(query);
    let mut authors: Vec<u32> = Vec::new();
    for i in 0..stories.len(){
        let record = MemRead(query_id,  MemCreateKey(vec![MemSetUInt(stories[i].clone() as u64)]));
        let record: Vec<&str> = record[0].split("|").collect();
        let record = &record[1..];
        authors.push(record[2].parse().unwrap());
    }
    // let stories = c
    //     .prep_exec(&format!(
    //         "SELECT stories.* FROM stories \
    //          WHERE stories.id IN ({})",
    //         story_params
    //     ), stories)
    //     .await?;
    // let mut authors: Vec<u32> = Vec::new();
    // for i in 0..records.len(){
    //     let record: Vec<&str> = records[i].split("|").collect();
    //     authors.push(records[2].parse().unwrap());
    // }

    // let (mut c, authors) = stories
    //     .reduce_and_drop(HashSet::new(), |mut authors, story| {
    //         authors.insert(story.get::<u32, _>("user_id").unwrap());
    //         authors
    //     })
    //     .await?;

    if let Some(uid) = acting_as {
        let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let comments: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(comments.iter().map(|c| c as &UserId))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT votes.* FROM votes \
                     WHERE votes.OWNER_user_id = ? \
                     AND votes.comment_id IN ({})",
                    params
                ),
                comments,
            )
            .await?;
    }

    // NOTE: the real website issues all of these one by one...
    let authors = authors
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    // println!("[comments2] users: {:?}", users);
    c = c
        .drop_query(&format!(
                "SELECT users.* FROM users \
                 WHERE users.id IN ({})",
                authors
            ))
        .await?;

    Ok((c, true))
}
