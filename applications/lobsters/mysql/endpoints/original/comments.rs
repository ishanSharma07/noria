use my;
use my::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let mut log_query = format!("--start: comments");

    let c = c.await?;
    let select_comments = "SELECT comments.* \
     FROM comments \
     WHERE comments.is_deleted = 0 \
     AND comments.is_moderated = 0 \
     ORDER BY id DESC \
     LIMIT 40";
    log_query.push_str(&format!("\n{}", select_comments));
    let comments = c
        .query(select_comments)
        .await?;

    let (mut c, (comments, users, stories)) = comments
        .reduce_and_drop(
            (Vec::new(), HashSet::new(), HashSet::new()),
            |(mut comments, mut users, mut stories), comment| {
                comments.push(comment.get::<u32, _>("id").unwrap());
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                stories.insert(comment.get::<u32, _>("story_id").unwrap());
                (comments, users, stories)
            },
        )
        .await?;

    if let Some(uid) = acting_as {
        let params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let args: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|c| c as &UserId))
            .collect();
        let select_one = &format!(
                "SELECT 1 FROM hidden_stories \
                 WHERE hidden_stories.user_id = ? \
                 AND hidden_stories.story_id IN ({})",
            params
        );
        let mut lq = select_one.clone();
        for &arg in args.iter(){
            lq = lq.replacen("?", &arg.to_string(), 1)
        }
        log_query.push_str(&format!("\n{}", lq));
        c = c
            .drop_exec(
                select_one,
                args,
            )
            .await?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    let select_users = &format!(
        "SELECT users.* FROM users \
         WHERE users.id IN ({})",
        users
    );
    log_query.push_str(&format!("\n{}", select_users));
    c = c
        .drop_query(select_users)
        .await?;

    let stories = stories
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    let select_stories = &format!(
        "SELECT stories.* FROM stories \
         WHERE stories.id IN ({})",
        stories
    );
    log_query.push_str(&format!("\n{}", select_stories));
    let stories = c
        .query(select_stories)
        .await?;

    let (mut c, authors) = stories
        .reduce_and_drop(HashSet::new(), |mut authors, story| {
            authors.insert(story.get::<u32, _>("user_id").unwrap());
            authors
        })
        .await?;

    if let Some(uid) = acting_as {
        let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let comments: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(comments.iter().map(|c| c as &UserId))
            .collect();
        let select_votes = &format!(
            "SELECT votes.* FROM votes \
             WHERE votes.OWNER_user_id = ? \
             AND votes.comment_id IN ({})",
            params
        );
        let mut lq = select_votes.clone();
        // Replace first ? with acting_as uid
        lq = lq.replacen("?", &comments[0].to_string(), 1);
        for i in 1..comments.len(){
            lq = lq.replacen("?", &comments[i].to_string(), 1)
        }
        log_query.push_str(&format!("\n{}", lq));
        c = c
            .drop_exec(
                select_votes,
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
    let select_usersv2 = &format!(
            "SELECT users.* FROM users \
             WHERE users.id IN ({})",
            authors
        );
    log_query.push_str(&format!("\n{}", select_usersv2));
    c = c
        .drop_query(select_usersv2)
        .await?;

    log_query.push_str(&format!("\n--end: comments"));
    println!("{}", log_query);

    Ok((c, true))
}
