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
    let mut log_query = format!("--start: recent");
    // /recent is a little weird:
    // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
    // but it *basically* just looks for stories in the past few days
    // because all our stories are for the same day, we add a LIMIT
    // also note the NOW() hack to support dbs primed a while ago
    let c = c.await?;
    let lq = "SELECT stories.* FROM stories \
     WHERE stories.merged_story_id IS NULL \
     AND stories.is_expired = 0 \
     AND stories.upvotes - stories.downvotes <= 5 \
     ORDER BY stories.id DESC LIMIT 51";
    log_query.push_str(&format!("\n{}", lq));
    let stories = c
        .query(
            lq,
        )
        .await?;
    let (mut c, (users, stories)) = stories
        .reduce_and_drop(
            (HashSet::new(), HashSet::new()),
            |(mut users, mut stories), story| {
                users.insert(story.get::<u32, _>("user_id").unwrap());
                stories.insert(story.get::<u32, _>("id").unwrap());
                (users, stories)
            },
        )
        .await?;

    assert!(!stories.is_empty(), "got no stories from /recent");

    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(uid) = acting_as {
        let select_hidden = "SELECT hidden_stories.story_id \
         FROM hidden_stories \
         WHERE hidden_stories.user_id = ?";
        let lq = select_hidden
           .replace("?", &uid.to_string());
        log_query.push_str(&format!("\n{}", lq));
        let x = c
            .drop_exec(
                select_hidden,
                (uid,),
            )
            .await?;

        let select_tag_filters = "SELECT tag_filters.* FROM tag_filters \
         WHERE tag_filters.user_id = ?";
        let lq = select_tag_filters
           .replace("?", &uid.to_string());
        log_query.push_str(&format!("\n{}", lq));
        let tags = x
            .prep_exec(
                select_tag_filters,
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
            let s = stories
                .iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");
            let tags = tags
                .into_iter()
                .map(|id| format!("{}", id))
                .collect::<Vec<_>>()
                .join(",");

            let select_taggings = &format!(
                "SELECT taggings.story_id \
                 FROM taggings \
                 WHERE taggings.story_id IN ({}) \
                 AND taggings.tag_id IN ({})",
                s, tags
            );
            log_query.push_str(&format!("\n{}", select_taggings));
            c = c
                .drop_query(select_taggings)
                .await?;
        }
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    let select_users = &format!(
        "SELECT users.* FROM users WHERE users.id IN ({})",
        users,
    );
    log_query.push_str(&format!("\n{}", select_users));
    c = c
        .drop_query(select_users)
        .await?;

    let select_sugg_titles = &format!(
        "SELECT suggested_titles.* \
         FROM suggested_titles \
         WHERE suggested_titles.story_id IN ({})",
        stories_in
    );
    log_query.push_str(&format!("\n{}", select_sugg_titles));
    c = c
        .drop_query(select_sugg_titles)
        .await?;

    let select_sugg_taggings = &format!(
        "SELECT suggested_taggings.* \
         FROM suggested_taggings \
         WHERE suggested_taggings.story_id IN ({})",
        stories_in
    );
    log_query.push_str(&format!("\n{}", select_sugg_taggings));
    c = c
        .drop_query(select_sugg_taggings)
        .await?;

    let select_taggingsv2 = &format!(
        "SELECT taggings.* FROM taggings \
         WHERE taggings.story_id IN ({})",
        stories_in
    );
    log_query.push_str(&format!("\n{}", select_taggingsv2));
    let taggings = c
        .query(select_taggingsv2)
        .await?;

    let (mut c, tags) = taggings
        .reduce_and_drop(HashSet::new(), |mut tags, tagging| {
            tags.insert(tagging.get::<u32, _>("tag_id").unwrap());
            tags
        })
        .await?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    let select_tags = &format!(
        "SELECT tags.* FROM tags WHERE tags.id IN ({})",
        tags
    );
    log_query.push_str(&format!("\n{}", select_tags));
    c = c
        .drop_query(select_tags)
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        let values_str = values
            .iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(",");
        let select_votes = &format!(
            "SELECT votes.* FROM votes \
             WHERE votes.OWNER_user_id = ? \
             AND votes.story_id IN ({}) \
             AND votes.comment_id IS NULL",
            story_params
        );
        let mut lq = select_votes.clone();
        // Replace first ? with acting_as uid
        lq = lq.replacen("?", &values[0].to_string(), 1);
        for i in 1..values.len(){
            lq = lq.replacen("?", &values[i].to_string(), 1)
        }
        log_query.push_str(&format!("\n{}", lq));
        c = c
            .drop_exec(
                select_votes,
                values,
            )
            .await?;

        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        let select_hiddenv2 = &format!(
            "SELECT hidden_stories.* \
             FROM hidden_stories \
             WHERE hidden_stories.user_id = ? \
             AND hidden_stories.story_id IN ({})",
            story_params
        );
        let mut log_hiddenv2 = select_hiddenv2.clone();
        // Replace first ? with acting_as uid
        log_hiddenv2 = log_hiddenv2.replacen("?", &values[0].to_string(), 1);
        for i in 1..values.len(){
            log_hiddenv2 = log_hiddenv2.replacen("?", &values[i].to_string(), 1)
        }
        log_query.push_str(&format!("\n{}", log_hiddenv2));
        c = c
            .drop_exec(
                select_hiddenv2,
                values,
            )
            .await?;

        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        let select_saved = &format!(
            "SELECT saved_stories.* \
             FROM saved_stories \
             WHERE saved_stories.user_id = ? \
             AND saved_stories.story_id IN ({})",
            story_params
        );
        let mut log_saved = select_saved.clone();
        // Replace first ? with acting_as uid
        log_saved = log_saved.replacen("?", &values[0].to_string(), 1);
        for i in 1..values.len(){
            log_saved = log_saved.replacen("?", &values[i].to_string(), 1)
        }
        log_query.push_str(&format!("\n{}", log_saved));
        c = c
            .drop_exec(
                select_saved,
                values,
            )
            .await?;
    }

    log_query.push_str("\n--end: recent");
    println!("{}", log_query);

    Ok((c, true))
}
