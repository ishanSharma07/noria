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
    let c = c.await?;
    let select_stories = "SELECT  stories.* FROM stories \
     WHERE stories.merged_story_id IS NULL \
     AND stories.is_expired = 0 \
     AND stories.upvotes - stories.downvotes >= 0 \
     ORDER BY hotness ASC LIMIT 51";
    let log_query = select_stories;
    println!("{}", log_query);
    let stories = c
        .query(
            select_stories,
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

    assert!(!stories.is_empty(), "got no stories from /frontpage");

    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(uid) = acting_as {
        let select_hidden = "SELECT hidden_stories.story_id \
         FROM hidden_stories \
         WHERE hidden_stories.user_id = ?";
         let log_query = select_hidden.replace("?", &uid.to_string());
         println!("{}", log_query);
        let x = c
            .drop_exec(
                select_hidden,
                (uid,),
            )
            .await?;

        let select_tags = "SELECT tag_filters.* FROM tag_filters \
         WHERE tag_filters.user_id = ?";
         let log_query = select_tags.replace("?", &uid.to_string());
         println!("{}", log_query);
        let tags = x
            .prep_exec(
                select_tags,
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
                stories_in, tags
            );
            println!("{}", select_taggings);
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
    let select_usersv2 = &format!(
        "SELECT users.* FROM users WHERE users.id IN ({})",
        users,
    );
    println!("{}", select_usersv2);
    c = c
        .drop_query(select_usersv2)
        .await?;

    let select_sugg_titles = &format!(
        "SELECT suggested_titles.* \
         FROM suggested_titles \
         WHERE suggested_titles.story_id IN ({})",
        stories_in
    );
    println!("{}", select_sugg_titles);
    c = c
        .drop_query(select_sugg_titles)
        .await?;

    let select_sugg_taggings = &format!(
        "SELECT suggested_taggings.* \
         FROM suggested_taggings \
         WHERE suggested_taggings.story_id IN ({})",
        stories_in
    );
    println!("{}", select_sugg_taggings);
    c = c
        .drop_query(select_sugg_taggings)
        .await?;

    let select_taggingsv2 = &format!(
        "SELECT taggings.* FROM taggings \
         WHERE taggings.story_id IN ({})",
        stories_in
    );
    println!("{}", select_taggingsv2);
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
    let select_tagsv2 = &format!(
        "SELECT tags.* FROM tags WHERE tags.id IN ({})",
        tags
    );
    println!("{}", select_tagsv2);
    c = c
        .drop_query(select_tagsv2)
        .await?;

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let values: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|s| s as &UserId))
            .collect();
        let select_votes = &format!(
            "SELECT votes.* FROM votes \
             WHERE votes.user_id = ? \
             AND votes.story_id IN ({}) \
             AND votes.comment_id IS NULL",
            story_params
        );
        let mut values_str = String::from("");
        for &value in values.iter(){
            values_str.push_str(&value.to_string());
            values_str.push(',');
        }
        // Delete the last ','
        values_str.pop();
        let log_votes = select_votes.replace("?", &values_str);
        println!("{}", log_votes);
        c = c
            .drop_exec(
                select_votes,
                values,
            )
            .await?;

        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        let select_hiddenv2 = &format!(
            "SELECT hidden_stories.* \
             FROM hidden_stories \
             WHERE hidden_stories.user_id = ? \
             AND hidden_stories.story_id IN ({})",
            story_params
        );
        let log_hiddenv2 = select_hiddenv2.replace("?", &values_str);
        println!("{}", log_hiddenv2);
        c = c
            .drop_exec(
                select_hiddenv2,
                values,
            )
            .await?;

        let values: Vec<_> = iter::once(&uid as &_)
            .chain(stories.iter().map(|s| s as &_))
            .collect();
        let select_saved = &format!(
            "SELECT saved_stories.* \
             FROM saved_stories \
             WHERE saved_stories.user_id = ? \
             AND saved_stories.story_id IN ({})",
            story_params
        );
        let log_saved = select_saved.replace("?", &values_str);
        println!("{}", log_saved);
        c = c
            .drop_exec(
                select_saved,
                values,
            )
            .await?;
    }

    Ok((c, true))
}
