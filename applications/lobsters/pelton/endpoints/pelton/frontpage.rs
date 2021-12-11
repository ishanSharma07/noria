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
    let stories = c
        .prep_exec(
            "SELECT stories.* FROM stories \
             WHERE stories.merged_story_id IS NULL \
             AND stories.is_expired = 0 \
             AND stories.upvotes - stories.downvotes >= 0 \
             ORDER BY hotness ASC LIMIT 51", (),
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
            // let tags = tags
            //     .into_iter()
            //     .map(|id| format!("{}", id))
            //     .collect::<Vec<_>>()
            //     .join(",");
            let tags_params = tags.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let all_values: Vec<&u32> = tags.iter().map(|s| s as &_).chain(stories_in_values.iter().map(|s| s as &_)).collect();

            c = c
                .drop_exec(&format!(
                    "SELECT taggings.story_id \
                     FROM taggings \
                     WHERE taggings.story_id IN ({}) \
                     AND taggings.tag_id IN ({})",
                    stories_in_params, tags_params
                ), all_values)
                .await?;
        }
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    c = c
        .drop_query(&format!(
            "SELECT users.* FROM users WHERE users.id IN ({})",
            users,
        ))
        .await?;

    c = c
        .drop_exec(&format!(
            "SELECT suggested_titles.* \
             FROM suggested_titles \
             WHERE suggested_titles.story_id IN ({})",
            stories_in_params
        ), &stories_in_values)
        .await?;


    c = c
        .drop_exec(&format!(
            "SELECT suggested_taggings.* \
             FROM suggested_taggings \
             WHERE suggested_taggings.story_id IN ({})",
            stories_in_params
        ), &stories_in_values)
        .await?;

    let taggings = c
        .prep_exec( &format!(
            "SELECT taggings.* FROM taggings \
             WHERE taggings.story_id IN ({})",
            stories_in_params
        ), &stories_in_values)
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
            .chain(stories.iter().map(|s| s as &UserId))
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
            .chain(stories.iter().map(|s| s as &UserId))
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
            .chain(stories.iter().map(|s| s as &UserId))
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
