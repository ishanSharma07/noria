use my;
use my::prelude::*;
use std::future::Future;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let mut log_query = String::from("--start: user");

    let c = c.await?;
    let select_users = "SELECT users.* FROM users \
     WHERE users.PII_username = ?";
    log_query.push_str(&format!("\n{}", select_users.replace("?", &format!("'{}'", uid))));
    let (mut c, user) = c
        .first_exec::<_, _, my::Row>(
            select_users,
            (format!("{}", uid),),
        )
        .await?;
    let uid = user.expect(&format!("user {} should exist", uid)).get::<u32, _>("id").unwrap();

    // most popular tag
    let select_tags = "SELECT tags.id, count(*) AS `count` FROM tags \
     INNER JOIN taggings ON tags.id = taggings.tag_id \
     INNER JOIN stories ON taggings.story_id = stories.id \
     WHERE tags.inactive = 0 \
     AND stories.user_id = ? \
     GROUP BY tags.id \
     ORDER BY `count` DESC LIMIT 1";
    log_query.push_str(&format!("\n{}", select_tags.replace("?", &uid.to_string())));
    c = c
        .drop_exec(
            select_tags,
            (uid,),
        )
        .await?;

    let select_keystore = "SELECT keystores.* \
     FROM keystores \
     WHERE keystores.keyX = ?";
    log_query.push_str(&format!("\n{}", select_keystore.replace("?", &format!("'user:{}:stories_submitted'", uid))));

    c = c
        .drop_exec(
            select_keystore,
            (format!("user:{}:stories_submitted", uid),),
        )
        .await?;

    log_query.push_str(&format!("\n{}", select_keystore.replace("?", &format!("'user:{}:comments_posted'", uid))));

    c = c
        .drop_exec(
            select_keystore,
            (format!("user:{}:comments_posted", uid),),
        )
        .await?;

    let select_hats = "SELECT 1 AS `one` FROM hats \
     WHERE hats.OWNER_user_id = ? LIMIT 1";
    log_query.push_str(&format!("\n{}", select_hats.replace("?", &uid.to_string())));
    c = c
        .drop_exec(
            "SELECT 1 AS `one` FROM hats \
             WHERE hats.OWNER_user_id = ? LIMIT 1",
            (uid,),
        )
        .await?;

    log_query.push_str("\n--end: user");

    Ok((c, true))
}
