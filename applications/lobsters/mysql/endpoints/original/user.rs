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
    let c = c.await?;
    let select_users = "SELECT  users.* FROM users \
     WHERE users.PII_username = ?";
    let mut log_query = select_users.replace("?", &format!("'{}'", uid));
    println!("{}", log_query);
    let (mut c, user) = c
        .first_exec::<_, _, my::Row>(
            select_users,
            (format!("'{}'", uid),),
        )
        .await?;
    let uid = user.expect(&format!("user {} should exist", uid)).get::<u32, _>("id").unwrap();

    // most popular tag
    let select_tags = "SELECT  tags.id, count(*) AS `count` FROM tags \
     INNER JOIN taggings ON taggings.tag_id = tags.id \
     INNER JOIN stories ON stories.id = taggings.story_id \
     WHERE tags.inactive = 0 \
     AND stories.user_id = ? \
     GROUP BY tags.id \
     ORDER BY `count` DESC LIMIT 1";
    log_query = select_tags.replace("?", &uid.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_tags,
            (uid,),
        )
        .await?;

    let select_keystore = "SELECT  keystores.* \
     FROM keystores \
     WHERE keystores.keyX = ?";
    log_query = select_keystore.replace("?", &format!("'user:{}:stories_submitted'", uid));
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_keystore,
            (format!("user:{}:stories_submitted", uid),),
        )
        .await?;

    log_query = select_keystore.replace("?", &format!("'user:{}:comments_posted'", uid));
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_keystore,
            (format!("user:{}:comments_posted", uid),),
        )
        .await?;

    let select_hats = "SELECT  1 AS `one` FROM hats \
     WHERE hats.OWNER_user_id = ? LIMIT 1";
    log_query = select_hats.replace("?", &uid.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            "SELECT  1 AS `one` FROM hats \
             WHERE hats.OWNER_user_id = ? LIMIT 1",
            (uid,),
        )
        .await?;

    Ok((c, true))
}
