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
    let (mut c, user) = c
        .first_exec::<_, _, my::Row>(
            "SELECT users.* FROM users \
             WHERE users.PII_username = ?",
            (format!("{}", uid),),
        )
        .await?;
    let uid = user.expect(&format!("user {} should exist", uid)).get::<u32, _>("id").unwrap();

    // most popular tag
    c = c
        .drop_exec(
            "SELECT id, count FROM q22 WHERE user_id = ?",
            (uid,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT keystores.* \
             FROM keystores \
             WHERE keystores.keyX = ?",
            (format!("user:{}:stories_submitted", uid),),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT keystores.* \
             FROM keystores \
             WHERE keystores.keyX = ?",
            (format!("user:{}:comments_posted", uid),),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT `one` FROM q27 \
             WHERE OWNER_user_id = ?",
            (uid,),
        )
        .await?;

    Ok((c, true))
}
