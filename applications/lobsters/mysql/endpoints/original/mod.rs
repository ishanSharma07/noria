pub(crate) mod comment;
pub(crate) mod comment_vote;
pub(crate) mod comments;
pub(crate) mod frontpage;
pub(crate) mod recent;
pub(crate) mod story;
pub(crate) mod story_vote;
pub(crate) mod submit;
pub(crate) mod user;

use my;
use my::prelude::*;

pub(crate) async fn notifications(mut c: my::Conn, uid: u32) -> Result<my::Conn, my::error::Error> {
    let select_count = "SELECT COUNT(*) \
             FROM replying_comments_for_count \
             WHERE replying_comments_for_count.user_id = ? \
             GROUP BY replying_comments_for_count.user_id \
             ";
    let mut log_query = select_count.replace("?", &uid.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_count,
            (uid,),
        )
        .await?;

    let select_keystore = "SELECT keystores.* \
     FROM keystores \
     WHERE keystores.keyX = ?";
    log_query = select_keystore.replace("?", &format!("'user:{}:unread_messages'", uid));
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_keystore,
            (format!("user:{}:unread_messages", uid),),
        )
        .await?;

    Ok(c)
}
