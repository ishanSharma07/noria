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

use noria_applications::memcached::*;


pub(crate) async fn notifications(mut c: my::Conn, uid: u32) -> Result<my::Conn, my::error::Error> {
    let query = "SELECT read_ribbons.user_id, COUNT(*) \
        FROM read_ribbons \
        JOIN stories ON (read_ribbons.story_id = stories.id) \
        JOIN comments ON (read_ribbons.story_id = comments.story_id) \
        LEFT JOIN comments AS parent_comments \
        ON (comments.parent_comment_id = parent_comments.id) \
        WHERE read_ribbons.is_following = 1 \
        AND comments.user_id <> read_ribbons.user_id \
        AND comments.is_deleted = 0 \
        AND comments.is_moderated = 0 \
        AND ( comments.upvotes - comments.downvotes ) >= 0 \
        AND read_ribbons.updated_at < comments.created_at \
        AND ( \
        ( \
            parent_comments.user_id = read_ribbons.user_id \
            AND \
            ( parent_comments.upvotes - parent_comments.downvotes ) >= 0 \
        ) \
        OR \
        ( \
            parent_comments.id IS NULL \
            AND \
            stories.user_id = read_ribbons.user_id \
        ) \
    ) GROUP BY read_ribbons.user_id HAVING read_ribbons.user_id = ?";
    let query_id = MemCache(&query);
    let _records = MemRead(query_id,  MemCreateKey(vec![]));
    // c = c
    //     .drop_exec(
    //         ,
    //         (uid,),
    //     )
    //     .await?;

    c = c
        .drop_exec(
            "SELECT keystores.* \
             FROM keystores \
             WHERE keystores.keyX = ?",
            (format!("user:{}:unread_messages", uid),),
        )
        .await?;

    Ok(c)
}
