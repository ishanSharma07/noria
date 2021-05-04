use my;
use my::prelude::*;
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();
    let select_comments = "SELECT `comments`.* \
     FROM `comments` \
     WHERE `comments`.`short_id` = ?";
    let mut log_query = select_comments.replace("?", ::std::str::from_utf8(&comment[..]).unwrap());
    println!("{}", log_query);
    let (mut c, comment) = c
        .first_exec::<_, _, my::Row>(
            select_comments,
            (::std::str::from_utf8(&comment[..]).unwrap(),),
        )
        .await?;

    let comment = comment.unwrap();
    let author = comment.get::<u32, _>("user_id").unwrap();
    let sid = comment.get::<u32, _>("story_id").unwrap();
    let upvotes = comment.get::<u32, _>("upvotes").unwrap();
    let downvotes = comment.get::<u32, _>("downvotes").unwrap();
    let comment = comment.get::<u32, _>("id").unwrap();
    let select_votes = "SELECT  `votes`.* \
     FROM `votes` \
     WHERE `votes`.`user_id` = ? \
     AND `votes`.`story_id` = ? \
     AND `votes`.`comment_id` = ?";
    log_query = select_votes
    .replacen("?", &user.to_string(), 1)
    .replacen("?", &sid.to_string(), 1)
    .replacen("?", &comment.to_string(), 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_votes,
            (user, sid, comment),
        )
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load comment under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let insert_votes = "INSERT INTO `votes` \
     (`user_id`, `story_id`, `comment_id`, `vote`, `reason`) \
     VALUES \
     (?, ?, ?, ?, NULL)";
    log_query = insert_votes
     .replacen("?", &user.to_string(), 1)
     .replacen("?", &sid.to_string(), 1)
     .replacen("?", &comment.to_string(), 1)
     .replacen("?", match v {
         Vote::Up => "1",
         Vote::Down => "0",
     }, 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            insert_votes,
            (
                user,
                sid,
                comment,
                match v {
                    Vote::Up => 1,
                    Vote::Down => 0,
                },
            ),
        )
        .await?;
    let update_users = &format!(
        "UPDATE `users` \
         SET `users`.`karma` = `users`.`karma` {} \
         WHERE `users`.`id` = ?",
        match v {
            Vote::Up => "+ 1",
            Vote::Down => "- 1",
        }
    );
    log_query = update_users.replace("?", &author.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            update_users,
            (author,),
        )
        .await?;

    // approximate Comment::calculate_hotness
    let confidence = (upvotes as f64 / (upvotes as f64 + downvotes as f64)).ceil();
    let update_comments = &format!(
        "UPDATE `comments` \
         SET \
         `comments`.`upvotes` = `comments`.`upvotes` {}, \
         `comments`.`downvotes` = `comments`.`downvotes` {}, \
         `comments`.`confidence` = ? \
         WHERE `id` = ?",
        match v {
            Vote::Up => "+ 1",
            Vote::Down => "+ 0",
        },
        match v {
            Vote::Up => "+ 0",
            Vote::Down => "+ 1",
        },
    );
    log_query = update_comments
     .replacen("?", &confidence.to_string(), 1)
     .replacen("?", &comment.to_string(), 1);
    println!("{}", log_query);
    c = c
        .drop_exec(
            update_comments,
            (confidence, comment),
        )
        .await?;

    // get all the stuff needed to compute updated hotness
    let select_stories = "SELECT `stories`.* \
     FROM `stories` \
     WHERE `stories`.`id` = ?";
    log_query = select_stories
      .replace("?", &sid.to_string());
    println!("{}", log_query);
    let (mut c, story) = c
        .first_exec::<_, _, my::Row>(
            select_stories,
            (sid,),
        )
        .await?;
    let story = story.unwrap();
    let score = story.get::<i64, _>("hotness").unwrap();

    let select_tags = "SELECT `tags`.* \
     FROM `tags` \
     INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
     WHERE `taggings`.`story_id` = ?";
     log_query = select_tags
       .replace("?", &sid.to_string());
     println!("{}", log_query);
    c = c
        .drop_exec(
            select_tags,
            (sid,),
        )
        .await?;

    let select_commentsv2 = "SELECT \
     `comments`.`upvotes`, \
     `comments`.`downvotes` \
     FROM `comments` \
     JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
     WHERE `comments`.`story_id` = ? \
     AND `comments`.`user_id` <> `stories`.`user_id`";
     log_query = select_commentsv2
       .replace("?", &sid.to_string());
     println!("{}", log_query);
    c = c
        .drop_exec(
            select_commentsv2,
            (sid,),
        )
        .await?;

    let select_storiesv2 = "SELECT `stories`.`id` \
     FROM `stories` \
     WHERE `stories`.`merged_story_id` = ?";
    log_query = select_storiesv2
       .replace("?", &sid.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            select_storiesv2,
            (sid,),
        )
        .await?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    let udpate_stories = &format!(
        "UPDATE stories SET \
         stories.upvotes = stories.upvotes {}, \
         stories.downvotes = stories.downvotes {}, \
         stories.hotness = ? \
         WHERE id = ?",
        match v {
            Vote::Up => "+ 1",
            Vote::Down => "+ 0",
        },
        match v {
            Vote::Up => "+ 0",
            Vote::Down => "+ 1",
        },
    );
    log_query = udpate_stories
       .replace("?", &(score
           - match v {
               Vote::Up => 1,
               Vote::Down => -1,
           }).to_string())
       .replace("?", &sid.to_string());
    println!("{}", log_query);
    c = c
        .drop_exec(
            udpate_stories,
            (
                score
                    - match v {
                        Vote::Up => 1,
                        Vote::Down => -1,
                    },
                sid,
            ),
        )
        .await?;

    Ok((c, false))
}
