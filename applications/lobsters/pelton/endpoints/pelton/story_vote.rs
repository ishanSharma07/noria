use my;
use my::prelude::*;
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let mut log_query = format!("--start: story_vote");
    let c = c.await?;
    let user = acting_as.unwrap();
    let select_stories = "SELECT stories.* \
     FROM stories \
     WHERE stories.short_id = ?";
    let lq = select_stories.replace("?",&format!("'{}'", ::std::str::from_utf8(&story[..]).unwrap()));
    log_query.push_str(&format!("\n{}", lq));
    let (mut c, mut story) = c
        .prep_exec(
            select_stories,
            (::std::str::from_utf8(&story[..]).unwrap(),),
        )
        .await?
        .collect_and_drop::<my::Row>()
        .await?;
    let story = story.swap_remove(0);

    let author = story.get::<u32, _>("user_id").unwrap();
    let score = story.get::<i64, _>("hotness").unwrap();
    let story = story.get::<u32, _>("id").unwrap();
    let select_votes ="SELECT votes.* \
     FROM votes \
     WHERE votes.OWNER_user_id = ? \
     AND votes.story_id = ? \
     AND votes.comment_id IS NULL";
    let lq = select_votes
    .replacen("?", &user.to_string(), 1)
    .replacen("?", &story.to_string(), 1);
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            select_votes,
            (user, story),
        )
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let insert_votes = "INSERT INTO votes \
     (OWNER_user_id, story_id, vote, comment_id, reason) \
     VALUES (?, ?, ?, NULL, NULL)";
    c = c
        .drop_exec(
            insert_votes,
            (
                user,
                story,
                match v {
                    Vote::Up => 1,
                    Vote::Down => 0,
                },
            ),
        )
        .await?;
    let vote_insert_id = c.last_insert_id().unwrap();
    let lq = format!("INSERT INTO votes \
     (id, OWNER_user_id, story_id, comment_id, vote, reason) \
     VALUES \
     ({}, {}, {}, NULL, {}, NULL)", vote_insert_id, user, story,
     match v {
         Vote::Up => "1",
         Vote::Down => "0",
     });
    log_query.push_str(&format!("\n{}", lq));

    let update_users = &format!(
        "UPDATE users \
         SET users.karma = users.karma {} \
         WHERE users.id = ?",
        match v {
            Vote::Up => "+ 1",
            Vote::Down => "- 1",
        }
    );
    let lq = update_users.replace("?", &author.to_string());
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            update_users,
            (author,),
        )
        .await?;

    // get all the stuff needed to compute updated hotness
    let select_tags = "SELECT * FROM q13 \
     WHERE story_id = ?";
    let lq = select_tags.replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            select_tags,
            (story,),
        )
        .await?;

    let select_comments = "SELECT * from q6 \
     WHERE story_id = ?";
    let lq = select_comments.replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            select_comments,
            (story,),
        )
        .await?;

    let select_storiesv2 = "SELECT id FROM q11 \
     WHERE merged_story_id = ?";
    let lq = select_storiesv2.replace("?", &story.to_string());
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            select_storiesv2,
            (story,),
        )
        .await?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    let update_stories = &format!(
        "UPDATE stories SET \
         stories.upvotes = stories.upvotes {}, \
         stories.downvotes = stories.downvotes {}, \
         stories.hotness = ? \
         WHERE stories.id = ?",
        match v {
            Vote::Up => "+ 1",
            Vote::Down => "+ 0",
        },
        match v {
            Vote::Up => "+ 0",
            Vote::Down => "+ 1",
        },
    );
    let lq = update_stories
     .replacen("?", &(score
         - match v {
             Vote::Up => 1,
             Vote::Down => -1,
         }).to_string(), 1)
     .replacen("?", &story.to_string(), 1);
    log_query.push_str(&format!("\n{}", lq));
    c = c
        .drop_exec(
            update_stories,
            (
                score
                    - match v {
                        Vote::Up => 1,
                        Vote::Down => -1,
                    },
                story,
            ),
        )
        .await?;

    log_query.push_str("\n--end: story_vote");
    println!("{}", log_query);

    Ok((c, false))
}
