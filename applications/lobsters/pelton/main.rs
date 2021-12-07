#![feature(type_alias_impl_trait)]

extern crate mysql_async as my;

use clap::value_t_or_exit;
use clap::{App, Arg};
use my::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time;
use tower_service::Service;
use trawler::{LobstersRequest, TrawlerRequest};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

const PELTON_SCHEMA: &'static str = include_str!("db-schema/pelton.sql");

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Variant {
    Pelton,
}

struct MysqlTrawlerBuilder {
    opts: my::OptsBuilder,
    variant: Variant,
    stories_counter: Arc<Mutex<u16>>,
    taggings_counter: Arc<Mutex<u16>>,
    votes_counter: Arc<Mutex<u16>>,
    ribbons_counter: Arc<Mutex<u16>>,
    comments_counter: Arc<Mutex<u16>>,
}

enum MaybeConn {
    None,
    Pending(my::futures::GetConn),
    Ready(my::Conn),
}

struct MysqlTrawler {
    c: my::Pool,
    next_conn: MaybeConn,
    variant: Variant,
    stories_counter: Arc<Mutex<u16>>,
    taggings_counter: Arc<Mutex<u16>>,
    votes_counter: Arc<Mutex<u16>>,
    ribbons_counter: Arc<Mutex<u16>>,
    comments_counter: Arc<Mutex<u16>>,
}
/*
impl Drop for MysqlTrawler {
    fn drop(&mut self) {
        self.c.disconnect();
    }
}
*/

mod endpoints;

impl Service<bool> for MysqlTrawlerBuilder {
    type Response = MysqlTrawler;
    type Error = my::error::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, priming: bool) -> Self::Future {
        let orig_opts = self.opts.clone();
        let variant = self.variant;
        let stories_counter = Arc::clone(&self.stories_counter);
        let taggings_counter = Arc::clone(&self.taggings_counter);
        let votes_counter = Arc::clone(&self.votes_counter);
        let ribbons_counter = Arc::clone(&self.ribbons_counter);
        let comments_counter = Arc::clone(&self.comments_counter);
        if priming {
            // we need a special conn for setup
            let mut opts = self.opts.clone();
            opts.pool_options(my::PoolOptions::with_constraints(
                my::PoolConstraints::new(1, 1).unwrap(),
            ));
            let db: String = my::Opts::from(opts.clone())
                .get_db_name()
                .unwrap()
                .to_string();
            opts.db_name(None::<String>);
            opts.prefer_socket(false);
            // let db_drop = format!("DROP DATABASE IF EXISTS {}", db);
            // let db_create = format!("CREATE DATABASE {}", db);
            // let db_use = format!("USE {}", db);
            Box::pin(async move {
                let mut c = my::Conn::new(opts).await?;
                // c = c.drop_query(&db_drop).await?;
                // c = c.drop_query(&db_create).await?;
                // c = c.drop_query(&db_use).await?;
                let schema = match variant {
                    Variant::Pelton => PELTON_SCHEMA,
                };
                let mut current_q = String::new();
                for line in schema.lines() {
                    if line.starts_with("--") || line.is_empty() {
                        continue;
                    }
                    if !current_q.is_empty() {
                        current_q.push_str(" ");
                    }
                    current_q.push_str(line);
                    if current_q.ends_with(';') {
                        c = c.drop_query(&current_q).await?;
                        current_q.clear();
                    }
                }

                Ok(MysqlTrawler {
                    c: my::Pool::new(orig_opts.clone()),
                    next_conn: MaybeConn::None,
                    variant,
                    stories_counter: stories_counter,
                    taggings_counter: taggings_counter,
                    votes_counter: votes_counter,
                    ribbons_counter: ribbons_counter,
                    comments_counter: comments_counter,
                })
            })
        } else {
            Box::pin(async move {
                Ok(MysqlTrawler {
                    c: my::Pool::new(orig_opts.clone()),
                    next_conn: MaybeConn::None,
                    variant,
                    stories_counter: stories_counter,
                    taggings_counter: taggings_counter,
                    votes_counter: votes_counter,
                    ribbons_counter: ribbons_counter,
                    comments_counter: comments_counter,
                })
            })
        }
    }
}

impl Service<TrawlerRequest> for MysqlTrawler {
    type Response = ();
    type Error = my::error::Error;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.next_conn {
                MaybeConn::None => {
                    self.next_conn = MaybeConn::Pending(self.c.get_conn());
                }
                MaybeConn::Pending(ref mut getconn) => {
                    if let Poll::Ready(conn) = Pin::new(getconn).poll(cx)? {
                        self.next_conn = MaybeConn::Ready(conn);
                    } else {
                        return Poll::Pending;
                    }
                }
                MaybeConn::Ready(_) => {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
    fn call(
        &mut self,
        TrawlerRequest {
            user: acting_as,
            page: req,
            is_priming: priming,
            ..
        }: TrawlerRequest,
    ) -> Self::Future {
        let c = match std::mem::replace(&mut self.next_conn, MaybeConn::None) {
            MaybeConn::None | MaybeConn::Pending(_) => {
                unreachable!("call called without poll_ready")
            }
            MaybeConn::Ready(c) => c,
        };
        // This is to hack around the following error:
        // `self` has an anonymous lifetime `'_` but it needs to satisfy a `'static` lifetime requirement
        let stories_counter_local = Arc::clone(&self.stories_counter);
        let taggings_counter_local = Arc::clone(&self.taggings_counter);
        let votes_counter_local = Arc::clone(&self.votes_counter);
        let ribbons_counter_local = Arc::clone(&self.ribbons_counter);
        let comments_counter_local = Arc::clone(&self.comments_counter);
        let c = futures_util::future::ready(Ok(c));

        // TODO: traffic management
        // https://github.com/lobsters/lobsters/blob/master/app/controllers/application_controller.rb#L37
        /*
        let c = c.and_then(|c| {
            c.start_transaction(my::TransactionOptions::new())
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:date' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "SELECT keystores.* FROM keystores \
                         WHERE keystores.key = 'traffic:hits' FOR UPDATE",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 100 \
                         WHERE keystores.key = 'traffic:hits'",
                    )
                })
                .and_then(|t| {
                    t.drop_query(
                        "UPDATE keystores SET value = 1521590012 \
                         WHERE keystores.key = 'traffic:date'",
                    )
                })
                .and_then(|t| t.commit())
        });
        */

        macro_rules! handle_req {
            ($module:tt, $req:expr) => {{
                match req {
                    LobstersRequest::User(uid) => {
                        endpoints::$module::user::handle(c, acting_as, uid).await
                    }
                    LobstersRequest::Frontpage => {
                        endpoints::$module::frontpage::handle(c, acting_as).await
                    }
                    LobstersRequest::Comments => {
                        endpoints::$module::comments::handle(c, acting_as).await
                    }
                    LobstersRequest::Recent => {
                        endpoints::$module::recent::handle(c, acting_as).await
                    }
                    LobstersRequest::Login => {
                        let c = c.await?;
                        let (mut c, user) = c
                            .first_exec::<_, _, my::Row>(
                                "SELECT 1 AS `one` FROM users WHERE users.PII_username = ?",
                                (format!("'user{}'", acting_as.unwrap()),),
                            )
                            .await?;

                        if user.is_none() {
                            let uid = acting_as.unwrap();
                            c = c
                                .drop_exec(
                                    "INSERT INTO users \
                                    (id, PII_username, email, password_digest, created_at, is_admin, \
                                    password_reset_token, session_token, about, invited_by_user_id,\
                                    is_moderator, pushover_mentions, rss_token, mailing_list_token,\
                                    mailing_list_mode, karma, banned_at, banned_by_user_id, \
                                    banned_reason, deleted_at, disabled_invite_at, \
                                    disabled_invite_by_user_id, disabled_invite_reason, settings) \
                                    VALUES (?, ?, 'x@gmail.com', 'asdf', '2021-05-07 18:00', 0, NULL, ?, NULL, NULL, NULL, NULL, NULL, \
                                        NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                                    (format!("{}", uid), format!("'{}'", uid), format!("'session_token_{}'", uid),),
                                )
                                .await?;
                        }

                        Ok((c, false))
                    }
                    LobstersRequest::Logout => Ok((c.await?, false)),
                    LobstersRequest::Story(id) => {
                        let ribbon_count;
                        {
                            // This scope is used to hack round the following:
                            // `std::sync::MutexGuard<'_, u16>` cannot be sent between threads safely
                            let mut counter = ribbons_counter_local.lock().unwrap();
                            *counter += 1;
                            ribbon_count = Into::<u16>::into(*counter);
                            drop(counter);
                        }
                        endpoints::$module::story::handle(c, acting_as, id, ribbon_count).await
                    }
                    LobstersRequest::StoryVote(story, v) => {
                        let vote_count;
                        {
                            // This scope is used to hack round the following:
                            // `std::sync::MutexGuard<'_, u16>` cannot be sent between threads safely
                            let mut counter = votes_counter_local.lock().unwrap();
                            *counter += 1;
                            vote_count = Into::<u16>::into(*counter);
                            drop(counter);
                        }
                        endpoints::$module::story_vote::handle(c, acting_as, story, v, vote_count).await
                    }
                    LobstersRequest::CommentVote(comment, v) => {
                        let vote_count;
                        {
                            // This scope is used to hack round the following:
                            // `std::sync::MutexGuard<'_, u16>` cannot be sent between threads safely
                            let mut counter = votes_counter_local.lock().unwrap();
                            *counter += 1;
                            vote_count = Into::<u16>::into(*counter);
                            drop(counter);
                        }
                        endpoints::$module::comment_vote::handle(c, acting_as, comment, v, vote_count).await
                    }
                    LobstersRequest::Submit { id, title } => {
                        let story_count;
                        let tagging_count;
                        let vote_count;
                        {
                            // This scope is used to hack round the following:
                            // `std::sync::MutexGuard<'_, u16>` cannot be sent between threads safely
                            let mut counter = stories_counter_local.lock().unwrap();
                            *counter += 1;
                            story_count = Into::<u16>::into(*counter);
                            counter = taggings_counter_local.lock().unwrap();
                            *counter += 1;
                            tagging_count = Into::<u16>::into(*counter);
                            counter = votes_counter_local.lock().unwrap();
                            *counter += 1;
                            vote_count = Into::<u16>::into(*counter);
                            drop(counter);
                        }
                        endpoints::$module::submit::handle(c, acting_as, id, title, priming, story_count, tagging_count, vote_count).await
                    }
                    LobstersRequest::Comment { id, story, parent } => {
                        let comment_count;
                        let vote_count;
                        {
                            // This scope is used to hack round the following:
                            // `std::sync::MutexGuard<'_, u16>` cannot be sent between threads safely
                            let mut counter =  comments_counter_local.lock().unwrap();
                            *counter += 1;
                            comment_count = Into::<u16>::into(*counter);
                            counter = votes_counter_local.lock().unwrap();
                            *counter += 1;
                            vote_count = Into::<u16>::into(*counter);
                            drop(counter);
                        }
                        endpoints::$module::comment::handle(
                            c, acting_as, id, story, parent, priming, comment_count, vote_count
                        )
                        .await
                    }
                }
            }};
        }

        let variant = self.variant;

        Box::pin(async move {
            let inner = async move {
                let (c, with_notifications) = match variant {
                    Variant::Pelton => handle_req!(pelton, req),
                }?;

                // notifications
                if let Some(uid) = acting_as {
                    if with_notifications && !priming {
                        match variant {
                            Variant::Pelton => endpoints::pelton::notifications(c, uid).await,
                        }?;
                    }
                }

                Ok(())
            };

            // if the pool is disconnected, it just means that we exited while there were still
            // outstanding requests. that's fine.
            match inner.await {
                Ok(())
                | Err(my::error::Error::Driver(my::error::DriverError::PoolDisconnected)) => Ok(()),
                Err(e) => Err(e),
            }
        })
    }
}

impl trawler::AsyncShutdown for MysqlTrawler {
    type Future = impl Future<Output = ()>;
    fn shutdown(mut self) -> Self::Future {
        let _ = std::mem::replace(&mut self.next_conn, MaybeConn::None);
        async move {
            let _ = self.c.disconnect().await.unwrap();
        }
    }
}

fn main() {
    let args = App::new("lobsters-pelton")
        .version("0.1")
        .about("Benchmark a lobste.rs Rails installation using MySQL directly")
        .arg(
            Arg::with_name("scale")
                .long("scale")
                .takes_value(true)
                .default_value("1.0")
                .help("Reuest load scale factor for workload"),
        )
        .arg(
            Arg::with_name("in-flight")
                .long("in-flight")
                .takes_value(true)
                .default_value("50")
                .help("Number of allowed concurrent requests"),
        )
        .arg(
            Arg::with_name("prime")
                .long("prime")
                .help("Set if the backend must be primed with initial stories and comments."),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .long("queries")
                .possible_values(&["pelton"])
                .takes_value(true)
                .required(true)
                .help("Which set of queries to run"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .takes_value(true)
                .default_value("30")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("histogram")
                .long("histogram")
                .help("Use file-based serialized HdrHistograms")
                .takes_value(true)
                .long_help("There are multiple histograms, two for each lobsters request."),
        )
        .arg(
            Arg::with_name("dbn")
                .value_name("DBN")
                .takes_value(true)
                .default_value("mysql://lobsters@localhost/soup")
                .index(1),
        )
        .get_matches();

    let variant = match args.value_of("queries").unwrap() {
        "pelton" => Variant::Pelton,
        _ => unreachable!(),
    };
    let in_flight = value_t_or_exit!(args, "in-flight", usize);

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(value_t_or_exit!(args, "scale", f64))
        .time(time::Duration::from_secs(value_t_or_exit!(
            args, "runtime", u64
        )))
        .in_flight(in_flight);

    if let Some(h) = args.value_of("histogram") {
        wl.with_histogram(h);
    }

    // check that we can indeed connect
    let mut opts = my::OptsBuilder::from_opts(args.value_of("dbn").unwrap());
    opts.tcp_nodelay(true);
    opts.pool_options(my::PoolOptions::with_constraints(
        my::PoolConstraints::new(in_flight, in_flight).unwrap(),
    ));
    // Atomic counter to generate ids for stories. This serves as a replacement for auto
    // increment the id column.
    // Preserve a parent counter so that it does not go out of scope.
    let stories_counter: Arc<Mutex<u16>> = Arc::new(Mutex::new(0));
    let taggings_counter: Arc<Mutex<u16>> = Arc::new(Mutex::new(0));
    let votes_counter: Arc<Mutex<u16>> = Arc::new(Mutex::new(0));
    let ribbons_counter: Arc<Mutex<u16>> = Arc::new(Mutex::new(0));
    let comments_counter: Arc<Mutex<u16>> = Arc::new(Mutex::new(0));


    let s = MysqlTrawlerBuilder {
        variant,
        opts: opts.into(),
        stories_counter: stories_counter,
        taggings_counter: taggings_counter,
        votes_counter: votes_counter,
        ribbons_counter: ribbons_counter,
        comments_counter: comments_counter,
    };

    wl.run(s, args.is_present("prime"));
}
