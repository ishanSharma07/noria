use clap;
use std::future::Future;

#[derive(Copy, Clone, Debug)]
pub(crate) struct Parameters {
    pub(crate) prime: bool,
    pub(crate) articles: usize,
    // The following parameter is only used for pelton.
    pub(crate) mysql_backend: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct WriteRequest(pub Vec<i32>);

#[derive(Clone, Debug)]
pub(crate) struct ReadRequest(pub Vec<i32>);

pub(crate) trait VoteClient
where
    Self: Sized,
{
    type Future: Future<Output = Result<Self, failure::Error>> + Send + 'static;
    fn new(params: Parameters, args: clap::ArgMatches) -> <Self as VoteClient>::Future;
}

pub(crate) mod hybrid;
//pub(crate) mod localsoup;
pub(crate) mod memcached;
//pub(crate) mod mssql;
pub(crate) mod mysql;
pub(crate) mod pelton;
pub(crate) mod memcached_hybrid;
pub(crate) mod redis_hybrid;
//pub(crate) mod netsoup;
pub(crate) mod redis;
