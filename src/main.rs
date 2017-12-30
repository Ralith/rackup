#![recursion_limit = "1024"]   // for error-chain

extern crate clap;
extern crate indicatif;
extern crate gpgme;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate futures_cpupool;
extern crate zstd;

use std::{fs, fmt};
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Arg, ArgGroup, App};
use indicatif::{ProgressBar, MultiProgress, ProgressStyle};
use error_chain::ChainedError;
use futures::{Future, Stream, stream};
use futures_cpupool::CpuPool;

error_chain! {}

pub struct PrettyErr<'a, E: ChainedError>(&'a E);

impl<'a, E: ChainedError> fmt::Display for PrettyErr<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)?;
        for x in self.0.iter().skip(1) {
            f.pad(": ")?;
            fmt::Display::fmt(&x, f)?;
        }
        Ok(())
    }
}

/// Compact display wrapper for the full cause chain of an error.
pub fn pretty_err<E: ChainedError>(x: &E) -> PrettyErr<E> { PrettyErr(x) }


fn main() {
    let args = App::new("rackup")
        .version("0.1")
        .author("Benjamin Saunders <ben.e.saunders@gmail.com>")
        .about("A back-up tool")
        .arg(Arg::with_name("ID")
             .help("Identifier for this backup")
             .required(true))
        .arg(Arg::with_name("SOURCE")
             .help("Location of data to back up")
             .default_value("."))
        .arg(Arg::with_name("local dest")
             .takes_value(true)
             .help("Destination for backed-up data on the local filesystem")
             .short("l")
             .long("local"))
        .arg(Arg::with_name("key")
             .takes_value(true)
             .help("GPG public key to encrypt to")
             .short("k")
             .long("key"))
        .group(ArgGroup::with_name("destination")
               .args(&["local dest"])
               .required(true))
        .get_matches();

    if let Err(e) = run(args) {
        eprintln!("{}", pretty_err(&e));
        ::std::process::exit(1);
    }
}

fn run<'a>(args: clap::ArgMatches<'a>) -> Result<()> {
    let ctx = Arc::new(Context::new(args)?);
    
    let walk = walk_dir(ctx.clone(), ctx.prefix.clone());

    ctx.progress.join_and_clear().unwrap();
    println!("progress finished");
    walk.wait().unwrap();
    Ok(())
}

fn walk_dir(ctx: Arc<Context>, dir: PathBuf) -> Box<Future<Item=(), Error=()> + Send> { // TODO: No errors!
    let pb = ctx.progress.add(ProgressBar::new_spinner());
    pb.set_style(ProgressStyle::default_spinner()
                 .template("{spinner} {msg}"));
    let ctx2 = ctx.clone();
    Box::new(ctx2.pool.spawn_fn(move || {
        let mut spawned = Vec::new();
        if let Err(e) = (|| -> Result<()> {
            for entry in fs::read_dir(&dir).chain_err(|| format!("failed to access {}", dir.display()))? {
                let result: Result<()> = (|| {
                    let entry = entry.chain_err(|| format!("failed to access entry of {}", dir.display()))?;
                    pb.set_message(&format!("checking {}", entry.path().strip_prefix(&ctx.prefix).unwrap().display()));
                    if entry.file_type().chain_err(|| "failed to determine directory entry type")?.is_dir() {
                        spawned.push(walk_dir(ctx.clone(), entry.path()));
                    } else {
                        let meta = entry.metadata().chain_err(|| format!("failed to access metadata for {}", entry.path().display()))?;
                        //spawned.push(check(&pool2, m.clone(), entry.path(), meta));
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    eprintln!("ERROR: {}", pretty_err(&e));
                }
            }
            Ok(())
        })() {
            eprintln!("ERROR: {}", pretty_err(&e));
        }
        pb.finish_and_clear();
        stream::futures_unordered(spawned)
            .for_each(|()| Ok(()))
    }))
}

struct Context {
    gpg: gpgme::Token,
    pool: CpuPool,
    progress: Arc<MultiProgress>,
    prefix: PathBuf,
}

impl Context {
    pub fn new<'a>(args: clap::ArgMatches<'a>) -> Result<Context> {
        Ok(Context {
            gpg: gpgme::init(),
            pool: CpuPool::new_num_cpus(),
            progress: Arc::new(MultiProgress::new()),
            prefix: args.value_of_os("SOURCE").unwrap().into(),
        })
    }
}
