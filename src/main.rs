#![feature(integer_atomics)]

extern crate clap;
extern crate yapb;
extern crate termion;
extern crate failure;
extern crate rayon;
extern crate rdedup_lib as rdedup;
extern crate url;
extern crate rpassword;

use std::{fmt, thread, cmp};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Arg, App};
use yapb::{Spinner, Progress};
use failure::{Fail, Error, ResultExt};
use rayon::prelude::*;
use url::Url;

type Result<T> = std::result::Result<T, Error>;

pub struct PrettyErr<'a>(&'a Fail);
impl<'a> fmt::Display for PrettyErr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)?;
        let mut x: &Fail = self.0;
        while let Some(cause) = x.cause() {
            f.write_str(": ")?;
            fmt::Display::fmt(&cause, f)?;
            x = cause;
        }
        Ok(())
    }
}

pub trait ErrorExt {
    fn pretty(&self) -> PrettyErr;
}

impl ErrorExt for Error {
    fn pretty(&self) -> PrettyErr { PrettyErr(self.cause()) }
}

fn main() {
    let args = App::new("rackup")
        .version("0.1")
        .author("Benjamin Saunders <ben.e.saunders@gmail.com>")
        .about("A back-up tool")
        .arg(Arg::with_name("REPO")
             .help("rdedup repository URL to write to")
             .required(true)
             .validator(|x| Url::parse(&x).map(|_| ()).map_err(|x| x.to_string())))
        .arg(Arg::with_name("SOURCE")
             .help("location of data to back up")
             .default_value("."))
        .arg(Arg::with_name("NAME")
             .long("name")
             .short("n")
             .help("rdedup name to write to")
             .default_value("backup"))
        .get_matches();

    if let Err(e) = run(args) {
        eprintln!("FATAL: {}", e.pretty());
        ::std::process::exit(1);
    }
}

fn run<'a>(args: clap::ArgMatches<'a>) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let repo = rdedup::Repo::open(&Url::parse(args.value_of("REPO").unwrap()).unwrap(), None)
        .context("failed to open rdedup repo")?;
    let encrypt = repo.unlock_encrypt(&|| {
        println!("Enter passprase: ");
        rpassword::read_password()
    }).context("failed to prepare encryption for rdedup repo")?;

    let index = {
        let source = PathBuf::from(args.value_of_os("SOURCE").unwrap());
        let (send, recv) = mpsc::sync_channel(16);
        let source2 = source.clone();
        let thread = thread::spawn(move || {
            let index = walk_dir(send, source2);
            index
        });
        let index;
        let mut spinner = yapb::Snake::new();
        loop {
            match recv.recv_timeout(Duration::from_millis(100)) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    write!(stdout, "{}{}{} indexing {}",
                           termion::cursor::Left(!0), termion::clear::CurrentLine,
                           spinner, source.display()).unwrap();
                    stdout.flush().unwrap();
                    spinner.step(1);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    index = thread.join().unwrap()
                        .with_context(|_| format!("failed to index {}", source.display()))?;
                    break;
                }
                Ok(e) => {
                    eprintln!("ERROR: {}", e.pretty());
                }
            }
        }
        index
    };

    writeln!(stdout, "{}indexed {} files, {}B",
             termion::cursor::Left(!0),
             index.files.len(),
             yapb::prefix::Binary(index.bytes as f64)).unwrap();

    let total_bytes = index.bytes;
    let start_time = Instant::now();
    let (reader, status) = DataReader::new(index);
    let name = args.value_of("NAME").unwrap().to_owned();
    let thread = thread::spawn(move || -> Result<(u64, usize)> {
        let stats = repo.write(&name, reader, &encrypt)?;
        Ok((stats.new_bytes, stats.new_chunks))
    });

    let mut bar = yapb::Bar::new();
    let mut spinner = yapb::Snake::new();
    let mut throughput = None;
    let mut last_bytes = 0;
    loop {
        match status.errors.recv_timeout(Duration::from_millis(100)) {
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let (width, _) = termion::terminal_size().unwrap();
                let bytes = status.bytes.load(Ordering::Relaxed);
                let rate = (bytes - last_bytes) as f32;
                if throughput.is_none() {
                    throughput = Some(yapb::MovingAverage::new(0.05, rate));
                } else {
                    throughput.as_mut().unwrap().update(rate);
                }
                last_bytes = bytes;
                bar.set(cmp::min(total_bytes, bytes) as f32 / total_bytes as f32);
                write!(stdout, "{}{} {:3}% [{:width$}] {}B/s{}",
                       termion::cursor::Left(!0), spinner, (bar.get() * 100.0) as u32, bar,
                       yapb::prefix::Binary(throughput.as_ref().unwrap().get() as f64),
                       termion::clear::UntilNewline,
                       width = width as usize - 20).unwrap();
                stdout.flush().unwrap();
                spinner.step(1);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let (bytes, chunks) = thread.join().unwrap().context("write failed")?;
                let time = Instant::now() - start_time;
                writeln!(stdout, "{}{}backup completed in {}s: wrote {}B in {} chunks",
                         termion::cursor::Left(!0), termion::clear::AfterCursor,
                         yapb::prefix::Scientific(time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9),
                         yapb::prefix::Binary(bytes as f64), chunks).unwrap();
                break;
            }
            Ok(e) => {
                eprintln!("ERROR: {}{}", e.pretty(), termion::clear::UntilNewline);
            }
        }
    }

    Ok(())
}

struct Index {
    files: Vec<PathBuf>,
    bytes: u64,
}

impl Index {
    fn new() -> Self { Self {
        files: Vec::new(),
        bytes: 0,
    }}

    fn push(&mut self, path: PathBuf, meta: &fs::Metadata) {
        self.files.push(path);
        self.bytes += meta.len();
    }
}

impl ::std::ops::Add for Index {
    type Output = Index;

    fn add(mut self, other: Index) -> Index {
        self.files.extend(other.files.into_iter());
        self.bytes += other.bytes;
        self
    }
}

impl ::std::iter::Sum for Index {
    fn sum<I>(iter: I) -> Self
        where I: Iterator<Item = Index>
    {
        iter.fold(Index::new(), |x, y| x + y)
    }
}

fn walk_dir(errs: mpsc::SyncSender<Error>, dir: PathBuf) -> Result<Index> {
    let mut index = Index::new();
    let subdirs = fs::read_dir(&dir).with_context(|_| format!("failed to access {}", dir.display()))
        ?.map(|entry| {
            let entry = entry.with_context(|_| format!("failed to access entry of {}", dir.display()))?;
            let path = entry.path();
            if entry.file_type().context("failed to determine directory entry type")?.is_dir() {
                Ok(Some(path))
            } else {
                let meta = entry.metadata()
                    .with_context(|_| format!("failed to access metadata for {}", path.display()))?;
                index.push(path, &meta);
                Ok(None)
            }
        })
        .filter_map(|x| x.map(|x| x.map(Ok)).unwrap_or_else(|x| Some(Err(x))) )
        .collect::<Vec<_>>()
        .into_par_iter()
        .filter_map(|dir| match dir.and_then(|x| walk_dir(errs.clone(), x)) {
            Err(e) => { errs.send(e).unwrap(); None }
            Ok(x) => Some(x)
        })
        .sum::<Index>();
    Ok(subdirs + index)
}

struct DataReader {
    index: Index,
    offsets: Vec<u64>,
    next: usize,
    file: Option<File>,
    bytes: Arc<AtomicU64>,
    errors: mpsc::SyncSender<Error>
}

struct ReadStatus {
    bytes: Arc<AtomicU64>,
    errors: mpsc::Receiver<Error>,
}

impl DataReader {
    fn new(index: Index) -> (Self, ReadStatus) {
        let bytes = Arc::new(AtomicU64::new(0));
        let (err_send, err_recv) = mpsc::sync_channel(16);
        let result = Self {
            offsets: Vec::with_capacity(index.files.len()),
            index,
            next: 0,
            file: None,
            bytes: bytes.clone(),
            errors: err_send,
        };
        let status = ReadStatus {
            bytes,
            errors: err_recv,
        };
        (result, status)
    }
}

impl Read for DataReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            while self.file.is_none() {
                if self.next == self.index.files.len() { return Ok(0); }
                match File::open(&self.index.files[self.next])
                    .with_context(|_| format!("couldn't open {}", self.index.files[self.next].display()))
                {
                    Ok(file) => { self.file = Some(file); }
                    Err(e) => { self.errors.send(e.into()).unwrap(); }
                }
                self.offsets.push(self.bytes.load(Ordering::Relaxed));
                self.next += 1;
            }
            // TODO: Write index
            // TODO: Handle symlinks
            match self.file.as_mut().unwrap().read(buf)
                .with_context(|_| format!("failed reading {}", self.index.files[self.next-1].display()))
            {
                Ok(0) => { self.file = None; }
                Ok(n) => { self.bytes.fetch_add(n as u64, Ordering::Relaxed); return Ok(n); }
                Err(e) => { self.errors.send(e.into()).unwrap(); }
            }
        }
    }
}
