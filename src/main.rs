#![feature(integer_atomics)]

extern crate clap;
extern crate yapb;
extern crate termion;
#[macro_use]
extern crate failure;
extern crate rayon;
extern crate rdedup_lib as rdedup;
extern crate url;
extern crate rpassword;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;

use std::{fmt, thread, cmp, fs};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Arg, App};
use yapb::{Spinner, Progress};
use failure::{Fail, Error, ResultExt};
use rayon::prelude::*;
use url::Url;

mod meta;

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
        let start_time = Instant::now();
        let thread = thread::spawn(move || {
            let index = walk_dir(send, &source2);
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
                    let time = Instant::now() - start_time;

                    writeln!(stdout, "{}indexed {} files, {}B in {}s",
                             termion::cursor::Left(!0),
                             index.cumulative_files,
                             yapb::prefix::Binary(index.cumulative_bytes as f64),
                             yapb::prefix::Scientific(time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9)).unwrap();

                    break;
                }
                Ok(e) => {
                    eprintln!("ERROR: {}", e.pretty());
                }
            }
        }
        index
    };

    let total_bytes = index.cumulative_bytes;
    let start_time = Instant::now();
    let (mut reader, status) = DataReader::new(index);
    let name = args.value_of("NAME").unwrap().to_owned();
    let thread = thread::spawn(move || -> Result<(u64, usize)> {
        let stats = repo.write(&name, &mut reader, &encrypt)?;
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
                write!(stdout, "{}{}backup completed in {}s: ",
                       termion::cursor::Left(!0), termion::clear::AfterCursor,
                       yapb::prefix::Scientific(time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9)).unwrap();
                if chunks == 0 {
                    writeln!(stdout, "no new data").unwrap();
                } else {
                    writeln!(stdout, "stored {} new chunks totalling {}B",
                             chunks, yapb::prefix::Binary(bytes as f64)).unwrap();
                }
                break;
            }
            Ok(e) => {
                eprintln!("ERROR: {}{}", e.pretty(), termion::clear::UntilNewline);
            }
        }
    }

    Ok(())
}

struct Directory {
    files: Vec<PathBuf>,
    meta: Vec<meta::File>,
    subdirs: Vec<(PathBuf, Directory)>,
    cumulative_bytes: u64,
    cumulative_files: u64,
}

impl Directory {
    fn new() -> Self { Self {
        files: Vec::new(),
        meta: Vec::new(),
        subdirs: Vec::new(),
        cumulative_bytes: 0,
        cumulative_files: 0,
    }}

    fn push(&mut self, path: PathBuf, meta: &fs::Metadata) {
        self.files.push(path);
        self.cumulative_bytes += meta.len();
        self.cumulative_files += 1;
    }

    fn get_dir(&mut self, path: &[usize]) -> &mut Directory {
        if path.len() == 0 { self }
        else { self.subdirs[path[0]].1.get_dir(&path[1..]) }
    }
}

impl ::std::ops::Add for Directory {
    type Output = Directory;

    fn add(mut self, other: Directory) -> Directory {
        self.files.extend(other.files.into_iter());
        self.cumulative_bytes += other.cumulative_bytes;
        self
    }
}

impl ::std::iter::Sum for Directory {
    fn sum<I>(iter: I) -> Self
        where I: Iterator<Item = Directory>
    {
        iter.fold(Directory::new(), |x, y| x + y)
    }
}

fn walk_dir(errs: mpsc::SyncSender<Error>, dir: &Path) -> Result<Directory> {
    let mut index = Directory::new();
    let subdirs = fs::read_dir(dir).with_context(|_| format!("failed to access {}", dir.display()))
        ?.map(|entry| {
            let entry = entry.with_context(|_| format!("failed to access entry of {}", dir.display()))?;
            let path = entry.path();
            if path.to_str().is_none() {
                errs.send(format_err!("skipping non-unicode path {}", path.display())).unwrap();
                Ok(None)
            } else if entry.file_type().context("failed to determine directory entry type")?.is_dir() {
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
        .filter_map(|dir| match dir.and_then(|x| walk_dir(errs.clone(), &x).map(|dir| (x, dir))) {
            Err(e) => { errs.send(e).unwrap(); None }
            Ok(x) => Some(x)
        })
        .collect::<Vec<_>>();
    index.subdirs = subdirs;
    for &(_, ref subdir) in &index.subdirs {
        index.cumulative_bytes += subdir.cumulative_bytes;
        index.cumulative_files += subdir.cumulative_files;
    }
    Ok(index)
}

struct DataReader {
    root: Directory,
    subdir: Vec<usize>,
    next_file: usize,
    file: Option<fs::File>,
    bytes: Arc<AtomicU64>,
    errors: mpsc::SyncSender<Error>
}

struct ReadStatus {
    bytes: Arc<AtomicU64>,
    errors: mpsc::Receiver<Error>,
}

impl DataReader {
    fn new(root: Directory) -> (Self, ReadStatus) {
        let bytes = Arc::new(AtomicU64::new(0));
        let (err_send, err_recv) = mpsc::sync_channel(16);
        let result = Self {
            root,
            subdir: Vec::new(),
            next_file: 0,
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

// f(dir) {
//   for file in dir { read; }
//   for sub in dir { f(sub); }
// }

fn advance_subdir(subdir: &mut Vec<usize>, root: &mut Directory) -> bool {
    // a/b
    // c
    subdir.push(0);
    loop {
        {
            let (&head, tail) = subdir.split_last().unwrap();
            if root.get_dir(tail).subdirs.len() > head {
                return false;
            }
        }

        // No subdirs left at this level
        subdir.pop().unwrap();
        if let Some(x) = subdir.pop() {
            subdir.push(x+1);
        } else {
            // We were at the top level, so we're done
            return true;
        }
    }
}

impl Read for DataReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            while self.file.is_none() {
                while self.next_file == self.root.get_dir(&self.subdir).files.len() {
                    if advance_subdir(&mut self.subdir, &mut self.root) {
                        return Ok(0);
                    }
                    self.next_file = 0;
                }

                let dir = self.root.get_dir(&self.subdir);
                let path = &dir.files[self.next_file];
                let name = path.to_str().unwrap().as_bytes().to_vec();
                match fs::symlink_metadata(path)
                    .with_context(|_| format!("couldn't stat {}", path.display()))
                {
                    Ok(meta) => {
                        if meta.file_type().is_symlink() {
                            match fs::read_link(path)
                                .with_context(|_| format!("couldn't readlink {}", path.display()))
                                .map_err(|e| -> Error { e.into() })
                                .and_then(|target| target.to_str()
                                          .ok_or_else(|| format_err!("skipping non-unicode symlink {}", target.display()))
                                          .map(|x| x.as_bytes().to_vec()))
                            {
                                Ok(target) => {
                                    dir.meta.push(meta::File {
                                        name, meta: meta::Data::Link {
                                            target: target
                                        }
                                    });
                                }
                                Err(e) => {
                                    self.errors.send(e).unwrap();
                                }
                            }
                        } else {
                            match fs::File::open(&dir.files[self.next_file])
                                .with_context(|_| format!("couldn't open {}", path.display()))
                            {
                                Ok(file) => {
                                    dir.meta.push(meta::File {
                                        name, meta: meta::Data::Regular {
                                            offset: self.bytes.load(Ordering::Relaxed),
                                            length: 0,
                                        }
                                    });
                                    self.file = Some(file);
                                }
                                Err(e) => { self.errors.send(e.into()).unwrap(); }
                            }
                        }
                    }
                    Err(e) => { self.errors.send(e.into()).unwrap(); }
                }
                self.next_file += 1;
            }
            let dir = self.root.get_dir(&self.subdir);
            let path = &dir.files[self.next_file-1];
            // TODO: Write index
            match self.file.as_mut().unwrap().read(buf)
                .with_context(|_| format!("failed reading {}", path.display()))
            {
                Ok(0) => { self.file = None; }
                Ok(n) => {
                    self.bytes.fetch_add(n as u64, Ordering::Relaxed);
                    if let meta::Data::Regular { ref mut length, .. } = dir.meta.last_mut().unwrap().meta {
                        *length += n as u64;
                    } else { unreachable!() }
                    return Ok(n);
                }
                Err(e) => {
                    self.errors.send(e.into()).unwrap();
                    self.file = None;
                }
            }
        }
    }
}
