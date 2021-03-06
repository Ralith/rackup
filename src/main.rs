#![feature(integer_atomics)]

extern crate clap;
extern crate yapb;
extern crate termion;
#[macro_use]
extern crate failure;
extern crate rdedup_lib as rdedup;
extern crate url;
extern crate rpassword;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate walkdir;

use std::{fmt, thread, cmp, fs};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{App, AppSettings, SubCommand, Arg};
use yapb::{Spinner, Progress};
use failure::{Fail, Error, ResultExt};
use url::Url;
use walkdir::WalkDir;

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
        .setting(AppSettings::SubcommandRequired)
        .version("0.1")
        .author("Benjamin Saunders <ben.e.saunders@gmail.com>")
        .about("A back-up tool")
        .arg(Arg::with_name("REPO")
             .help("rdedup repository URL to write to")
             .required(true)
             .validator(|x| Url::parse(&x).map(|_| ()).map_err(|x| x.to_string())))
        .arg(Arg::with_name("NAME")
             .required(true)
             .help("rdedup name to operate on"))
        .subcommand(SubCommand::with_name("store").about("Create a new backup")
                    .arg(Arg::with_name("SOURCE")
                         .help("location of data to back up")
                         .default_value(".")))
        .subcommand(SubCommand::with_name("ls").about("Access metadata for a path in an existing backup")
                    .arg(Arg::with_name("PATH")
                         .help("path within the backup to inspect")))
        .subcommand(SubCommand::with_name("cat").about("Read a single file in an existing backup")
                    .arg(Arg::with_name("PATH")
                         .help("path within the backup to read")))
        .get_matches();

    if let Err(e) = run(args) {
        eprintln!("FATAL: {}", e.pretty());
        ::std::process::exit(1);
    }
}

fn run<'a>(args: clap::ArgMatches<'a>) -> Result<()> {
    let repo = rdedup::Repo::open(&Url::parse(args.value_of("REPO").unwrap()).unwrap(), None)
        .context("failed to open rdedup repo")?;
    let name = args.value_of("NAME").unwrap().to_owned();
    match args.subcommand() {
        ("store", Some(args)) => store(repo, name, args),
        ("ls", Some(args)) => list(repo, name, args),
        ("cat", _) => unimplemented!(),
        _ => unreachable!(),
    }
}

fn store<'a>(repo: rdedup::Repo, name: String, args: &clap::ArgMatches<'a>) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let encrypt = Arc::new(
        repo.unlock_encrypt(&|| {
            println!("Enter passprase: ");
            rpassword::read_password()
        }).context("failed to prepare encryption for rdedup repo")?
    );

    let source = PathBuf::from(args.value_of_os("SOURCE").unwrap());
    let stats = {
        let (send, recv) = mpsc::sync_channel(16);
        let source2 = source.clone();
        let start_time = Instant::now();
        let thread = thread::spawn(move || { scan(send, &source2) });
        let stats;
        let mut spinner = yapb::Snake::new();
        loop {
            match recv.recv_timeout(Duration::from_millis(100)) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    write!(stdout, "{}{}{} scanning {}",
                           termion::cursor::Left(!0), termion::clear::CurrentLine,
                           spinner, source.display()).unwrap();
                    stdout.flush().unwrap();
                    spinner.step(1);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    stats = thread.join().unwrap();
                    let time = Instant::now() - start_time;

                    writeln!(stdout, "{}scanned {} files, {}B in {}s",
                             termion::cursor::Left(!0),
                             stats.files,
                             yapb::prefix::Binary(stats.bytes as f64),
                             yapb::prefix::Scientific(time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9)).unwrap();

                    break;
                }
                Ok(e) => {
                    eprintln!("ERROR: {}", e.pretty());
                }
            }
        }
        stats
    };

    let start_time = Instant::now();
    let meta_name = name.clone() + "-meta";
    let (mut data_reader, status, mut meta_reader) = DataReader::new(&source);
    let repo = Arc::new(repo);
    let data_thread = {
        let repo = repo.clone();
        let encrypt = encrypt.clone();
        thread::spawn(move || -> Result<(u64, usize)> {
            let stats = repo.write(&name, &mut data_reader, &encrypt)?;
            Ok((stats.new_bytes, stats.new_chunks))
        })
    };
    let meta_thread = thread::spawn(move || -> Result<(u64, usize)> {
        let stats = repo.write(&meta_name, &mut meta_reader, &encrypt)?;
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
                bar.set(cmp::min(stats.bytes, bytes) as f32 / stats.bytes as f32);
                write!(stdout, "{}{} {:3}% [{:width$}] {}B/s{}",
                       termion::cursor::Left(!0), spinner, (bar.get() * 100.0) as u32, bar,
                       yapb::prefix::Binary(throughput.as_ref().unwrap().get() as f64),
                       termion::clear::UntilNewline,
                       width = width as usize - 20).unwrap();
                stdout.flush().unwrap();
                spinner.step(1);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                write!(stdout, "{}{}", termion::cursor::Left(!0), termion::clear::AfterCursor).unwrap();
                stdout.flush().unwrap();
                let (bytes, chunks) = data_thread.join().unwrap().context("write failed")?;
                let (metabytes, metachunks) = meta_thread.join().unwrap().context("metadata write failed")?;
                let time = Instant::now() - start_time;
                writeln!(stdout, "backup completed in {}s: ",
                         yapb::prefix::Scientific(time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9)).unwrap();
                if chunks == 0 {
                    writeln!(stdout, "no new data").unwrap();
                } else {
                    writeln!(stdout, "stored {} new chunks totalling {}B",
                             chunks, yapb::prefix::Binary(bytes as f64)).unwrap();
                }
                if metachunks == 0 {
                    writeln!(stdout, "no new metadata").unwrap();
                } else {
                    writeln!(stdout, "stored {} new metadata chunks totalling {}B",
                             metachunks, yapb::prefix::Binary(metabytes as f64)).unwrap();
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

struct ScanStats {
    bytes: u64,
    files: u64,
}

impl ScanStats {
    fn new(meta: &fs::Metadata) -> Self { Self {
        bytes: meta.len(),
        files: 1,
    }}
}

impl Default for ScanStats {
    fn default() -> Self { Self { bytes: 0, files: 0 } }
}

impl ::std::ops::Add for ScanStats {
    type Output = ScanStats;

    fn add(mut self, other: ScanStats) -> ScanStats {
        self.bytes += other.bytes;
        self.files += 1;
        self
    }
}

impl ::std::iter::Sum for ScanStats {
    fn sum<I>(iter: I) -> Self
        where I: Iterator<Item = ScanStats>
    {
        iter.fold(ScanStats::default(), |x, y| x + y)
    }
}

fn scan(errs: mpsc::SyncSender<Error>, dir: &Path) -> ScanStats {
    WalkDir::new(dir).into_iter()
        .map(|entry| {
            let entry = entry.with_context(|_| format_err!("error scanning {}", dir.display()))?;
            if entry.file_type().is_dir() {
                Ok(ScanStats::default())
            } else {
                let meta = entry.metadata().with_context(|_| format_err!("error scanning {}", entry.path().display()))?;
                Ok(ScanStats::new(&meta))
            }
        })
        .filter_map(|x| match x {
            Err(e) => { errs.send(e).unwrap(); None }
            Ok(x) => { Some(x) }
        })
        .sum()
}

struct Meta {
    entry: walkdir::DirEntry,
    offset: u64,
    len: u64,
}

struct DataReader {
    iter: walkdir::IntoIter,
    file: Option<(fs::File, Meta)>,
    bytes: Arc<AtomicU64>,
    errors: mpsc::SyncSender<Error>,
    meta: mpsc::SyncSender<Meta>,
}

struct ReadStatus {
    bytes: Arc<AtomicU64>,
    errors: mpsc::Receiver<Error>,
}

impl DataReader {
    fn new(dir: &Path) -> (Self, ReadStatus, MetaReader) {
        let bytes = Arc::new(AtomicU64::new(0));
        let (err_send, err_recv) = mpsc::sync_channel(16);
        let (meta_send, meta_recv) = mpsc::sync_channel(128);
        (Self {
            iter: WalkDir::new(dir).contents_first(true).into_iter(),
            file: None,
            bytes: bytes.clone(),
            errors: err_send.clone(),
            meta: meta_send,
        },
         ReadStatus { bytes: bytes, errors: err_recv },
         MetaReader {
             base_depth: dir.iter().count() - 1,
             meta: meta_recv,
             cursor: 0,
             buffer: io::Cursor::new(Vec::new()),
             dirs: Vec::new(),
             errors: err_send,
         })
    }
}

impl Read for DataReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            while self.file.is_none() {
                match self.iter.next() {
                    None => { return Ok(0); }
                    Some(Err(e)) => { self.errors.send(e.context("failed while scanning").into()).unwrap(); }
                    Some(Ok(x)) => {
                        if x.path().to_str().is_none() {
                            self.errors.send(format_err!("ignoring non-unicode path {}", x.path().display())).unwrap();
                            continue;
                        }
                        let meta = Meta {
                            entry: x,
                            offset: self.bytes.load(Ordering::Relaxed),
                            len: 0,
                        };
                        if meta.entry.file_type().is_file() {
                            match fs::File::open(meta.entry.path())
                                .with_context(|_| format_err!("failed to open {}", meta.entry.path().display()))
                            {
                                Err(e) => { self.errors.send(e.into()).unwrap(); }
                                Ok(f) => { self.file = Some((f, meta)); }
                            }
                        } else {
                            self.meta.send(meta).unwrap();
                        }
                    }
                }
            }

            match self.file.as_mut().unwrap().0.read(buf) {
                Ok(0) => {
                    let (_, meta) = self.file.take().unwrap();
                    self.meta.send(meta).unwrap();
                }
                Ok(n) => {
                    self.bytes.fetch_add(n as u64, Ordering::Relaxed);
                    self.file.as_mut().unwrap().1.len += n as u64;
                    return Ok(n);
                }
                // We deliberately don't yield metadata for files with read errors.
                Err(e) => { self.errors.send(e.into()).unwrap(); self.file = None; }
            }
        }
    }
}

struct MetaReader {
    base_depth: usize,
    meta: mpsc::Receiver<Meta>,
    cursor: u64,
    buffer: io::Cursor<Vec<u8>>,
    dirs: Vec<(Box<[u8]>, Vec<meta::Entry>)>,
    errors: mpsc::SyncSender<Error>,
}

impl Read for MetaReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.buffer.position() != self.buffer.get_ref().len() as u64 {
                let n = self.buffer.read(buf)?;
                self.cursor += n as u64;
                return Ok(n);
            }
            self.buffer.set_position(0);
            self.buffer.get_mut().clear();
            let x = match self.meta.recv() {
                Err(mpsc::RecvError) => { assert!(self.dirs.is_empty(), "wrote all metadata"); return Ok(0); }
                Ok(x) => x,
            };
            //eprintln!("processing {}", x.path().display());
            if x.entry.depth() > self.dirs.len() {
                let prior_depth = self.dirs.len();
                //eprintln!("depth: base {}, prior {}, new {}", self.base_depth, prior_depth, x.depth());
                self.dirs.extend(x.entry.path().iter()
                                 .skip(self.base_depth + prior_depth)
                                 .take(x.entry.depth() - prior_depth)
                                 .map(|x| (x.to_str().unwrap().as_bytes().to_vec().into_boxed_slice(), Vec::new())));
                // eprint!("new dir:");
                // for dir in &self.dirs {
                //     eprint!(" {}", ::std::str::from_utf8(&dir.0).unwrap());
                // }
                // eprintln!("");
            }
            if x.entry.file_type().is_dir() {
                let files = if self.dirs.len() > x.entry.depth() {
                    let (_, entries) = self.dirs.pop().unwrap();
                    for entry in &entries {
                        bincode::serialize_into(&mut self.buffer, entry, bincode::Infinite).unwrap();
                    }
                    entries.len() as u64
                } else { 0 };
                if let Some(dir) = self.dirs.last_mut() {
                    dir.1.push(meta::Entry {
                        name: x.entry.file_name().to_str().unwrap().as_bytes().to_vec().into_boxed_slice(),
                        data: meta::Data::Directory { offset: self.cursor, files }
                    });
                } else {
                    bincode::serialize_into(&mut self.buffer, &(self.cursor, files), bincode::Infinite).unwrap()
                }
                self.buffer.set_position(0);
            } else if x.entry.path_is_symlink() {
                match fs::read_link(x.entry.path())
                    .map_err(|e| e.context(format_err!("failed following symlink {}", x.entry.path().display())).into())
                    .and_then(|path| path.to_str()
                              .ok_or_else(|| format_err!("ignoring non-unicode-targeted symlink {}", x.entry.path().display()))
                              .map(|x| x.as_bytes().to_vec().into_boxed_slice()))
                {
                    Err(e) => {
                        self.errors.send(e).unwrap();
                    }
                    Ok(target) => {
                        self.dirs.last_mut().unwrap().1.push(meta::Entry {
                            name: x.entry.file_name().to_str().unwrap().as_bytes().to_vec().into_boxed_slice(),
                            data: meta::Data::Link { target },
                        });
                    }
                };
            } else {
                self.dirs.last_mut().unwrap().1.push(meta::Entry {
                    name: x.entry.file_name().to_str().unwrap().as_bytes().to_vec().into_boxed_slice(),
                    data: meta::Data::Regular { offset: x.offset, len: x.len },
                });
            }
        }
    }
}

fn list<'a>(repo: rdedup::Repo, name: String, args: &clap::ArgMatches<'a>) -> Result<()> {
    let decrypt = repo.unlock_decrypt(&|| {
        println!("Enter passprase: ");
        rpassword::read_password()
    }).context("failed to decrypt rdedup repo")?;

    // TODO: Random access
    let mut meta = Vec::new();
    repo.read(&(name + "-meta"), &mut meta, &decrypt)?;

    let root_size = bincode::serialized_size(&(0u64, 0u64));
    let (offset, files): (u64, u64) = bincode::deserialize(&meta[meta.len()-(root_size as usize)..]).unwrap();
    let path = args.value_of("PATH").unwrap_or("");
    list_inner(&meta, path.as_bytes(), meta::Entry {
        name: b"/".to_vec().into_boxed_slice(),
        data: meta::Data::Directory { offset, files },
    })
}

fn list_inner(meta: &[u8], path: &[u8], base: meta::Entry) -> Result<()> {
    if path.is_empty() {
        match base.data {
            meta::Data::Directory { offset, files } => {
                let mut dir = &meta[offset as usize..];
                for _ in 0..files {
                    let entry: meta::Entry = bincode::deserialize_from(&mut dir, bincode::Infinite).unwrap();
                    match entry.data {
                        meta::Data::Directory { .. } => println!("{}/", String::from_utf8_lossy(&entry.name)),
                        meta::Data::Regular { len, .. } => println!("{}\t{}B", String::from_utf8_lossy(&entry.name), yapb::prefix::Binary(len as f64)),
                        meta::Data::Link { target } => println!("{}\t-> {}", String::from_utf8_lossy(&base.name), String::from_utf8_lossy(&target)),
                    }
                }
            }
            meta::Data::Regular { len, .. } => println!("{}\t{}B", String::from_utf8_lossy(&base.name), yapb::prefix::Binary(len as f64)),
            meta::Data::Link { target } => println!("{}\t-> {}", String::from_utf8_lossy(&base.name), String::from_utf8_lossy(&target)),
        }
        Ok(())
    } else {
        match base.data {
            meta::Data::Directory { offset, files } => {
                let delim = path.iter().position(|&x| x == b'/').unwrap_or(path.len());
                let name = &path[0..delim];
                let mut dir = &meta[offset as usize..];
                for _ in 0..files {
                    let entry: meta::Entry = bincode::deserialize_from(&mut dir, bincode::Infinite).unwrap();
                    if &*entry.name == name {
                        return list_inner(meta, &path[cmp::min(path.len(), delim+1)..], entry);
                    }
                }
                bail!("{}: no such file or directory", String::from_utf8_lossy(name));
            }
            _ => { bail!("{} is not a directory", String::from_utf8_lossy(&base.name)); }
        }
    }
}
