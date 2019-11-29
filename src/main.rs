use clap::clap_app;
use std::io::{Seek, SeekFrom, Read, BufWriter, Write, StdoutLock};
use std::sync::{Arc, Condvar, Mutex, Barrier};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::fs::{OpenOptions, File};
use std::time::Duration;
use std::cmp::max;

fn main() {
    if let Err(e) = run() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let matches = clap_app!(("eat") =>
        (version: "0.1")
        (author: "Richard Patel <me@terorie.dev>")
        (about: "Does backwards truncating reads")
        (@arg block_size: -b --("block-size") +takes_value default_value("1048576") "Block size in bytes")
        (@arg lines: -n --lines +takes_value "Max lines")
        (@arg file: +required "File to read")
    ).get_matches();

    let file_path = matches.value_of("file").unwrap();
    let block_size = matches.value_of("block_size").unwrap()
        .parse::<isize>()
        .map_err(|_| "Invalid --block_size".to_owned())
        ?;
    if block_size < 128 {
        return Err("Too small --block_size".to_owned());
    }
    let max_lines = matches.value_of("lines")
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| "Invalid --lines".to_owned())
        ?;
    if max_lines == Some(0) {
        return Ok(());
    }

    // Open file
    let mut file = OpenOptions::new()
        .read(true).write(true)
        .open(file_path)
        .map_err(|e| format!("Failed to open file: {}", e))
        ?;

    // Get file size
    let mut file_size = file.seek(SeekFrom::End(0))
        .map_err(|e| format!("{}", e))
        ?;
    if file_size == 0 {
        return Ok(());
    }

    // Check for final newline
    {
        file.seek(SeekFrom::Current(-1))
            .map_err(|e| format!("Failed final newline seek: {}", e))
            ?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)
            .map_err(|e| format!("Failed final newline read: {}", e))
            ?;
        if buf[0] == b'\n' {
            file_size -= 1;
        }
    }

    // Setup shared state
    struct State {
        position: AtomicIsize,
        notify: Condvar,
        file: Mutex<File>,
        prepare: Barrier,
        thread_mutex: Mutex<()>,
    }
    let state = Arc::new(State {
        position: AtomicIsize::new(file_size as isize),
        notify: Condvar::new(),
        file: Mutex::new(file),
        prepare: Barrier::new(2),
        thread_mutex: Mutex::new(()),
    });
    let read_state = Arc::clone(&state);

    // Read file thread
    std::thread::spawn(move || {
        let state = read_state;
        state.prepare.wait();

        // Stdout Writer
        let stdout = std::io::stdout();
        let mut wr = LineWriter::new(stdout.lock(), max_lines);

        'outer: loop {
            let old_pos = state.position.load(Ordering::Relaxed);
            let new_pos = max(0, old_pos - block_size);
            let len = old_pos - new_pos;

            // Read chunk
            let mut file = state.file.lock().unwrap();
            file.seek(SeekFrom::Start(new_pos as u64))
                .map_err(|_| format!("Seek to {} failed", new_pos))
                .unwrap();
            let mut buf = vec![0u8; len as usize];
            file.read_exact(&mut buf).unwrap();
            drop(file);

            // Dump lines and continually update position
            // TODO Consider sendfile
            let mut tokens_iter = buf.rsplit(|byte| *byte == b'\n');
            let mut current_token = tokens_iter.next();
            while let Some(current) = current_token {
                if wr.done() {
                    break 'outer;
                }

                let next_token = tokens_iter.next();
                match next_token {
                    // There is another token
                    Some(_) => {
                        // Commit current token
                        if wr.write(current).is_err() {
                            break 'outer;
                        }
                        state.position.fetch_sub(current.len() as isize + 1, Ordering::Relaxed);
                    },
                    // current_token is the last one
                    None => if new_pos == 0 {
                        // Only commit if token first in file
                        if wr.write(current).is_err() {
                            break 'outer;
                        }
                        state.position.store(0, Ordering::Relaxed);
                        break 'outer;
                    }
                };
                current_token = next_token;
            }
        }

        // All done
        state.notify.notify_all();
    });

    // Poll lock, truncate
    state.prepare.wait();
    let mut guard = state.thread_mutex.lock().unwrap();
    let mut file_size = file_size;
    while file_size > 0 {
        // Wait for reader thread to finish
        let (next_guard, lock_status) =
            state.notify.wait_timeout(guard, Duration::from_millis(500)).unwrap();
        guard = next_guard;
        let timed_out = lock_status.timed_out();

        // Truncate file
        let pos = state.position.load(Ordering::Relaxed) as u64;
        if pos < file_size {
            let file = state.file.lock().unwrap();
            let target = if pos > 0 { pos + 1 } else { 0 };
            if let Err(e) = file.set_len(target) {
                eprintln!("Failed to truncate file: {}", e);
                std::process::exit(1);
            }
        }
        file_size = pos;

        // If notified, end is reached
        if !timed_out {
            break;
        }
    }

    Ok(())
}

struct LineWriter<'a> {
    inner: BufWriter<StdoutLock<'a>>,
    max_lines: u64,
    written_lines: u64,
}

impl<'a> LineWriter<'a> {
    fn new(lock: StdoutLock<'a>, max_lines: Option<u64>) -> LineWriter {
        LineWriter {
            inner: BufWriter::new(lock),
            max_lines: max_lines.unwrap_or(std::u64::MAX),
            written_lines: 0,
        }
    }

    fn write(&mut self, token: &[u8]) -> std::io::Result<()> {
        self.written_lines += 1;
        self.inner.write_all(&token)?;
        self.inner.write_all(&[b'\n'])?;
        Ok(())
    }

    fn done(&self) -> bool {
        self.written_lines >= self.max_lines
    }
}

impl<'a> Drop for LineWriter<'a> {
    fn drop(&mut self) {
        if let Err(e) = self.inner.flush() {
            eprintln!("Failed to flush lines: {}", e);
        }
    }
}
