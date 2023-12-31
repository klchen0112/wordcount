use crossbeam_channel::{bounded, Receiver, Sender};
use jieba_rs::Jieba;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rusqlite::{Connection, Result as SqliteResult};
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::thread;

fn create_tables(conn: &Connection) -> SqliteResult<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS word_frequency (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT UNIQUE, -- 设置 word 列为唯一
            frequency INTEGER
        )",
        [],
    )?;

    // 添加对 word 字段的索引
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_word_frequency_word ON word_frequency (word)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS next_word_freq (
            id1 INTEGER,
            id2 INTEGER,
            frequency INTEGER,
            PRIMARY KEY (id1, id2),
            FOREIGN KEY (id1) REFERENCES word_frequency(id),
            FOREIGN KEY (id2) REFERENCES word_frequency(id)
        )",
        [],
    )?;
    Ok(())
}

fn contains_special_characters(s: &str) -> bool {
    for c in s.chars() {
        if c.is_ascii_punctuation() || c.is_ascii_whitespace() || c.is_control() {
            return true;
        }
        // 中文标点范围：\u{3000}-\u{303F}
        if ('\u{3000}'..='\u{303F}').contains(&c) {
            return true;
        }
        // 中文书名号范围：\u{3008}-\u{3011}
        if ('\u{3008}'..='\u{3011}').contains(&c) {
            return true;
        }
        // 全角ASCII范围：\u{FF00}-\u{FFEF}
        if ('\u{FF00}'..='\u{FFEF}').contains(&c) {
            return true;
        }
        if c == '"' || c == '“' || c == '”' {
            return true;
        }
    }
    false
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();

    let paths: Vec<PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();

    let (sender, receiver): (Sender<Vec<String>>, Receiver<Vec<String>>) = bounded(64);

    let db_thread_handle = thread::spawn(move || {
        #[warn(unused_assignments)]
        let mut iteration_counter = 0;
        const PRINT_FREQUENCY: usize = 1000; // Adjust this value as needed

        if let Ok(conn) = Connection::open("word_freq.db") {
            if let Err(err) = create_tables(&conn) {
                eprintln!("Error creating tables: {}", err);
                return;
            }
            if let Ok(mut stmt_word) = conn.prepare_cached("INSERT INTO word_frequency (word, frequency) VALUES (?1, 1) ON CONFLICT(word) DO UPDATE SET frequency = frequency + 1") {
            if let Ok(mut stmt_next_word) = conn.prepare_cached("INSERT INTO next_word_freq (id1, id2, frequency) VALUES (?1, ?2, 1) ON CONFLICT(id1, id2) DO UPDATE SET frequency = frequency + 1") {
                while let Ok(words) = receiver.recv() {
                    let mut prev_word_id: Option<i64> = None;

                    for word in words.iter().map(|s| s.as_str()) {
                        if !contains_special_characters(word) {
                            if let Ok(word_id) = conn.query_row(
                                "SELECT id FROM word_frequency WHERE word = ?1",
                                [&word],
                                |row| row.get(0),
                            ) {
                                // Insert into word_frequency table
                                if let Err(e) = stmt_word.execute([&word]) {
                                    eprintln!("Error executing query: {}", e);
                                }

                                // Insert into next_word_freq table
                                if let Some(prev_id) = prev_word_id {
                                    if let Err(e) = stmt_next_word.execute([&prev_id, &word_id]) {
                                        eprintln!("Error executing query: {}", e);
                                    }
                                }
                                prev_word_id = Some(word_id);
                            }
                        }
                    }
                }
            } else {
                eprintln!("Error preparing next_word_freq statement");
            }
        } else {
            eprintln!("Error preparing word_frequency statement");
        }
        } else {
            eprintln!("Error opening connection");
        }

        iteration_counter += 1;

        if iteration_counter == PRINT_FREQUENCY {
            println!("Processed {} iterations", iteration_counter);
            iteration_counter = 0;
        }
    });

    let pool = ThreadPoolBuilder::new().num_threads(32).build().unwrap();

    for path in paths {
        if let Ok(file) = fs::File::open(&path) {
            let reader = BufReader::new(file);
            pool.install(|| {
                reader.lines().par_bridge().for_each(|line| {
                    if let Ok(line) = line {
                        let json_data: Value = serde_json::from_str(&line).unwrap_or_default();
                        if let Some(text) = json_data.get("text").and_then(Value::as_str) {
                            let mut words: Vec<String> = Vec::new();
                            let tokenized_words = jieba.cut(text, true);

                            for word in &tokenized_words {
                                words.push(word.to_string());
                            }
                            if let Err(err) = sender.send(words.clone()) {
                                eprintln!("Error sending words: {}", err);
                            }
                        }
                    }
                });
            });
        }
    }

    drop(sender); // Close the sender to signal completion
    db_thread_handle.join().unwrap();

    Ok(())
}

fn main() {
    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
