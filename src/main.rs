use crossbeam_channel::{bounded, Receiver, Sender};
use jieba_rs::Jieba;
use rayon::prelude::*;
use rusqlite::params;
use rusqlite::{Connection, Result as SqliteResult};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::thread;

fn create_tables(conn: &Connection) -> SqliteResult<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS word_frequency (
            id INTEGER PRIMARY KEY,
            word TEXT UNIQUE, -- 设置 word 列为唯一
            frequency INTEGER
        )",
        [],
    )?;

    // // 添加对 word 字段的索引
    // conn.execute(
    //     "CREATE INDEX IF NOT EXISTS idx_word_frequency_word ON word_frequency (word)",
    //     [],
    // )?;

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
        .filter(|entry| {
            if let Some(extension) = entry.path().extension() {
                extension == "jsonl"
            } else {
                false
            }
        })
        .map(|entry| entry.path())
        .collect();

    let (sender, receiver): (Sender<Vec<String>>, Receiver<Vec<String>>) = bounded(10000);

    let db_thread_handle = thread::spawn(move || {
        const CACHE_LIMIT: usize = 10000; // Adjust this value as needed
        let mut word_to_id = HashMap::<String, i64>::new();
        let mut id_to_word = HashMap::<i64, String>::new();
        let mut word_freq = HashMap::<i64, i64>::new();
        let mut next_word_freq = HashMap::<(i64, i64), i64>::new();
        if let Ok(conn) = Connection::open("word_freq.db") {
            if let Err(err) = create_tables(&conn) {
                eprintln!("Error creating tables: {}", err);
                return;
            }

            while let Ok(words) = receiver.recv() {
                let mut prev_word_id: i64 = -1;
                for word in words.iter().map(|s| s.as_str()) {
                    if !contains_special_characters(word) {
                        // Insert into word_frequency table
                        let key = word.to_string();
                        if !word_to_id.contains_key(&key) {
                            id_to_word.insert(word_to_id.len() as i64, key.clone());
                            word_to_id.insert(key.clone(), word_to_id.len() as i64);
                        }
                        if let Some(word_id) = word_to_id.get(&key) {
                            *word_freq.entry(*word_id).or_insert(0) += 1;

                            // Insert into next_word_freq table
                            if prev_word_id != -1 {
                                *next_word_freq.entry((prev_word_id, *word_id)).or_insert(0) += 1;
                            }
                            prev_word_id = *word_id;
                        }
                    }
                }
                if word_freq.len() >= CACHE_LIMIT {
                    if let Ok(mut stmt_word) = conn.prepare_cached("INSERT INTO word_frequency (id, word, frequency) VALUES (?1, ?2, ?3) ON CONFLICT(word) DO UPDATE SET frequency = frequency + ?3") {
                        conn.execute_batch("BEGIN").unwrap();

                        for (word_id, freq) in &word_freq {
                            if let Some(word) = id_to_word.get(word_id) {
                                stmt_word.execute(params![word_id, word, freq]).expect("Failed to execute statement for word_frequency");
                            }
                        }

                        conn.execute_batch("COMMIT").unwrap();
                        word_freq.clear();
                    }

                    if let Ok(mut stmt_next_word) = conn.prepare_cached("INSERT INTO next_word_freq (id1, id2, frequency) VALUES (?1, ?2, ?3) ON CONFLICT(id1, id2) DO UPDATE SET frequency = frequency + ?3") {
                        conn.execute_batch("BEGIN").unwrap();
                        for ((id1, id2), freq) in &next_word_freq {
                            stmt_next_word.execute(params![id1, id2, freq]).expect("Failed to execute statement for next_word_freq");
                        }

                        conn.execute_batch("COMMIT").unwrap();
                        next_word_freq.clear();
                    }
                }
            }

            if let Ok(mut stmt_word) = conn.prepare_cached("INSERT INTO word_frequency (id, word, frequency) VALUES (?1, ?2, ?3) ON CONFLICT(word) DO UPDATE SET frequency = frequency + ?3") {
                conn.execute_batch("BEGIN").unwrap();

                for (word_id, freq) in &word_freq {
                    if let Some(word) = id_to_word.get(word_id) {
                        stmt_word.execute(params![word_id, word, freq]).expect("Failed to execute statement for word_frequency");
                    }
                }

                conn.execute_batch("COMMIT").unwrap();
                word_freq.clear();
            }

            if let Ok(mut stmt_next_word) = conn.prepare_cached("INSERT INTO next_word_freq (id1, id2, frequency) VALUES (?1, ?2, ?3) ON CONFLICT(id1, id2) DO UPDATE SET frequency = frequency + ?3") {
                conn.execute_batch("BEGIN").unwrap();

                for ((id1, id2), freq) in &next_word_freq {
                    stmt_next_word.execute(params![id1, id2, freq]).expect("Failed to execute statement for next_word_freq");
                }

                conn.execute_batch("COMMIT").unwrap();
                next_word_freq.clear();
            }
        }
    });

    for path in paths {
        let file_name = path.file_name().unwrap().to_string_lossy().into_owned();

        if let Ok(file) = fs::File::open(&path) {
            let reader = BufReader::new(file);
            let lines: Vec<_> = reader.lines().filter_map(Result::ok).collect();

            lines.par_iter().for_each(|line| {
                let json_data: Value = serde_json::from_str(&line).unwrap_or_default();
                if let Some(text) = json_data.get("text").and_then(Value::as_str) {
                    let text_lines: Vec<&str> = text.split('\n').collect();
                    for t_line in text_lines {
                        let mut words: Vec<String> = Vec::new();
                        let tokenized_words = jieba.cut(t_line, true);

                        for word in &tokenized_words {
                            words.push(word.to_string());
                        }

                        let _ = sender.send(words.clone());
                    }
                }
            });
            rayon::join(|| {}, || {});

            // Mark file as processed

            println!("{}", file_name);
        }
    }

    drop(sender); // Close the sender to signal completion
    let _ = db_thread_handle.join();

    Ok(())
}

fn main() {
    env::set_var("RAYON_NUM_THREADS", "20");
    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
