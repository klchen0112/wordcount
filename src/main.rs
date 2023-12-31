use jieba_rs::Jieba;
use rusqlite::{Connection, Result as SqliteResult};
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    }
    false
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();

    let paths: Vec<PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();

    let conn = Connection::open("word_freq.db")?;
    create_tables(&conn)?;
    let conn_mutex = Arc::new(Mutex::new(conn));

    for path in paths {
        if let Some(extension) = path.extension() {
            if extension == "jsonl" {
                let file = fs::File::open(&path)?;
                let reader = BufReader::new(file);

                for line in reader.lines() {
                    if let Ok(line) = line {
                        let json_data: Value = serde_json::from_str(&line)?;

                        if let Some(text) = json_data.get("text").and_then(Value::as_str) {
                            let words = jieba.cut(text, false);
                            let conn = conn_mutex.lock().unwrap();

                            for word in words {
                                if !contains_special_characters(word) {
                                    // 插入或更新单词频率
                                    conn.execute(
                                        "INSERT INTO word_frequency (word, frequency) VALUES (?1, 1)
                                        ON CONFLICT(word) DO UPDATE SET frequency = frequency + 1",
                                        &[&word],
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn main() {
    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
