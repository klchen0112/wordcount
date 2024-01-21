use csv::WriterBuilder;
use dashmap::{DashMap, DashSet};
use jieba_rs::Jieba;
use rayon::prelude::*;
use rusqlite::{params, Connection};
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

fn write_i64_i64_map_to_sqlite(
    conn: &mut Connection,
    map: &DashMap<String, i64>,
    commit_interval: usize, // 添加参数，表示适时提交的间隔
) -> Result<(), Box<dyn Error>> {
    let mut transaction = conn.transaction()?;
    let mut counter = 0; // 初始化计数器
    for pair in map.iter() {
        let (word1, frequency) = pair.pair();
        // 使用事务中的 SQLite 语句插入数据
        transaction.execute(
            "INSERT OR REPLACE INTO word_freq (word, frequency) VALUES (?, COALESCE((SELECT frequency FROM word_freq WHERE word = ?), 0) + ?)",
            [word1,word1,  &frequency.to_string()],
        )?;

        counter += 1;

        if counter % commit_interval == 0 {
            // 当计数器达到设定的提交间隔时，提交事务并重置计数器
            transaction.commit()?;
            transaction = conn.transaction()?;
        }
    }
    transaction.commit()?;
    Ok(())
}

fn write_tuple_i64_i64_map_to_sqlite(
    conn: &mut Connection,
    map: &DashMap<(String, String), i64>,
    commit_interval: usize, // 添加参数，表示适时提交的间隔
) -> Result<(), Box<dyn Error>> {
    let mut transaction = conn.transaction()?;
    let mut counter = 0; // 初始化计数器
    for pair in map.iter() {
        let ((word1, word2), frequency) = pair.pair();
        // 使用事务中的 SQLite 语句插入数据
        transaction.execute(
            "INSERT OR REPLACE INTO next_word_freq (word1, word2, frequency) VALUES (?, ?, COALESCE((SELECT frequency FROM next_word_freq WHERE word1 = ? AND word2 = ?), 0) + ?)",
            [word1, word2,word1, word2,  &frequency.to_string()],
        )?;

        counter += 1;

        if counter % commit_interval == 0 {
            // 当计数器达到设定的提交间隔时，提交事务并重置计数器
            transaction.commit()?;
            transaction = conn.transaction()?;
        }
    }
    transaction.commit()?;

    Ok(())
}

fn process_line(
    line: &str,
    word_freq: &DashMap<String, i64>,
    next_word_freq: &DashMap<(String, String), i64>,
    jieba: &Jieba,
) {
    let text_lines: Vec<&str> = line.split('\n').collect();
    for t_line in text_lines {
        let tokenized_words = jieba.cut(t_line, true);
        let mut prev_word: &str = "";
        for token in tokenized_words {
            *word_freq.entry(token.to_string()).or_insert(0) += 1;

            if !prev_word.is_empty() {
                *next_word_freq
                    .entry((prev_word.to_string(), token.to_string()))
                    .or_insert(0) += 1;
            }
            prev_word = token;
        }
    }
}

fn process_line_from_json(
    json_text: &str,
    word_freq: &DashMap<String, i64>,
    next_word_freq: &DashMap<(String, String), i64>,
    jieba: &Jieba,
) {
    // 解析JSON，获取text字段
    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(json_text) {
        if let Some(text_value) = parsed_json.get("text") {
            if let Some(text) = text_value.as_str() {
                // 进行分词
                let tokenized_words = jieba.cut(text, true);

                let mut prev_word: &str = "";
                for token in tokenized_words {
                    *word_freq.entry(token.to_string()).or_insert(0) += 1;

                    if !prev_word.is_empty() {
                        *next_word_freq
                            .entry((prev_word.to_string(), token.to_string()))
                            .or_insert(0) += 1;
                    }
                    prev_word = token;
                }
            }
        }
    }
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();

    // SQLite Connection
    let mut conn = Connection::open("results/data.db")?;

    // Create tables if not exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS word_freq (word TEXT PRIMARY KEY, frequency INTEGER)",
        params![],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS next_word_freq (word1 TEXT, word2 TEXT, frequency INTEGER, PRIMARY KEY (word1, word2), FOREIGN KEY (word1) REFERENCES word_freq(word) ON DELETE CASCADE, FOREIGN KEY (word2) REFERENCES word_freq(word) ON DELETE CASCADE)",
        params![],
    )?;

    let processed_files: DashSet<String> = DashSet::new(); // Store processed filenames
    if let Ok(visit_file) = File::open("results/visit.txt") {
        let visit_reader = BufReader::new(visit_file);
        visit_reader
            .lines()
            .map_while(Result::ok)
            .for_each(|filename| {
                processed_files.insert(filename);
            });
    }
    let paths: Vec<PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .filter(|entry| {
            if let Some(extension) = entry.path().extension() {
                extension == "jsonl" || extension == "txt"
            } else {
                false
            }
        })
        .map(|entry| entry.path())
        .collect();
    let visit_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("results/visit.txt")
        .expect("Failed to open or create visit.txt");

    // Wrap the file in a BufWriter for buffering
    let mut visit_file = BufWriter::new(visit_file);

    for path in paths {
        let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
        if processed_files.contains(&file_name) {
            continue;
        }
        if let Ok(file) = fs::File::open(&path) {
            let reader = BufReader::new(file);
            let word_freq: DashMap<String, i64> = DashMap::with_capacity(65536);
            let next_word_freq: DashMap<(String, String), i64> = DashMap::with_capacity(65536);
            reader
                .lines()
                .par_bridge()
                .for_each(|line| match path.extension() {
                    Some(ext) if ext == "jsonl" => process_line_from_json(
                        line.expect("REASON").trim(),
                        &word_freq,
                        &next_word_freq,
                        &jieba,
                    ),
                    Some(ext) if ext == "txt" => process_line(
                        line.expect("REASON").trim(),
                        &word_freq,
                        &next_word_freq,
                        &jieba,
                    ),
                    _ => (),
                });
            write_i64_i64_map_to_sqlite(&mut conn, &word_freq, 2000)?;
            write_tuple_i64_i64_map_to_sqlite(&mut conn, &next_word_freq, 1000)?;

            println!("{} complete", file_name);

            // Mark the file as processed
            processed_files.insert(file_name.clone());

            // Write the processed filename into visit.txt

            writeln!(visit_file, "{}", file_name).expect("Failed to write to visit.txt");
        }
    }

    // 在处理 JSONL 文件后，将以下代码添加到导出表到 CSV 文件的位置
    if let Err(e) = write_table_to_csv(&conn, "word_freq.csv") {
        eprintln!("Error writing word_freq to CSV: {}", e);
    }

    if let Err(e) = write_next_word_freq_to_csv(&conn, "next_word_freq.csv") {
        eprintln!("Error writing next_word_freq to CSV: {}", e);
    }
    Ok(())
}

fn write_table_to_csv(conn: &Connection, file_path: &str) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = WriterBuilder::new().delimiter(b'\t').from_writer(file);

    // Query table data
    let mut stmt = conn.prepare("SELECT * FROM word_freq ORDER BY frequency DESC")?;
    let rows = stmt.query_map(params![], |row| {
        // Adjust column indices and types based on your table structure
        Ok((row.get::<usize, String>(0)?, row.get::<usize, i64>(1)?))
    })?;

    for row in rows {
        let (column1, column2) = row?;
        writer.write_record(&[column1, column2.to_string()])?;
    }

    writer.flush()?;
    Ok(())
}

fn write_next_word_freq_to_csv(conn: &Connection, file_path: &str) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = WriterBuilder::new().delimiter(b'\t').from_writer(file);

    // Query next_word_freq table data
    let mut stmt = conn.prepare("SELECT * FROM next_word_freq ORDER BY frequency DESC")?;
    let rows = stmt.query_map(params![], |row| {
        // Adjust column indices and types based on your table structure
        Ok((
            row.get::<usize, String>(0)?,
            row.get::<usize, String>(1)?,
            row.get::<usize, i64>(2)?,
        ))
    })?;

    for row in rows {
        let (column1, column2, column3) = row?;
        writer.write_record(&[column1, column2, column3.to_string()])?;
    }

    writer.flush()?;
    Ok(())
}

fn main() {
    env::set_var("RAYON_NUM_THREADS", "16");
    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
