use dashmap::DashMap;
use jieba_rs::Jieba;
use rayon::prelude::*;
use rusqlite::{params, Connection};
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

fn create_results_directories() -> Result<(), Box<dyn Error>> {
    fs::create_dir_all("results/word_freq")?;
    fs::create_dir_all("results/next_word_freq")?;
    Ok(())
}

fn write_i64_i64_map_to_csv(
    file_path: &str,
    map: &DashMap<String, i64>,
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = csv::Writer::from_writer(file);

    for pair in map.iter() {
        let (word, frequency) = pair.pair();
        writer.write_record(&[word.clone(), frequency.to_string()])?;
    }

    writer.flush()?;
    Ok(())
}

fn write_tuple_i64_i64_map_to_csv(
    file_path: &str,
    map: &DashMap<(String, String), i64>,
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = csv::Writer::from_writer(file);

    for pair in map.iter() {
        let ((word1, word2), frequency) = pair.pair();
        writer.write_record(&[word1.clone(), word2.clone(), frequency.to_string()])?;
    }

    writer.flush()?;
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
    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(json_text) {
        if let Some(text_value) = parsed_json.get("text") {
            if let Some(text) = text_value.as_str() {
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
    create_results_directories()?; // 创建目录

    let jieba = Jieba::new();

    let word_freq: DashMap<String, i64> = DashMap::with_capacity(65536);
    let next_word_freq: DashMap<(String, String), i64> = DashMap::with_capacity(65536);

    let mut processed_files: Vec<String> = Vec::new();

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

    for path in paths {
        let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
        if processed_files.contains(&file_name) {
            continue;
        }

        if let Ok(file) = fs::File::open(&path) {
            let reader = BufReader::new(file);

            reader
                .lines()
                .par_bridge()
                .for_each(|line| match path.extension() {
                    Some(ext) if ext == "jsonl" => process_line_from_json(
                        line.expect("Failed to read line").trim(),
                        &word_freq,
                        &next_word_freq,
                        &jieba,
                    ),
                    Some(ext) if ext == "txt" => process_line(
                        line.expect("Failed to read line").trim(),
                        &word_freq,
                        &next_word_freq,
                        &jieba,
                    ),
                    _ => (),
                });

            // 输出结果到CSV文件
            let word_freq_csv_path = "results/word_freq/word_freq.csv".to_string();
            let next_word_freq_csv_path = "results/next_word_freq/next_word_freq.csv".to_string();

            if let Err(e) = write_i64_i64_map_to_csv(&word_freq_csv_path, &word_freq) {
                eprintln!("Error writing word_freq to CSV: {}", e);
            }

            if let Err(e) =
                write_tuple_i64_i64_map_to_csv(&next_word_freq_csv_path, &next_word_freq)
            {
                eprintln!("Error writing next_word_freq to CSV: {}", e);
            }

            println!("{} complete", file_name);
            processed_files.push(file_name.clone());
        }
    }

    Ok(())
}

fn insert_word_freq_csv_to_sqlite(
    conn: &mut Connection,
    csv_file: &str,
    word_id_map: &DashMap<String, i64>,
) -> Result<(), Box<dyn Error>> {
    let table_name = "word_freq";
    let mut reader = csv::Reader::from_path(csv_file)?;

    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, word TEXT UNIQUE, frequency INTEGER)",
            table_name
        ),
        params![],
    )?;

    let transaction = conn.transaction()?;
    for result in reader.records() {
        let record = result?;
        let word = record.get(0).ok_or("Missing word column")?;
        let frequency: i64 = record.get(1).ok_or("Missing frequency column")?.parse()?;
        let word_id = *word_id_map.entry(word.to_string()).or_insert_with(|| {
            // 直接使用word_id_map的自增值作为id
            word_id_map.len() as i64 + 1
        });

        transaction.execute(
            &format!(
                "INSERT OR REPLACE INTO {} (id, word, frequency) VALUES (?, ?, ?)",
                table_name
            ),
            params![word_id, word, frequency],
        )?;
    }
    transaction.commit()?;
    println!("Inserted data from {} into SQLite", table_name);

    Ok(())
}

fn insert_next_word_freq_csv_to_sqlite(
    conn: &mut Connection,
    csv_file: &str,
    word_id_map: &DashMap<String, i64>,
) -> Result<(), Box<dyn Error>> {
    let table_name = "next_word_freq";
    let mut reader = csv::Reader::from_path(csv_file)?;

    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, word1_id INTEGER, word2_id INTEGER, frequency INTEGER)",
            table_name
        ),
        params![],
    )?;

    let transaction = conn.transaction()?;
    for result in reader.records() {
        let record = result?;
        let word1 = record.get(0).ok_or("Missing word1 column")?;
        let word2 = record.get(1).ok_or("Missing word2 column")?;
        let frequency: i64 = record.get(2).ok_or("Missing frequency column")?.parse()?;
        let word1_id = *word_id_map.get(word1).unwrap(); // 使用映射获取word1的id
        let word2_id = *word_id_map.get(word2).unwrap(); // 使用映射获取word2的id

        transaction.execute(
            &format!(
                "INSERT OR REPLACE INTO {} (word1_id, word2_id, frequency) VALUES (?, ?, ?)",
                table_name
            ),
            params![word1_id, word2_id, frequency],
        )?;
    }
    transaction.commit()?;
    println!("Inserted data from {} into SQLite", table_name);

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RAYON_NUM_THREADS", "16");

    let mut conn = Connection::open_in_memory()?;

    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
        Err(e)
    } else {
        println!("Processing of JSONL files complete.");

        let word_id_map: DashMap<String, i64> = DashMap::with_capacity(65536);

        if let Err(e) = insert_word_freq_csv_to_sqlite(
            &mut conn,
            "results/word_freq/word_freq.csv",
            &word_id_map,
        ) {
            eprintln!("Error inserting word_freq CSV to SQLite: {}", e);
            return Err(e);
        }

        if let Err(e) = insert_next_word_freq_csv_to_sqlite(
            &mut conn,
            "results/next_word_freq/next_word_freq.csv",
            &word_id_map,
        ) {
            eprintln!("Error inserting next_word_freq CSV to SQLite: {}", e);
            return Err(e);
        }

        conn.backup(rusqlite::DatabaseName::Main, "results/data.db", None)
            .expect("Error during database backup");

        Ok(())
    }
}
