use dashmap::DashMap;
use jieba_rs::Jieba;
use rayon::prelude::*;
use serde_json::Value;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

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

fn write_i64_i64_map_to_csv(
    file_name: &str,
    map: &DashMap<i64, i64>,
) -> Result<(), Box<dyn Error>> {
    let cloned_map = map.clone(); // Clone the map to move it into the thread

    let file_name = Arc::new(Mutex::new(file_name.to_string())); // Create an Arc<Mutex<_>> to share file_name

    let file_name_clone = Arc::clone(&file_name); // Clone for the thread

    thread::spawn(move || {
        let file_name = file_name_clone.lock().unwrap(); // Lock to access the file_name
        let file = File::create(&*file_name).expect("Failed to create file");
        let mut writer = BufWriter::new(file);

        for pair in cloned_map.iter() {
            let (key, value) = pair.pair();
            writeln!(writer, "{},{}", key, value).expect("Failed to write to file");
        }

        writer.flush().expect("Failed to flush buffer");
    })
    .join()
    .expect("Thread panicked");

    Ok(())
}

fn write_tuple_i64_i64_map_to_csv(
    file_name: &str,
    map: &DashMap<(i64, i64), i64>,
) -> Result<(), Box<dyn Error>> {
    let cloned_map = map.clone(); // Clone the map to move it into the thread

    let file_name = Arc::new(Mutex::new(file_name.to_string())); // Create an Arc<Mutex<_>> to share file_name

    let file_name_clone = Arc::clone(&file_name); // Clone for the thread

    thread::spawn(move || {
        let file_name = file_name_clone.lock().unwrap(); // Lock to access the file_name
        let file = File::create(&*file_name).expect("Failed to create file");
        let mut writer = BufWriter::new(file);

        for pair in cloned_map.iter() {
            let ((key1, key2), value) = pair.pair();
            writeln!(writer, "{},{},{}", key1, key2, value).expect("Failed to write to file");
        }

        writer.flush().expect("Failed to flush buffer");
    })
    .join()
    .expect("Thread panicked");

    Ok(())
}

fn write_string_i64_map_to_csv(
    file_name: &str,
    map: &DashMap<String, i64>,
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_name)?;
    let mut writer = BufWriter::new(file);

    for pair in map.iter() {
        let (key, value) = pair.pair();
        writeln!(writer, "{},{}", key, value)?;
    }

    writer.flush()?;
    Ok(())
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

    fs::create_dir_all("results/word_freq")?;
    fs::create_dir_all("results/next_word_freq")?;
    let word_to_id: DashMap<String, i64> = DashMap::new();
    let id_to_word: DashMap<i64, String> = DashMap::new();
    let id_count = Arc::new(Mutex::<i64>::new(0));
    for path in paths {
        let file_name = path.file_name().unwrap().to_string_lossy().into_owned();

        if let Ok(file) = fs::File::open(&path) {
            let reader = BufReader::new(file);
            let lines: Vec<_> = reader.lines().map_while(Result::ok).collect();
            let word_freq: DashMap<i64, i64> = DashMap::with_capacity(65536);
            let next_word_freq: DashMap<(i64, i64), i64> = DashMap::with_capacity(65536);
            lines.par_iter().for_each(|line| {
                let json_data: Value = serde_json::from_str(line).unwrap_or_default();
                if let Some(text) = json_data.get("text").and_then(Value::as_str) {
                    let text_lines: Vec<&str> = text.split('\n').collect();
                    for t_line in text_lines {
                        let tokenized_words = jieba.cut(t_line, false);
                        let mut word_id: i64 = -1;
                        let mut prev_word_id: i64 = -1;
                        for token in tokenized_words {
                            if contains_special_characters(token) {
                                continue;
                            }

                            if let Some(existing_id) = word_to_id.get(token) {
                                word_id = *existing_id;
                            } else if let Ok(mut id_count_lock) = id_count.lock() {
                                if !word_to_id.contains_key(token) {
                                    id_to_word.insert(*id_count_lock, token.to_string());
                                    word_to_id.insert(token.to_string(), *id_count_lock);
                                    word_id = *id_count_lock;
                                    *id_count_lock += 1;
                                } else {
                                    word_id = *word_to_id.get(token).unwrap();
                                }
                            }

                            *word_freq.entry(word_id).or_insert(0) += 1;

                            *word_freq.entry(word_id).or_insert(0) += 1;
                            if prev_word_id >= 0 {
                                *next_word_freq.entry((prev_word_id, word_id)).or_insert(0) += 1;
                            }
                            prev_word_id = word_id;
                        }
                    }
                }
            });
            rayon::join(|| {}, || {});

            let file_name_word_freq = format!("results/word_freq/{}.csv", file_name);
            let file_name_next_word_freq = format!("results/next_word_freq/{}.csv", file_name);

            write_i64_i64_map_to_csv(&file_name_word_freq, &word_freq)?;
            write_tuple_i64_i64_map_to_csv(&file_name_next_word_freq, &next_word_freq)?;

            println!("{}", file_name);
        }
    }
    let file_name_word_to_id = "results/word_to_id.csv";
    write_string_i64_map_to_csv(file_name_word_to_id, &word_to_id)?;
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
