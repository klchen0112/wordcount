use dashmap::{DashMap, DashSet};
use jieba_rs::Jieba;
use rayon::prelude::*;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

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
    map: &DashMap<String, i64>,
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_name).expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    for pair in map.iter() {
        let (key, value) = pair.pair();
        writeln!(writer, "{},{}", key, value).expect("Failed to write to file");
    }

    writer.flush().expect("Failed to flush buffer");

    Ok(())
}

fn write_tuple_i64_i64_map_to_csv(
    file_name: &str,
    map: &DashMap<(String, String), i64>,
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_name).expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    for pair in map.iter() {
        let ((key1, key2), value) = pair.pair();
        writeln!(writer, "{},{},{}", key1, key2, value).expect("Failed to write to file");
    }

    writer.flush().expect("Failed to flush buffer");

    Ok(())
}

fn process_line(
    line: Result<String, std::io::Error>,
    word_freq: &DashMap<String, i64>,
    next_word_freq: &DashMap<(String, String), i64>,
    jieba: &Jieba,
) {
    if let Ok(text) = line {
        let text_lines: Vec<&str> = text.split('\n').collect();
        for t_line in text_lines {
            let tokenized_words = jieba.cut(t_line, true);
            let mut prev_word: &str = "";
            for token in tokenized_words {
                if contains_special_characters(token) {
                    prev_word = "";
                    continue;
                }
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

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();
    fs::create_dir_all("results/word_freq")?;
    fs::create_dir_all("results/next_word_freq")?;
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
                extension == "jsonl"
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
            let word_freq: DashMap<String, i64> = DashMap::with_capacity(65536);
            let next_word_freq: DashMap<(String, String), i64> = DashMap::with_capacity(65536);
            reader
                .lines()
                .par_bridge()
                .for_each(|line| process_line(line, &word_freq, &next_word_freq, &jieba));
            rayon::join(|| {}, || {});

            let file_name_word_freq = format!("results/word_freq/{}.csv", file_name);
            let file_name_next_word_freq = format!("results/next_word_freq/{}.csv", file_name);

            write_i64_i64_map_to_csv(&file_name_word_freq, &word_freq).unwrap();

            write_tuple_i64_i64_map_to_csv(&file_name_next_word_freq, &next_word_freq).unwrap();

            println!("{}", file_name);

            // Mark the file as processed
            processed_files.insert(file_name.clone());

            // Write the processed filename into visit.txt
            if let Ok(mut visit_file) = File::create("results/visit.txt").map(BufWriter::new) {
                writeln!(visit_file, "{}", file_name).expect("Failed to write to visit.txt");
            }
        }
    }

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
