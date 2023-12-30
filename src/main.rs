use csv::Writer;
use jieba_rs::Jieba;
use rayon::prelude::*;
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::io::{BufRead, BufReader};
use dashmap::DashMap;

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

fn write_to_csv(file_name: &str, data: &DashMap<String, usize>) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(file_name)?;
    for pair in data.iter() {
        writer.write_record(&[&pair.key(), &pair.value().to_string()])?;
    }
    writer.flush()?;
    Ok(())
}

fn write_to_csv_two_columns(
    file_name: &str,
    data: &DashMap<(String, String), usize>,
) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(file_name)?;
    for pair in data.iter() {
        let (word1, word2) = pair.key();
        writer.write_record(&[&format!("{} {}", word1, word2), &pair.value().to_string()])?;
    }
    writer.flush()?;
    Ok(())
}

fn process_single_jsonl_file(
    file_path: &PathBuf,
    jieba: &Jieba,
    all_word_freq: &mut DashMap<String, usize>,
    all_next_word_freq: &mut DashMap<(String, String), usize>,
) -> Result<(), Box<dyn Error>> {


    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);

    let text_lines: Vec<_> = reader.lines().collect::<Result<_, _>>()?;
    text_lines.par_iter().into_par_iter().for_each(|text| {
        let v: Value = serde_json::from_str(text).unwrap_or_default(); // 如果解析失败，使用默认值继续处理

        if let Some(text) = v.get("text").and_then(Value::as_str) {
            let lines: Vec<&str> = text.split('\n').collect();
            lines.into_par_iter().for_each(|line| {
                let tokens = jieba.cut(line, true);
                for token in &tokens {
                    if contains_special_characters(token) {
                        continue;
                    }
                    *all_word_freq.entry(token.to_string()).or_insert(0) += 1;
                }

                for window in tokens.windows(2) {
                    let first_token = window[0];
                    let sec_token = window[1];
                    if contains_special_characters(first_token) || contains_special_characters(sec_token) {
                        continue;
                    }
                    *all_next_word_freq
                            .entry((first_token.to_string(), sec_token.to_string()))
                            .or_insert(0) += 1;
                }
            });
        }
    });

    Ok(())
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();

    let paths: Vec<PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();

    let  mut all_word_freq = DashMap::new();
    let  mut all_next_word_freq = DashMap::new();


     for path in paths {
        if let Some(extension) = path.extension() {
            if extension == "jsonl" {
                if let Err(e) = process_single_jsonl_file(
                    &path,
                    &jieba,
                    &mut all_word_freq, // Pass as mutable reference
                    &mut all_next_word_freq, // Pass as mutable reference
                ) {
                    eprintln!("Error processing file {:?}: {}", path, e);
                } else {
                    println!("Processing file {:?} success", path);
                }
            }
        }
    }

    write_to_csv("word_freq.csv", &all_word_freq)?;
    write_to_csv_two_columns("next_word_freq.csv", &all_next_word_freq)?;

    Ok(())
}

fn main() {
    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
