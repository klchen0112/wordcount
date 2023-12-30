use csv::Writer;
use jieba_rs::Jieba;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use dashmap::DashMap;



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
    re: &Regex,
) -> Result<(DashMap<String, usize>, DashMap<(String, String), usize>), Box<dyn Error>> {
    // Initialize DashMaps
    let words_freq_inner = DashMap::new();
    let next_words_freq_inner = DashMap::new();
    let content = fs::read_to_string(file_path)?;

    let all_lines: Vec<&str> = content.lines().flat_map(|line| line.split('\n')).collect();

    all_lines.into_par_iter().for_each(|text| {
        let v: Value = serde_json::from_str(text).unwrap_or_default(); // 如果解析失败，使用默认值继续处理

        if let Some(text) = v.get("text").and_then(Value::as_str) {
            let lines: Vec<&str> = text.split('\n').collect();
            lines.into_par_iter().for_each(|line| {
                let tokens = jieba.cut(line, true);
                for token in &tokens {
                    if re.is_match(token) {
                        *words_freq_inner.entry(token.to_string()).or_insert(0) += 1;
                    }
                }

                for window in tokens.windows(2) {
                    let first_token = window[0];
                    let sec_token = window[1];
                    if re.is_match(first_token) && re.is_match(sec_token) {
                        *next_words_freq_inner
                            .entry((first_token.to_string(), sec_token.to_string()))
                            .or_insert(0) += 1;
                    }
                }
            });
        }
    });

    Ok((words_freq_inner, next_words_freq_inner))
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();
    let re = Regex::new(r#"[\u4e00-\u9fff]+"#)?;

    let paths: Vec<PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();

    // Replace Arc<Mutex<Vec<HashMap>>> with Arc<DashMap>
    let  all_word_freq = DashMap::new();
    let  all_next_word_freq = DashMap::new();


     for path in paths {
        if let Some(extension) = path.extension() {
            if extension == "jsonl" {
                if let Ok((word_freq, next_word_freq)) = process_single_jsonl_file(&path, &jieba, &re) {
                    word_freq.iter().for_each(|pair| {
                        let (word, freq) = pair.pair();
                        all_word_freq.entry(word.clone()).and_modify(|entry| *entry += *freq).or_insert(*freq);
                    });

                    next_word_freq.iter().for_each(|pair| {
                        let ((word1, word2), freq) = pair.pair();
                        all_next_word_freq
                            .entry((word1.clone(), word2.clone()))
                            .and_modify(|entry| *entry += *freq)
                            .or_insert(*freq);
                    });
                } else {
                    eprintln!("Error processing file {:?}", path);
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