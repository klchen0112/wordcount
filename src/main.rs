use csv::Writer;
use jieba_rs::Jieba;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::str;
use std::sync::{Arc, Mutex};

fn write_to_csv(file_name: &str, data: &HashMap<String, usize>) -> Result<(), Box<dyn Error>> {
        let mut writer = Writer::from_path(file_name)?;

    for (word, freq) in data {
        writer.write_record(&[word, &freq.to_string()])?;
    }

    writer.flush()?;
    Ok(())

}

fn write_to_csv_two_columns(
    file_name: &str,
    data: &HashMap<(String, String), usize>,
) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(file_name)?;

    for ((word1, word2), freq) in data {
        writer.write_record(&[&format!("{} {}", word1, word2), &freq.to_string()])?;
    }

    writer.flush()?;
    Ok(())
}

fn process_single_jsonl_file(
    file_path: &std::path::PathBuf,
    word_freq: &Arc<Mutex<HashMap<String, usize>>>,
    next_word_freq: &Arc<Mutex<HashMap<(String, String), usize>>>,
    jieba: &Jieba,
    re: &Regex,
) -> Result<(), Box<dyn Error>> {
    let content = fs::read_to_string(file_path)?;

    for line in content.lines() {
        let v: Value = serde_json::from_str(line)?;

        if let Some(text) = v.get("text").and_then(Value::as_str) {
            let lines: Vec<&str> = text.split('\n').collect();
            lines.par_iter().for_each(|line| {
                let tokens = jieba.cut(line, true);

                let mut word_freq_inner = word_freq.lock().unwrap();
                let mut next_word_freq_inner = next_word_freq.lock().unwrap();

                for token in &tokens {
                    if re.is_match(token) {
                        *word_freq_inner.entry(token.to_string()).or_insert(0) += 1;
                    }
                }

                for window in tokens.windows(2) {
                    let first_token = window[0];
                    if !re.is_match(first_token) {
                        continue;
                    }
                    let sec_token = window[1];
                    if !re.is_match(sec_token) {
                        continue;
                    }
                    *next_word_freq_inner
                        .entry((first_token.to_string(), sec_token.to_string()))
                        .or_insert(0) += 1;
                }
            });
        }
    }

    Ok(())
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let word_freq: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let next_word_freq: Arc<Mutex<HashMap<(String, String), usize>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let jieba = Jieba::new();
    let re = Regex::new(r#"[\u4e00-\u9fff]+"#).unwrap();

    let paths: Vec<std::path::PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();

    paths.par_iter().for_each(|path| {
        if let Some(extension) = path.extension() {
            if extension == "jsonl" {
                if let Err(err) =
                    process_single_jsonl_file(&path, &word_freq, &next_word_freq, &jieba, &re)
                {
                    eprintln!("Error processing file {:?}: {}", path, err);
                }
            }
        }
    });

    write_to_csv("word_freq.csv", &word_freq.lock().unwrap())?;
    write_to_csv_two_columns("next_word_freq.csv", &next_word_freq.lock().unwrap())?;

    Ok(())
}

fn main() {
    if let Err(e) = process_jsonl_files("/Users/klchen/myOpenSource/word_count_rust/data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
