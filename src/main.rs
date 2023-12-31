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


fn write_to_csv_with_words(
    file_name: &str,
    data: &DashMap<usize, usize>,
    id_word: &DashMap<usize,String>,
) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(file_name)?;
    writer.write_record(&["Word", "Frequency"])?;

    for pair in data.iter() {
        if let Some(word) = id_word.get(&pair.key()) {
            let word_str: String = word.clone(); // 转换为 String 类型
            writer.write_record(&[&word_str, &pair.value().to_string()])?;
        } else {
            // Handle the case when word is not found in id_word
            // This could be logging an error or taking another action as necessary
            println!("Word not found in id_word for key: {:?}", pair.key());
        }
    }
    writer.flush()?;
    Ok(())
}

fn write_to_csv_two_columns_with_words(
    file_name: &str,
    data: &DashMap<(usize, usize), usize>,
    id_word: &DashMap<usize,String>,
) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(file_name)?;
    writer.write_record(&["Word 1", "Word 2", "Frequency"])?;

    for pair in data.iter() {
        let (word_id_1, word_id_2) = pair.key();
        let word_1 = id_word.get(&word_id_1).unwrap();
        let word_2 = id_word.get(&word_id_2).unwrap();
        writer.write_record(&[&word_1, &word_2, &pair.value().to_string()])?;
    }
    writer.flush()?;
    Ok(())
}



fn process_single_jsonl_file(
    file_path: &PathBuf,
    jieba: &Jieba,
    all_word_freq: &mut DashMap<usize, usize>,
    all_next_word_freq: &mut DashMap<(usize, usize), usize>,
    word_id: &mut DashMap<String, usize>, // 使用 Arc<Mutex<DashMap<String, usize>>> 类型
) -> Result<(), Box<dyn Error>> {
    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        if let Ok(text_line) = line {
            let v: Value = serde_json::from_str(&text_line).unwrap_or_default(); // 如果解析失败，使用默认值继续处理
            if let Some(text) = v.get("text").and_then(Value::as_str) {
                let lines: Vec<&str> = text.split('\n').collect();
                lines.into_par_iter().for_each(|line| {
                    let tokens = jieba.cut(line, false);
                    let mut token_iter = tokens.iter().peekable();
                    while let Some(token) = token_iter.next() {
                        if contains_special_characters(token) {
                            continue;
                        }

                        let fst_wid = match word_id.get(&token.to_string()) {
                            Some(existing_id_ref) => *existing_id_ref,
                            None => {
                                let id = word_id.len();
                                word_id.insert(token.to_string(), id);
                                id
                            }
                        };
                        *all_word_freq.entry(fst_wid).or_insert(0) += 1;

                        if let Some(next_token) = token_iter.peek() {
                            if !contains_special_characters(next_token) {
                                let sec_wid = match word_id.get(&next_token.to_string()) {
                                    Some(existing_id_ref) => *existing_id_ref,
                                    None => {
                                        let id = word_id.len();
                                        word_id.insert(next_token.to_string(), id);
                                        id
                                    }
                                };
                                *all_next_word_freq.entry((fst_wid, sec_wid)).or_insert(0) += 1;
                            }
                        }
                    }
                });
            }
        }
    }

    Ok(())
}

fn process_jsonl_files(directory_path: &str) -> Result<(), Box<dyn Error>> {
    let jieba = Jieba::new();

    let paths: Vec<PathBuf> = fs::read_dir(directory_path)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();

    let mut all_word_freq = DashMap::new();
    let mut all_next_word_freq = DashMap::new();
    let mut word_id =  DashMap::<String, usize>::new();
    for path in paths {
        if let Some(extension) = path.extension() {
            if extension == "jsonl" {
                if let Err(e) = process_single_jsonl_file(
                    &path,
                    &jieba,
                    &mut all_word_freq, // Pass as mutable reference
                    &mut all_next_word_freq, // Pass as mutable reference
                    &mut word_id, // Pass as mutable reference
                ) {
                    eprintln!("Error processing file {:?}: {}", path, e);
                } else {
                    // println!("Processing file {:?} success", path);
                }
            }
        }
    }

    let id_word: DashMap<usize, String> = word_id.iter().map(|pair| (pair.value().clone(), pair.key().clone())).collect();
    write_to_csv_with_words("word_freq.csv", &all_word_freq,&id_word)?;
    write_to_csv_two_columns_with_words("next_word_freq.csv", &all_next_word_freq,&id_word)?;

    Ok(())
}

fn main() {
    if let Err(e) = process_jsonl_files("data") {
        eprintln!("Error: {}", e);
    } else {
        println!("Processing of JSONL files complete.");
    }
}
