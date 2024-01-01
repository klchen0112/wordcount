import os
import json
from multiprocessing import Pool, Manager
from ltp import LTP

def contains_special_characters(s):
    for c in s:
        if c in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~' or c.isspace() or c.isascii():
            return True
        if '\u3000' <= c <= '\u303F' or '\u3008' <= c <= '\u3011' or '\uFF00' <= c <= '\uFFEF':
            return True
        if c in ['"', '“', '”']:
            return True
    return False

def write_i64_i64_map_to_csv(file_name, _map):
    with open(file_name, 'w') as file:
        for key, value in _map.items():
            file.write(f"{key},{value}\n")

def write_tuple_i64_i64_map_to_csv(file_name, _map):
    with open(file_name, 'w') as file:
        for (key1, key2), value in _map.items():
            file.write(f"{key1},{key2},{value}\n")

def write_string_i64_map_to_csv(file_name, _map):
    with open(file_name, 'w') as file:
        for key, value in _map.items():
            file.write(f"{key},{value}\n")

def process_jsonl_file(file_path, word_to_id, id_to_word, id_count, ltp_model):

    ltp = ltp_model

    with open(file_path, 'r') as file:
        print(file_path)
        word_freq = {}
        next_word_freq = {}
        line = file.readline()
        while line:
            line = line.strip()
            json_data = json.loads(line)
            text = json_data.get("text", "")
            text = text.strip()
            text_lines = text.split('\n')
            tokens_list = ltp.pipeline(text_lines, tasks=["cws"]).cws
            for tokens in tokens_list:
                word_id = -1
                prev_word_id = -1

                for token in tokens:
                    if contains_special_characters(token):
                        continue

                    if token in word_to_id:
                        pass
                    else:
                        word_to_id[token] = id_count[0]
                        id_to_word[id_count[0]] = token
                        word_id = id_count[0]
                        id_count[0] += 1
                    word_id = word_to_id[token]
                    word_freq[word_id] = word_freq.get(word_id, 0) + 1

                    if prev_word_id >= 0:
                        next_word_freq[(prev_word_id, word_id)] = next_word_freq.get((prev_word_id, word_id), 0) + 1

                    prev_word_id = word_id
            line = file.readline()

        file_name_word_freq = f"results/word_freq/{os.path.basename(file_path)}.csv"
        file_name_next_word_freq = f"results/next_word_freq/{os.path.basename(file_path)}.csv"

        write_i64_i64_map_to_csv(file_name_word_freq, word_freq)
        write_tuple_i64_i64_map_to_csv(file_name_next_word_freq, next_word_freq)



if __name__ == "__main__":
    directory_path = "data"  # Path to your JSONL files
    os.makedirs("results/word_freq",exist_ok=True)
    os.makedirs("results/next_word_freq",exist_ok=True)
    file_paths = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith(".jsonl")]

    word_to_id = dict()
    id_to_word = dict()
    id_count = [0]
    ltp_model = LTP("./model/legacy")

    for file_path in file_paths:
        process_jsonl_file(file_path, word_to_id, id_to_word, id_count, ltp_model=ltp_model)


    file_name_word_to_id = "results/word_to_id.csv"
    write_string_i64_map_to_csv(file_name_word_to_id, word_to_id)

    print("Processing of JSONL files complete.")
