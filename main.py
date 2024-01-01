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

def process_jsonl_file(file_path, word_to_id, id_to_word, id_count, lock):

    ltp = LTP("./model/legacy")

    with open(file_path, 'r') as file:
        lines = file.readlines()
        word_freq = {}
        next_word_freq = {}

        for line in lines:
            json_data = json.loads(line)
            text = json_data.get("text", "")
            text_lines = text.split('\n')

            for t_line in text_lines:
                tokens = ltp.pipeline([t_line], tasks=["cws", "pos", "ner"]).to_tuple()[0]
                print(type(tokens))
                word_id = -1
                prev_word_id = -1

                for token in tokens:
                    if contains_special_characters(token):
                        continue

                    if token in word_to_id:
                        pass
                    elif len(id_count) > 0:
                        with lock:
                            if token not in word_to_id:
                                word_to_id[token] = id_count[0]
                                id_to_word[id_count[0]] = token
                                word_id = id_count[0]
                                id_count[0] += 1
                    word_id = word_to_id[token]
                    word_freq[word_id] = word_freq.get(word_id, 0) + 1

                    if prev_word_id >= 0:
                        next_word_freq[(prev_word_id, word_id)] = next_word_freq.get((prev_word_id, word_id), 0) + 1

                    prev_word_id = word_id

        file_name_word_freq = f"results/word_freq/{os.path.basename(file_path)}.csv"
        file_name_next_word_freq = f"results/next_word_freq/{os.path.basename(file_path)}.csv"

        write_i64_i64_map_to_csv(file_name_word_freq, word_freq)
        write_tuple_i64_i64_map_to_csv(file_name_next_word_freq, next_word_freq)



if __name__ == "__main__":
    directory_path = "data"  # Path to your JSONL files
    file_paths = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith(".jsonl")]

    manager = Manager()
    word_to_id = manager.dict()
    id_to_word = manager.dict()
    id_count = manager.list([0])
    lock = manager.Lock()

    pool = Pool(processes=8)  # Use 16 processes
    pool.starmap(process_jsonl_file, [(file_path, word_to_id, id_to_word, id_count, lock) for file_path in file_paths])
    pool.close()
    pool.join()

    file_name_word_to_id = "results/word_to_id.csv"
    write_string_i64_map_to_csv(file_name_word_to_id, word_to_id)

    print("Processing of JSONL files complete.")
