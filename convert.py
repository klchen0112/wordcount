import os
import chardet
import codecs

# 创建全局的 detector
detector = chardet.UniversalDetector()

def detect_encoding(file_path):
    global detector
    detector.reset()
    
    with open(file_path, 'rb') as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    return detector.result['encoding'] if detector.result and 'encoding' in detector.result else None

def convert_to_utf8(file_path, encoding):
    with open(file_path, 'r', encoding=encoding, errors='replace') as f:
        content = f.read()

    utf8_content = content.encode('utf-8')

    with open(file_path, 'wb') as f:
        f.write(utf8_content)

def convert_folder_to_utf8(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.txt'):
                file_path = os.path.join(root, file_name)
                detected_encoding = detect_encoding(file_path)

                if detected_encoding and detected_encoding.lower() != 'utf-8':
                    print(f"Converting {file_path} from {detected_encoding} to utf-8")
                    convert_to_utf8(file_path, detected_encoding)
                elif detected_encoding:
                    print(f"{file_path} is already utf-8")
                else:
                    print(f"Unable to detect encoding for {file_path}")

# 用法示例
folder_path = 'data'
convert_folder_to_utf8(folder_path)

