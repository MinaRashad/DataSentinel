import os
import sys

def split_file(dataset_path, chunk_size):
    folder_name = os.path.splitext(dataset_path)[0]
    os.makedirs(folder_name, exist_ok=True)

    with open(dataset_path, 'r') as dataset_file:
        chunk_num = 1
        lines = dataset_file.readlines()
        num_lines = len(lines)
        start_idx = 0
        end_idx = chunk_size

        while start_idx < num_lines:
            chunk_lines = lines[start_idx:end_idx]

            chunk_filename = os.path.join(folder_name, f'chunk_{chunk_num}.txt')
            with open(chunk_filename, 'w') as chunk_file:
                chunk_file.writelines(chunk_lines)

            chunk_num += 1
            start_idx = end_idx
            end_idx += chunk_size

    print(f'Splitting complete. {chunk_num - 1} files created in the folder: {folder_name}')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python indexer.py <dataset_path>')
        sys.exit(1)

    dataset_path = sys.argv[1]
    chunk_size = 10000

    split_file(dataset_path, chunk_size)
