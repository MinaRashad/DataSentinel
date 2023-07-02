import os
import sys

def split_file(dataset_path, chunk_size, watchlist=[]):
    # take everything after / 
    breaches = []
    folder_name = dataset_path.split('/')[-1] + '_chunked'

    os.makedirs(folder_name, exist_ok=True)

    # read file stream
    chunk_num = 1
    with open(dataset_path, 'rb') as dataset_file:
        while True:
            chunk = dataset_file.read(chunk_size)
            if not chunk:
                break

            # check if last character is \n
            while chunk[-1] != 10:
                # read more bytes until \n is found
                char = dataset_file.read(1)
                if not char:
                    break
                chunk += char
                # break if EOF
                

            # check if chunk contains any words from watchlist
            if watchlist:
                decoded_chunk = chunk.decode('latin-1')

                for line in watchlist:
                    # index of line in chunk
                    idx = decoded_chunk.find(line)

                    if idx != -1:
                        print(f'Found {line} in chunk {chunk_num} at index {idx}')
                        # get \n index before and after idx
                        
                        line_start = decoded_chunk.rfind('\n', 0, idx)
                        line_end = decoded_chunk.find('\n', idx)

                        # get line from chunk
                        breach = decoded_chunk[line_start:line_end]
                        breaches.append(breach)
                        print(f'{breach}')
                        
            # write chunk to file
            with open(f'{folder_name}/chunk_{chunk_num}.txt', 'wb') as chunk_file:
                chunk_file.write(chunk)
            chunk_num += 1


    print(f'{chunk_num-1} files created in {folder_name}')
    
    if breaches:
        print(f"Alert! Found {len(breaches)} breaches in {dataset_path}")
        for breach in breaches:
            print(f'{breach}')
    else:
        print(f'No breaches found in {dataset_path}')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python indexer.py <dataset_path> [--watchlist <watchlist_path>]')
        sys.exit(1)

    dataset_path = sys.argv[1]

    # get --watchlist argument
    watchlist_path = None
    if len(sys.argv) == 4:
        if sys.argv[2] == '--watchlist':
            watchlist_path = sys.argv[3]

    chunk_size = 10 * 1024 * 1024 # 10 MB

    watchlist = []

    if watchlist_path:
        with open(watchlist_path, 'r') as watchlist_file:
            watchlist = watchlist_file.readlines()
        
        watchlist = [word.strip() for word in watchlist]
        

    split_file(dataset_path, chunk_size, watchlist)
