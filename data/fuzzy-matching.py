import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import multiprocessing as mp
from tqdm import tqdm

def process_name(name, index, bf_names, threshold):
    matches = []
    for bf_name in bf_names:
        similarity = fuzz.ratio(name, bf_name)
        if similarity >= threshold:
            matches.append((bf_name, similarity))
    matches.sort(key=lambda x: x[1], reverse=True)
    top_matches = matches[:5]  # Keep top 5 matches
    return [(index, name, match[0], match[1]) for match in top_matches]

def process_chunk(args):
    chunk, bf_names, threshold, chunk_id, total_chunks = args
    results = []
    with tqdm(total=len(chunk), desc=f"Chunk {chunk_id}/{total_chunks}", position=chunk_id) as pbar:
        for _, row in chunk.iterrows():
            results.extend(process_name(row['name'], row['index'], bf_names, threshold))
            pbar.update(1)
    return results

def main():
    # Load your dataframes
    name_mapping_initial = pd.read_csv('processing-files/name_mapping_w_home.csv')
    betfair_names_remaining = pd.read_csv('processing-files/betfair_names_remaining_w_home.csv')

    # Convert betfair names to a list for faster processing
    bf_names = betfair_names_remaining['bf_name'].tolist()

    # Set similarity threshold
    threshold = 80  # You can adjust this value

    # Split the data into chunks for multiprocessing
    num_processes = mp.cpu_count()
    chunks = np.array_split(name_mapping_initial, num_processes)

    # Prepare arguments for multiprocessing
    pool_args = [(chunk, bf_names, threshold, i+1, num_processes) for i, chunk in enumerate(chunks)]

    # Use multiprocessing to process chunks in parallel
    with mp.Pool(processes=num_processes) as pool:
        results = list(pool.imap(process_chunk, pool_args))

    # Flatten results
    flat_results = [item for sublist in results for item in sublist]

    # Convert results to DataFrame
    df_results = pd.DataFrame(flat_results, columns=['index', 'name', 'bf_name', 'similarity'])

    # Save results to CSV
    df_results.to_csv('processing-files/fuzzy_name_matches_w_home.csv', index=False)
    print("\nResults saved to fuzzy_name_matches_w_home.csv")

if __name__ == "__main__":
    main()