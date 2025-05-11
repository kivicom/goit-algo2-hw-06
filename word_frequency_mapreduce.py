"""
Module for analyzing word frequency in text using MapReduce and visualizing the results.
Requires: requests, matplotlib
"""

import re
from collections import defaultdict
from multiprocessing import Pool
import requests
import matplotlib.pyplot as plt

def map_function(chunk):
    """Map function to count word frequencies in a text chunk."""
    word_counts = defaultdict(int)
    words = re.findall(r'\b\w+\b', chunk.lower())
    for word in words:
        word_counts[word] += 1
    return word_counts

def reduce_function(mapped_results):
    """Reduce function to aggregate word counts from all chunks."""
    final_counts = defaultdict(int)
    for result in mapped_results:
        for word, count in result.items():
            final_counts[word] += count
    return dict(final_counts)

def visualize_top_words(word_freq, top_n=10):
    """Visualize the top N most frequent words."""
    # Sort by frequency and get top N
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:top_n]
    words, frequencies = zip(*sorted_words)

    plt.figure(figsize=(10, 6))
    plt.bar(words, frequencies, color='skyblue')
    plt.title('Top 10 Most Frequent Words')
    plt.xlabel('Words')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def process_text(url):
    """Process text from URL using MapReduce and visualize results."""
    # Download text from URL
    response = requests.get(url)
    if response.status_code != 200:
        raise requests.RequestException(f"Failed to download text from {url}")
    text = response.text

    # Split text into chunks for parallel processing
    chunk_size = len(text) // 4  # Divide into 4 chunks for multiprocessing
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    # Use multiprocessing Pool for parallel mapping
    with Pool(processes=4) as pool:
        mapped_results = pool.map(map_function, chunks)

    # Reduce phase
    word_freq = reduce_function(mapped_results)

    # Visualize top 10 words
    visualize_top_words(word_freq)

if __name__ == "__main__":
    # Example URL (replace with any valid URL containing text)
    SAMPLE_URL = "https://www.gutenberg.org/cache/epub/2701/pg2701.txt"  # Moby Dick by Herman Melville
    process_text(SAMPLE_URL)