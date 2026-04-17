from collections import defaultdict

if __name__ == "__main__":
    words_count: dict[str, int] = defaultdict(int)

    with open("data/words.txt", "r") as f:
        for line in f:
            words = line.split()
            for word in words:
                words_count[word] += 1
    print(words_count)
