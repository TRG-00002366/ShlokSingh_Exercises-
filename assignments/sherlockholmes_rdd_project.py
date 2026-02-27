import sys
import os
import re
import string
from pyspark import SparkConf, SparkContext

INPUT_PATH = "/Users/shloksingh/Desktop/Training_DataEngineering_2366Class/ShlokSingh_Exercises-/assignments/1661.txt"
OUTPUT_DIR = "Holmes_WordCount_Results"

START_MARKERS = [
    "*** START OF THIS PROJECT GUTENBERG EBOOK",
    "*** START OF THE PROJECT GUTENBERG EBOOK"
]

END_MARKERS = [
    "*** END OF THIS PROJECT GUTENBERG EBOOK",
    "*** END OF THE PROJECT GUTENBERG EBOOK"
]

PUNCT_REGEX = re.compile(rf"[{re.escape(string.punctuation)}]")
ROMAN_REGEX = re.compile(r"^(?=[ivxlcdm]+$)m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$")

def normalize(line):
    line = line.lower()
    line = PUNCT_REGEX.sub(" ", line)
    line = re.sub(r"\s+", " ", line).strip()
    return line

def is_good_word(w):
    return w.isalpha() and len(w) > 1 and not ROMAN_REGEX.match(w)

conf = SparkConf().setAppName("SherlockHolmesRDD")
sc = SparkContext(conf=conf)

raw = sc.textFile(INPUT_PATH)

indexed = raw.zipWithIndex().map(lambda x: (x[1], x[0]))

def has_marker(line, markers):
    return any(m in line for m in markers)

start_idx = indexed.filter(lambda x: has_marker(x[1], START_MARKERS)).map(lambda x: x[0]).take(1)
end_idx = indexed.filter(lambda x: has_marker(x[1], END_MARKERS)).map(lambda x: x[0]).take(1)

if start_idx and end_idx and start_idx[0] < end_idx[0]:
    s, e = start_idx[0], end_idx[0]
    book = indexed.filter(lambda x: s < x[0] < e).map(lambda x: x[1])
else:
    book = raw

book = book.filter(lambda ln: "project gutenberg" not in ln.lower()).cache()
book.count()

# T1
normalized = book.map(normalize).cache()
normalized.count()

# T2
words_all = normalized.flatMap(lambda ln: ln.split(" ")).filter(lambda w: w != "")

# O1
stopwords = {
    "the","a","an","is","are","was","were","am","i","you","he","she","it","we","they",
    "to","of","in","on","and","or","for","with","as","at","by","from","that","this",
    "be","been","but","not","have","has","had","do","does","did","so","if","then",
    "there","here","my","your","his","her","our","their","me","him","them","us"
}
stop_bc = sc.broadcast(stopwords)

# T3
words = words_all.filter(lambda w: w not in stop_bc.value).filter(is_good_word).cache()
words.count()

# T4
total_chars = book.map(lambda ln: len(ln)).sum()

# T5
longest_line = book.filter(lambda ln: "gutenberg" not in ln.lower()).reduce(
    lambda a, b: a if len(a) >= len(b) else b
)
longest_len = len(longest_line)

# T6
watson_lines = normalized.filter(lambda ln: "watson" in ln)

# T7
unique_vocab = words.distinct().count()

# T8
word_counts = words.map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b)
top10_words = word_counts.takeOrdered(10, key=lambda x: -x[1])

# T9
first_words = normalized.filter(lambda ln: ln != "").map(lambda ln: ln.split(" ")[0]).filter(is_good_word)
first_word_counts = first_words.map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b)
top10_starters = first_word_counts.takeOrdered(10, key=lambda x: -x[1])

# T10
total_words = words.count()
total_word_chars = words.map(lambda w: len(w)).sum()
avg_word_len = total_word_chars / total_words if total_words else 0

# T11
word_len_dist = words.map(lambda w: (len(w),1)).reduceByKey(lambda a,b: a+b).sortByKey()

# T12
indexed_book = book.zipWithIndex().map(lambda x: (x[1], x[0]))
s_idx = indexed_book.filter(lambda x: "A SCANDAL IN BOHEMIA" in x[1]).map(lambda x: x[0]).take(1)
e_idx = indexed_book.filter(lambda x: "THE RED-HEADED LEAGUE" in x[1]).map(lambda x: x[0]).take(1)

if s_idx and e_idx and s_idx[0] < e_idx[0]:
    s, e = s_idx[0], e_idx[0]
    chapter = indexed_book.filter(lambda x: s <= x[0] < e).map(lambda x: x[1]).collect()
else:
    chapter = []

# O2
blank_acc = sc.accumulator(0)
def count_blank(line):
    if line.strip() == "":
        blank_acc.add(1)
    return line

book.map(count_blank).count()

# O3
word_len_pair = words.map(lambda w: (len(w), w))

# O4
letters = normalized.flatMap(list).filter(lambda ch: ch.isalpha())
char_freq = letters.map(lambda ch: (ch,1)).reduceByKey(lambda a,b: a+b).sortByKey()

# O5
grouped_words = words.map(lambda w: (w[0], w)).groupByKey().mapValues(lambda v: list(v)[:25])

print("\n===== Sherlock Holmes Analytics (RDD) =====")
print("Blank lines:", blank_acc.value)
print("T4 Total characters:", int(total_chars))
print("T5 Longest line length:", longest_len)
print("T6 Watson sample:", watson_lines.take(5))
print("T7 Unique vocab:", unique_vocab)
print("T8 Top 10 words:", top10_words)
print("T9 Top starters:", top10_starters)
print("T10 Avg word length:", round(avg_word_len,3))
print("T11 Word length distribution:", word_len_dist.take(15))
print("O3 Sample pairs:", word_len_pair.take(10))
print("O4 Char freq sample:", char_freq.take(10))
print("O5 Grouped sample:", grouped_words.take(5))
print("T12 Chapter sample:", chapter[:10])

sc.stop()