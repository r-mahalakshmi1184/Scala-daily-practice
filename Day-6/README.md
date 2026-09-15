# Day 6 – Word Count

## Topics Covered

- Classic Word Count using Spark RDDs
- `flatMap`
- `map`
- `reduceByKey`
- Case-insensitive word counting
- Removing punctuation
- Ignoring empty words
- Finding the top 10 most frequent words

## Word Count Flow

```text
Input Lines
    ↓
flatMap
    ↓
Individual Words
    ↓
map
    ↓
(word, 1)
    ↓
reduceByKey
    ↓
(word, count)
