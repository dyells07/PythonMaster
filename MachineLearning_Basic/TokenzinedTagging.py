import nltk
from nltk.corpus import stopwords, brown, treebank
from nltk.tag import UnigramTagger, BigramTagger, TrigramTagger, DefaultTagger, BrillTaggerTrainer, brill

# Define a function to ensure NLTK resources are downloaded
def ensure_nltk_resources():
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        print("Downloading 'punkt' tokenizer models...")
        nltk.download('punkt')
    
    try:
        nltk.data.find('averaged_perceptron_tagger')
    except LookupError:
        print("Downloading 'averaged_perceptron_tagger'...")
        nltk.download('averaged_perceptron_tagger')
    
    try:
        nltk.data.find('corpora/brown')
    except LookupError:
        print("Downloading 'brown' corpus...")
        nltk.download('brown')
    
    try:
        nltk.data.find('corpora/treebank')
    except LookupError:
        print("Downloading 'treebank' corpus...")
        nltk.download('treebank')
    
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        print("Downloading 'stopwords'...")
        nltk.download('stopwords')

# Ensure all necessary NLTK resources are available
ensure_nltk_resources()

# ==============================
# Example 1: Basic POS Tagging Using Pre-trained Tagger
# ==============================
sentence = "The quick brown fox jumps over the lazy dog."
words = nltk.word_tokenize(sentence)
tagged_words = nltk.pos_tag(words)
print("Basic POS Tagging:")
print(tagged_words)
print()

# ==============================
# Example 2: N-gram Tagging with Backoff Using Multiple Corpora
# ==============================
brown_tagged_sents = brown.tagged_sents(categories='news')
treebank_tagged_sents = treebank.tagged_sents()
combined_corpora = brown_tagged_sents + treebank_tagged_sents

tagger = None
for n in range(1, 4):
    if n == 1:
        tagger = UnigramTagger(combined_corpora, backoff=tagger)
    elif n == 2:
        tagger = BigramTagger(combined_corpora, backoff=tagger)
    else:
        tagger = TrigramTagger(combined_corpora, backoff=tagger)

sentence = "She saw a beautiful butterfly flying in the garden."
words = nltk.word_tokenize(sentence)
tagged_words = tagger.tag(words)
print("N-gram Tagging with Backoff Using Multiple Corpora:")
print(tagged_words)
print()

# ==============================
# Example 3: Custom Training with Backoff Tagger
# ==============================
brown_tagged_sents = brown.tagged_sents(categories='news')
unigram_tagger = UnigramTagger(brown_tagged_sents)
bigram_tagger = BigramTagger(brown_tagged_sents, backoff=unigram_tagger)
trigram_tagger = TrigramTagger(brown_tagged_sents, backoff=bigram_tagger)

sentence = "The company plans to launch a new product."
words = nltk.word_tokenize(sentence)
tagged_words = trigram_tagger.tag(words)
print("Custom Training with Backoff Tagger:")
print(tagged_words)
print()

# ==============================
# Example 4: Using a Default Tagger with Backoff
# ==============================
default_tagger = DefaultTagger('NN')
unigram_tagger = UnigramTagger(brown_tagged_sents, backoff=default_tagger)
bigram_tagger = BigramTagger(brown_tagged_sents, backoff=unigram_tagger)
trigram_tagger = TrigramTagger(brown_tagged_sents, backoff=bigram_tagger)

sentence = "Elon Musk announced a new Tesla model."
words = nltk.word_tokenize(sentence)
tagged_words = trigram_tagger.tag(words)
print("Using a Default Tagger with Backoff:")
print(tagged_words)
print()

# ==============================
# Example 5: Evaluate Tagger Accuracy
# ==============================
brown_tagged_sents = brown.tagged_sents(categories='news')
train_size = int(0.8 * len(brown_tagged_sents))
train_sents = brown_tagged_sents[:train_size]
test_sents = brown_tagged_sents[train_size:]

unigram_tagger = UnigramTagger(train_sents)
bigram_tagger = BigramTagger(train_sents, backoff=unigram_tagger)
trigram_tagger = TrigramTagger(train_sents, backoff=bigram_tagger)

accuracy = trigram_tagger.evaluate(test_sents)
print(f"Evaluate Tagger Accuracy:")
print(f"Accuracy: {accuracy:.2%}")
print()

# ==============================
# Example 6: Using Brill Tagger with Backoff
# ==============================
train_sents = treebank.tagged_sents()[:3000]
test_sents = treebank.tagged_sents()[3000:]

unigram_tagger = UnigramTagger(train_sents)
bigram_tagger = BigramTagger(train_sents, backoff=unigram_tagger)

templates = brill.fntbl37()
trainer = BrillTaggerTrainer(bigram_tagger, templates)
brill_tagger = trainer.train(train_sents)

accuracy = brill_tagger.evaluate(test_sents)
print("Using Brill Tagger with Backoff:")
print(f"Brill Tagger Accuracy: {accuracy:.2%}")
print()

# ==============================
# Example 7: Stop Words Removal and Sequential N-gram Tagging
# ==============================
sentence = "He tends to carp on a lot. He caught a magnificent carp!"
words = nltk.word_tokenize(sentence)

filtered_words = [w for w in words if w.lower() not in stopwords.words("english")]

brown_a = brown.tagged_sents(categories='news')
tagger = None
for n in range(1, 4):  # Unigram, Bigram, Trigram with backoff
    if n == 1:
        tagger = UnigramTagger(brown_a, backoff=tagger)
    elif n == 2:
        tagger = BigramTagger(brown_a, backoff=tagger)
    else:
        tagger = TrigramTagger(brown_a, backoff=tagger)

tagged_words = tagger.tag(filtered_words)
print("Stop Words Removal and Sequential N-gram Tagging:")
print(tagged_words)
