import nltk
import os
import shutil

# Function to download and verify the 'punkt' tokenizer
def download_and_verify_punkt():
    try:
        # Attempt to download 'punkt'
        nltk.download('punkt', quiet=True)
        
        # Verify that 'punkt' is available
        nltk.data.find('tokenizers/punkt')
        print("Punkt tokenizer is successfully downloaded and available.")
    except LookupError:
        print("Punkt tokenizer is not available. Please check the download or directory.")

# Function to reset the NLTK data directory
def reset_nltk_data_directory():
    # Path to the NLTK data directory
    nltk_data_dir = nltk.data.path[0]

    # Remove the existing NLTK data directory if it exists
    if os.path.exists(nltk_data_dir):
        shutil.rmtree(nltk_data_dir)
    
    # Recreate the NLTK data directory
    os.makedirs(nltk_data_dir)
    
    # Attempt to download 'punkt' again
    download_and_verify_punkt()

# Call the functions to reset NLTK data directory and download 'punkt'
reset_nltk_data_directory()

# Importing necessary NLTK modules
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, brown, treebank
from nltk.tag import UnigramTagger, BigramTagger, TrigramTagger, DefaultTagger, BrillTaggerTrainer, brill

# Download additional necessary data
nltk.download('averaged_perceptron_tagger', quiet=True)
nltk.download('brown', quiet=True)
nltk.download('treebank', quiet=True)
nltk.download('stopwords', quiet=True)

# ==============================
# Example 1: Basic POS Tagging Using Pre-trained Tagger
# ==============================
sentence = "The quick brown fox jumps over the lazy dog."
words = word_tokenize(sentence)
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
words = word_tokenize(sentence)
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
words = word_tokenize(sentence)
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
words = word_tokenize(sentence)
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
words = word_tokenize(sentence)

filtered_words = [w for w in words if not w in stopwords.words("english")]

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
