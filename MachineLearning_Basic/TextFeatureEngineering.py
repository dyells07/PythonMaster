from bs4 import BeautifulSoup
import re
import csv

# Load the dataset
def load_data(file_path):
    comments = []
    with open(file_path, 'rt') as f:
        reader = csv.DictReader(f)
        for line in reader:
            # Use BeautifulSoup to clean HTML tags
            comment = BeautifulSoup(str(line["Comment"]), "html.parser").get_text()
            comments.append(comment)
    return comments

# Clean and tokenize the text data
def clean_text(text):
    # Remove email addresses
    text = re.sub(r'[\w\-][\w\-\.]+@[\w\-][\w\-\.]+[a-zA-Z]{1,4}', '_EM', text)
    # Remove URLs
    text = re.sub(r'\w+:\/\/\S+', r'_U', text)
    # Format whitespaces and newlines
    text = text.replace('"', ' ').replace('\'', ' ').replace('_', ' ').replace('-', ' ')
    text = text.replace('\n', ' ').replace('\\n', ' ')
    text = re.sub(' +',' ', text)
    
    # Manage punctuation
    text = re.sub(r'([^!\?])(\?{2,})(\Z|[^!\?])', r'\1 _BQ\n\3', text)
    text = re.sub(r'([^\.])(\.{2,})', r'\1 _SS\n', text)
    text = re.sub(r'([^!\?])(\?|!){2,}(\Z|[^!\?])', r'\1 _BX\n\3', text)
    text = re.sub(r'([^!\?])\?(\Z|[^!\?])', r'\1 _Q\n\2', text)
    text = re.sub(r'([^!\?])!(\Z|[^!\?])', r'\1 _X\n\2', text)
    
    # Manage extended characters (e.g. loooong words)
    text = re.sub(r'([a-zA-Z])\1\1+(\w*)', r'\1\1\2 _EL', text)
    
    # Tokenize smileys
    text = re.sub(r'([#%&\*\$]{2,})(\w*)', r'\1\2 _SW', text)
    text = re.sub(r' [8x;:=]-?(?:\)|\}|\]|>){2,}', r' _BS', text)
    text = re.sub(r' (?:[;:=]-?[\)\}\]d>])|(?:<3)', r' _S', text)
    text = re.sub(r' [x:=]-?(?:\(|\[|\||\\|/|\{|<){2,}', r' _BF', text)
    text = re.sub(r' [x:=]-?[\(\[\|\\/\{<]', r' _F', text)
    
    # Remove non-alphabetical characters
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    
    return text

# Split into phrases and tokenize
def tokenize_text(text):
    # Split by punctuation
    phrases = re.split(r'[;:\.()\n]', text)
    # Tokenize by finding words
    phrases = [re.findall(r'[\w%\*&#]+', ph) for ph in phrases]
    # Remove empty lists
    phrases = [ph for ph in phrases if ph]
    
    words = []
    for ph in phrases:
        words.extend(ph)
    
    return words

# Save the tokenized comments to a CSV file
def save_to_csv(output_path, data):
    with open(output_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['Tokenized_Comment'])
        # Write each row
        for row in data:
            writer.writerow([' '.join(row)])  # Join tokens with space before writing

# Example usage:
file_path = 'trolls.csv'  # Replace with the path to your dataset
output_path = 'tokenized_comments.csv'  # Output CSV file

# Load and clean data
comments = load_data(file_path)

# Tokenize and save to CSV
tokenized_data = []
for comment in comments:
    cleaned_text = clean_text(comment)
    tokenized_text = tokenize_text(cleaned_text)
    tokenized_data.append(tokenized_text)

# Save the tokenized comments to CSV
save_to_csv(output_path, tokenized_data)

print(f"Tokenized data has been saved to {output_path}.")
