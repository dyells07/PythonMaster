import nltk
from nltk.chat.util import Chat, reflections
import re
import sqlite3
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

reflections = {
    "i am": "you are",
    "i was": "you were",
    "i": "you",
    "i'm": "you are",
    "i'd": "you would",
    "i've": "you have",
    "i'll": "you will",
    "my": "your",
    "you are": "I am",
    "you were": "I was",
    "you've": "I have",
    "you'll": "I will",
    "your": "my",
    "yours": "mine",
    "you": "me",
    "me": "you",
}

pairs = [
    [r"my name is (.*)", ["Hello %1, How are you today?",]],
    [r"hi|hey|hello", ["Hello", "Hey there",]],
    [r"what is your name ?|your name|name please", ["I am Y2K. You can call me crazy individual!",]],
    [r"how are you ?|how you doing|what about you|how about you ?", ["I'm doing good. How can I help you?",]],
    [r"sorry (.*)", ["It's alright", "It's OK, never mind",]],
    [r"I am fine", ["Great to hear that, How can I help you?",]],
    [r"(.*) continents", ["Asia, Africa, North America, South America, Antarctica, Europe, and Australia",]],
    [r"(.*) (english|hollywood) movie", ["The Shawshank Redemption", "The Lord of the Rings: The Return of the King", "Inception", "Interstellar", "Parasite", "Twilight", "Fast & Furious", "Lucky One", "A Walk to Remember", "The Last Song", "The Notebook", "The Fault in Our Stars", "Joker", "Me Before You", "All the Boys I've Loved Before", "Kissing Booth", "Titanic",]],
    [r"i'm (.*) doing good", ["Nice to hear that", "How can I help you?:)",]],
    [r"(.*) age?|are you an (idiot|stupid)|what do you think you are", ["I'm a computer program dude....Seriously you are asking me this?",]],
    [r"(.*) (online|free) courses", ["Udemy", "Udacity", "Great Learning", "Google Digital Garage", "Swayam",]],
    [r"(.*) (news channel|news)", ["BBC World News", "Fox News", "Cable News Network (CNN)", "Sky News", "MSNBC", "Republic World", "ZEE News", "ABP News",]],
    [r"(.*) (horror|spooky) movie", ["The Nun", "Annabelle", "The Conjuring", "Sinister", "The Cabin in the Woods", "Insidious", "IT", "Ouija", "Train to Busan", "The Ring", "Hush", "Evil Dead", "Oculus",]],
    [r"(.*) (bollywood|hindi) movie", ["War", "My Name is Khan", "Happy New Year", "Dilwale", "Uri", "Don", "Don 2", "Raees", "Raazi", "Kalank", "Dangal", "LUDO", "Good Newwz", "PK", "Jab Tak Hai Jaan", "Cocktail", "Bahubali", "M.S. Dhoni", "Aashiqui 2", "Dear Zindagi", "Anand", "Mughal-E-Azam", "Mother India", "Parinda", "Mr. India", "Mera Naam Joker", "Amar Akbar Anthony", "Agneepath", "Sahib Bibi Aur Ghulam", "Sholay",]],
    [r"(.*) (webseries|series)", ["You", "Lucifer", "Cursed", "Mismatched", "Money Heist", "Stranger Things", "Merlin", "The Protector", "Sabrina", "Dark", "Friends", "The Big Bang Theory", "Little Things", "Lock & Key", "Sherlock", "Sweet Tooth", "The Witcher", "Shadow and Bone", "Never Have I Ever", "Brooklyn Nine-Nine", "Ragnarok", "The Originals", "Vampire Diaries", "The Order", "The Boss Baby", "The Haunting of Hill House", "Pup Academy", "Mary Queen of Scots", "Bitten", "Titans", "Warrior Nun", "The Haunting of Bly Manor",]],
    [r"(.*) k-drama", ["Descendants of the Sun", "Busted", "Her Private Life", "What's Wrong with Secretary Kim", "It's Okay to Not Be Okay", "Hospital Playlist", "Crash Landing on You", "Weightlifting Fairy Kim Bok-joo", "My First First Love", "Beauty Inside", "Was It Love",]],
    [r"(.*) (novel|book)", ["Harry Potter", "Twilight", "The Alchemist", "Angels and Demons", "Dead Beautiful", "The Lost Symbol", "The Da Vinci Code", "Hunger Games",]],
    [r"(.*) created ?", ["I am created using Python's NLTK library", "Top secret",]],
    [r"(.*) band", ["BTS", "The Beatles", "The Rolling Stones", "Maroon 5", "One Direction", "No Doubt", "Blackpink", "EXO", "Monsta X", "Stray Kids", "The Chainsmokers",]],
    [r"(.*) actress", ["Scarlett Johansson", "Jennifer Lawrence", "Emma Watson", "Margot Robbie", "Angelina Jolie", "Kristen Stewart", "Rachel McAdams", "Deepika Padukone", "Priyanka Chopra", "Alia Bhatt", "Kareena Kapoor", "Nora Fatehi", "Jacqueline Fernandez", "Aishwarya Rai", "Sara Ali Khan", "Shraddha Kapoor", "Anushka Sharma", "Disha Patani",]],
    [r"(.*) (game|sport)", ["Cricket", "Hockey", "Basketball", "Football", "Baseball", "Badminton", "Tennis", "Swimming", "Archery", "Skating", "Volleyball", "Table Tennis", "Golf",]],
    [r"(.*) (sports person|player)", ["Lionel Messi", "Sania Mirza", "Sachin Tendulkar", "Virat Kohli", "Kevin Durant", "Hardik Pandya", "Rohit Sharma", "P.V. Sindhu", "Parupalli Kashyap", "Dhyan Chand", "Cristiano Ronaldo", "Robert Lewandowski", "Chris Gayle", "Steve Smith", "David Warner", "Ricky Ponting", "Stephen Curry", "LeBron James", "M.S. Dhoni", "Chris Paul",]],
    [r"(.*) actor", ["Robert Downey Jr.", "Chris Hemsworth", "Tom Holland", "Brad Pitt", "Tom Hiddleston", "Tom Cruise", "Chris Evans", "Benedict Cumberbatch", "Paul Rudd", "Jeremy Renner", "Ian Somerhalder", "Paul Wesley", "Aamir Khan", "Amitabh Bachchan", "Anil Kapoor", "Ranveer Singh", "Ranbir Kapoor", "Salman Khan", "Sanjay Dutt", "Shah Rukh Khan", "Tiger Shroff", "Varun Dhawan",]],
    [r"(.*) dialogue", ["Mere paas maa hai.", "Pushpa, I hate tears…", "Kitne aadmi the!", "Babumoshai, zindagi badi honi chahiye, lambi nahi.", "Rishtey mein toh hum tumhare baap lagte hai, naam hai Shahenshaah!", "Dosti ka ek usool hai madam – no sorry, no thank you.", "Mogambo khush hua!", "Hum jahan khade hote hain line yahi se shuru hoti hai.", "Bade bade deshon mein aisi choti-choti baatein hoti rehti hai, Senorita.", "Haar kar jeetne wale ko baazigar kehte hai.", "Mere Karan Arjun aayenge.", "Agar maa ka doodh piya hai toh samne aa!", "Uska to na bad luck hi kharab hai.", "Crime Master Gogo naam hai mera, aankhen nikal ke gotiyan khelta hun main.", "Tareekh pe tareekh, tareekh pe tareekh, tareekh pe tareekh milti gayi My Lord, par insaaf nahi mila", "Rahul, naam toh suna hi hoga.", "Mein apni favourite hoon!", "Picture abhi baaki hai mere dost!", "How’s the josh?", "Thappad se darr nahi lagta sahab, pyaar se lagta hai.", "Filmein sirf teen cheezo ke wajah se chalti hai…entertainment, entertainment, entertainment…aur main entertainment hoon.", "All izz well",]],
    [r"quit", ["Bye take care. See you soon :)", "It was nice talking to you. See you soon :)",]],
     [r"(.*) (weather|temperature) in (.*)", ["I'm not sure about the current weather, but you can check a weather website for the latest updates in %3.",]],
    [r"(.*) (recipe|cooking)", ["What kind of recipe are you looking for? I can help with many types like pasta, desserts, and more.",]],
    [r"(.*) (joke|funny)", ["Why don’t scientists trust atoms? Because they make up everything!", "Why did the scarecrow win an award? Because he was outstanding in his field!",]],
    [r"(.*) (technology|tech)", ["Are you interested in the latest tech news or gadgets? I can provide information on a variety of technology topics.",]],
    [r"(.*) (travel|vacation)", ["Where are you thinking of traveling? I can suggest popular destinations and travel tips.",]],
    [r"(.*) (health|fitness)", ["Do you need advice on workouts or healthy eating? I can share some tips on fitness and wellness.",]],
    [r"(.*) (history|historical)", ["Are you looking for information on a specific historical event or figure? I can help with various historical topics.",]],
    [r"(.*) (music|songs)", ["What kind of music or songs are you interested in? I can suggest popular tracks or artists based on your preferences.",]],
    [r"(.*) (book recommendation|book)", ["What genre are you interested in? I can suggest books from fiction, non-fiction, mystery, and more.",]],
    [r"(.*) (movie recommendation|film)", ["What type of movie are you in the mood for? I can suggest films based on genres or recent releases.",]],
    [r"(.*) (sports|latest scores)", ["I can provide updates on recent scores or information about different sports. What sport are you interested in?",]],
    [r"(.*) (current events|news)", ["For the latest updates on current events, you might want to check out news websites or apps.",]],
    [r"(.*) (celebrity news|gossip)", ["Are you looking for the latest news about a particular celebrity or just general gossip?",]],
    [r"(.*) (gaming|video games)", ["Are you looking for recommendations on new games or updates about your favorite ones?",]],
    [r"(.*) (science|scientific)", ["Are you interested in recent scientific discoveries or topics in a specific field of science?",]],
    [r"(.*) (art|artist)", ["What type of art or which artist are you interested in? I can provide information on various art styles and famous artists.",]],
    [r"(.*) (finance|investment)", ["Do you need advice on personal finance or investment opportunities? I can provide general information and tips.",]],
    [r"(.*) (education|learning)", ["Are you looking for resources or tips for learning new skills or subjects? I can recommend online courses and materials.",]],
    [r"(.*) (pets|animals)", ["Are you interested in information about pets or wild animals? I can provide tips on pet care or facts about animals.",]],
    [r"(.*) (fashion|style)", ["Do you need advice on fashion trends or style tips? I can help with various fashion-related questions.",]],
    [r"(.*) (movies|film) (recommendation|suggestion)", ["Sure! What genre are you interested in? I can recommend movies based on that.",]],
    [r"(.*) (music|songs) (recommendation|suggestion)", ["I’d be happy to suggest some music. What genre or artist do you like?",]],
    [r"(.*) (book|novel) (recommendation|suggestion)", ["What genre of books are you interested in? I can suggest a few based on your preference.",]],
    [r"(.*) (best|top) (restaurants|places) in (.*)", ["For the best places in %3, you might want to check local review sites or guides for recommendations.",]],
    [r"(.*) (motivation|inspiration)", ["Need some motivation? 'The only way to do great work is to love what you do.' – Steve Jobs",]],
    [r"(.*) (diet|nutrition)", ["Are you looking for advice on a specific diet or general nutrition tips? I can help with both.",]],
    [r"(.*) (language|learn|study)", ["Which language are you interested in learning? I can suggest resources and tips for various languages.",]],
    [r"(.*) (relationship|love) advice", ["Relationships can be complex. It’s important to communicate openly and honestly. What specific advice are you looking for?",]],
    [r"(.*) (car|automobile) advice", ["Are you looking for information on car maintenance, buying tips, or something else? I can provide general advice.",]],
    [r"(.*) (healthcare|doctor) advice", ["For healthcare questions, it’s always best to consult with a professional. What specific information are you seeking?",]],
    [r"(.*) (event|party) planning", ["Are you planning an event? I can offer tips and suggestions on organizing parties or events.",]],
    [r"(.*) (DIY|crafts)", ["Looking for DIY project ideas or craft tips? I can suggest some fun and creative activities for you.",]],
    [r"(.*) (travel|trip) planning", ["Need help planning a trip? I can suggest destinations, packing tips, and travel advice.",]],
    [r"(.*) (technology|latest gadgets)", ["Interested in the latest gadgets? I can provide information on new tech releases and trends.",]],
    [r"(.*) (job|career) advice", ["Are you looking for career advice or job search tips? I can offer guidance on resumes, interviews, and more.",]],
    [r"(.*) (mental health|wellbeing)", ["Mental health is important. It’s good to talk about your feelings. Do you need tips or resources for mental wellbeing?",]],
    [r"(.*) (financial planning|budgeting)", ["Looking to manage your finances better? I can provide tips on budgeting and financial planning.",]],
    [r"(.*) (sports|fitness) routines", ["Interested in fitness routines? I can suggest exercises and workout plans based on your goals.",]],
    [r"(.*) (history|historical facts)", ["Are you interested in historical facts or events? I can provide information on various historical topics.",]],
    [r"(.*) (celebrity|famous person) news", ["Want updates on celebrities or famous personalities? I can provide recent news and updates.",]],
    [r"(.*) (cooking|recipes)", ["Looking for cooking tips or recipes? I can suggest dishes and provide recipe ideas.",]],
    [r"(.*) (home improvement|decor)", ["Thinking of improving your home? I can offer tips and ideas for home decoration and renovation.",]],
    [r"(.*) (pets|animal care)", ["Need advice on taking care of pets or animals? I can provide tips on pet care and animal health.",]],
    [r"(.*) (shopping|fashion)", ["Looking for shopping tips or fashion advice? I can suggest trends and popular items.",]],
]

def initialize_database():
    conn = sqlite3.connect('chatbot.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_input TEXT,
            bot_response TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Call this function once to ensure the table is created
initialize_database()

def save_conversation(user_input, bot_response):
    conn = sqlite3.connect('chatbot.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO conversations (user_input, bot_response)
        VALUES (?, ?)
    ''', (user_input, bot_response))
    conn.commit()
    conn.close()

def retrieve_similar_conversations(user_input):
    conn = sqlite3.connect('chatbot.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_input, bot_response FROM conversations
    ''')
    conversations = cursor.fetchall()
    conn.close()

    if not conversations:
        return []

    # Use TF-IDF Vectorizer to compute similarity
    vectorizer = TfidfVectorizer().fit_transform([user_input] + [conv[0] for conv in conversations])
    similarity_matrix = cosine_similarity(vectorizer[0:1], vectorizer[1:])
    most_similar = similarity_matrix[0].argmax()
    return [conversations[most_similar][1]]

class AdvancedChat(Chat):
    def __init__(self, pairs, reflections):
        super().__init__(pairs, reflections)
        self.history = []
        self.user_name = None

    def converse(self):
        print("Hi! I am Y2K. How can I assist you today?")
        while True:
            user_input = input("You: ")
            if user_input.lower() in ["quit", "exit", "bye"]:
                print("Goodbye! Have a great day!")
                break
            
            self.history.append(f"You: {user_input}")
            response = self.respond(user_input)
            self.history.append(f"Y2K: {response}")
            print(f"Y2K: {response}")
            save_conversation(user_input, response)
    
    def respond(self, user_input):
        match = re.match(r"my name is (.*)", user_input)
        if match:
            self.user_name = match.group(1)
            return f"Hello {self.user_name}, How are you today?"
        
        similar_responses = retrieve_similar_conversations(user_input)
        if similar_responses:
            return f"I've seen similar questions before. Here's one way to respond: {similar_responses[0]}"
        
        if self.user_name:
            if re.search(r"(how are you\?)|(what's up\?)", user_input, re.I):
                return f"I'm good, {self.user_name}. How can I assist you today?"

        for pattern, responses in self._pairs:
            match = re.match(pattern, user_input)
            if match:
                response = responses[0]
                if self.user_name and "my name is" not in user_input:
                    response = re.sub(r"(.*)", f"{self.user_name}, {response}", response)
                return response
        
        return "I'm not sure how to respond to that."

def chat():
    chat = AdvancedChat(pairs, reflections)
    chat.converse()

if __name__ == "__main__":
    chat()