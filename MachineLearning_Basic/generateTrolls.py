import csv
import random
import string
from faker import Faker

# Initialize Faker to generate fake data
fake = Faker()

# Function to generate random text for comments
def generate_random_comment():
    # Random selection of templates for the comments
    templates = [
    "@{} you're so dumb!",
    "Visit my site at {} and win big!",
    "This is totally awesoooome!!!",
    "My email is {}, contact me!",
    "LOL this is hilarious :D",
    "Ugh... I can't believe you said that!",
    "This is the worst thing ever :( :( :(",
    "Wanna play? Add me at {}",
    "Your post is idiotic and you should feel bad.",
    "I h@te everything ab0ut th1s!!!!!!!",
    "How dare you say something like that???!!!!",
    "Congratulations!!! You won a free trip to {}",
    "Check out this awesome video http://{}.com",
    "I'm so done with this nonsense... goodbye.",
    "Seriously, what were you thinking?!?",
    "ROFL!!! That's the funniest thing I've seen today.",
    "Follow me on Twitter @{} for more updates.",
    "{} is the best and you all should agree with me.",
    "This makes no sense at all. Do you even logic?",
    "What a complete waste of time. Thanks for nothing!",
    "You clearly don't know anything about {}.",
    "This is the dumbest thing I've read all day...",
    "You're just jealous because {} is better than you!",
    "Why would anyone even listen to {}? Ridiculous!",
    "You're so wrong, I can't even start to explain.",
    "How about you educate yourself on {}, genius?",
    "Nobody asked for your opinion on {}.",
    "OMG, this is literally the best thing I've seen.",
    "So sad to see people like you ruining the internet.",
    "What a load of garbage... unfollowing.",
    "Your opinion on {} is trash.",
    "Can you even spell or are you too busy trolling?",
    "Wow, you're a keyboard warrior, aren't you?",
    "Not surprised someone like you would say that.",
    "I can't believe how stupid this comment is.",
    "You should really delete this post, seriously.",
    "Your logic is non-existent. Nice try, though!",
    "Thanks for wasting my time with this nonsense.",
    "Pathetic attempt at trying to be relevant.",
    "You clearly have no idea what you're talking about.",
    "Stop trolling and get a life!",
    "Why don't you take a break from the internet?",
    "Wow, this is a new level of ignorance.",
    "Just when I thought it couldn't get worse...",
    "Oh no, here comes another ridiculous opinion.",
    "You just embarrassed yourself in front of everyone.",
    "This is so laughably bad, I can't even.",
    "You're like a walking disaster of bad takes.",
    "Did you even read what you wrote before posting?",
    "How about you try using facts next time?",
    "This is beyond comprehension. Pure nonsense.",
    "It's amazing how wrong someone can be.",
    "You make the worst points and think you're clever.",
    "Nobody asked for your terrible opinion.",
    "This comment is trash, just like your logic.",
    "How do you manage to fail so consistently?",
    "You should probably quit while you're behind.",
    "Are you trying to set a new low with this post?",
    "Yikes! This is painful to read.",
    "You're not making any sense at all.",
    "Why does anyone bother replying to you?",
    "You're like a bad joke that never ends.",
    "I can't stop cringing at this comment.",
    "Please, just stop. You're embarrassing yourself.",
    "Your ignorance is showing, big time.",
    "Is this your idea of a smart response?",
    "You're not helping anyone with your comments.",
    "This is why no one takes you seriously.",
    "Wow, you really thought this was a good idea?",
    "I can't believe I'm wasting time on this.",
    "Are you trolling or just clueless?",
    "Every word you type is worse than the last.",
    "You just broke the internet with your stupidity.",
    "This is so wrong it actually hurts.",
    "What kind of logic are you even using?",
    "You're digging your own grave with this post.",
    "I've seen bad comments, but this takes the cake.",
    "Did you really think this would be a smart post?",
    "It's astonishing how wrong you are.",
    "You're just here for the attention, aren't you?",
    "I can't tell if this is satire or if you're serious.",
    "Your comment is the definition of cringe.",
    "Do everyone a favor and delete this.",
    "You're actually making my head hurt with this.",
    "This is a dumpster fire of bad opinions.",
    "It's like you're trying to be the worst person here.",
    "Please stop posting, you're embarrassing yourself.",
        "Wow, you really have no clue, do you?",
    "I can't believe you just typed that out loud.",
    "You're a prime example of why people don't comment anymore.",
    "You just took stupidity to a whole new level.",
    "This comment gave me second-hand embarrassment.",
    "I guess thinking before typing isn't your strong suit.",
    "Is there a prize for being this wrong?",
    "Just when I thought it couldn’t get worse, you posted this.",
    "Please take your negativity somewhere else.",
    "I genuinely feel dumber after reading that.",
    "You clearly missed the point by a mile.",
    "Your argument just collapsed under its own weight.",
    "This is exactly why we can't have nice things.",
    "I can’t believe anyone would take you seriously.",
    "You should probably fact-check before posting nonsense.",
    "If there was an award for worst comment, you’d win.",
    "You literally added nothing of value to this conversation.",
    "How do you come up with such terrible takes?",
    "I didn’t think anyone could be this wrong, but here you are.",
    "You’ve clearly never been right about anything, ever.",
    "Just admit you have no idea what you’re talking about.",
    "This post is exactly why we need better moderators.",
    "I don't think I've ever seen a worse comment.",
    "This isn't the flex you think it is.",
    "This must be a joke, because no one could be this wrong.",
    "You're clearly just here to stir the pot, aren't you?",
    "I could explain it to you, but I doubt you'd understand.",
    "How does it feel to be wrong all the time?",
    "You just proved everyone’s point about trolls.",
    "This comment belongs in a trash can, not online.",
    "It’s like you’re trying to be the worst version of yourself.",
    "You’ve just embarrassed yourself in front of the whole internet.",
    "Your comment is like a trainwreck – I can’t look away.",
    "You’ve achieved nothing but making everyone dumber.",
    "I’d respond with facts, but I don’t think you’d understand them.",
    "This is painful to read, even for a troll.",
    "You're like a walking encyclopedia of bad opinions.",
    "Thanks for confirming you have no idea what you’re talking about.",
    "Do you get joy from being consistently wrong?",
    "Your lack of self-awareness is truly impressive.",
    "This post is proof that trolls are getting worse.",
    "What you just posted makes absolutely no sense.",
    "Why don’t you try contributing something useful for once?",
    "You're not just wrong; you're impressively wrong.",
    "It’s amazing how wrong someone can be this consistently.",
    "If ignorance was a sport, you’d be a world champion.",
    "Congratulations, you just lost all credibility with that comment.",
    "This comment is what happens when you don’t think before posting.",
    "I don’t know whether to laugh or feel sorry for you.",
    "You should take a break from the internet for a while."
]

    # Randomly generate placeholders for the comment
    site = fake.domain_name()
    email = fake.email()
    username = fake.user_name()
    return random.choice(templates).format(username, site, email)

# Generate 1000 random comments and write to CSV
def generate_csv(filename='trolls.csv', num_entries=1000):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['ID', 'Date', 'Comment'])
        
        for i in range(1, num_entries + 1):
            comment_id = i
            date = fake.date_time_this_decade().strftime("%Y%m%d%H%M%S") + "Z"
            comment = generate_random_comment()
            writer.writerow([comment_id, date, comment])

# Generate the trolls.csv file with 1000 entries
generate_csv()
