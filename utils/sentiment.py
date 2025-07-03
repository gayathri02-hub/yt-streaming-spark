from textblob import TextBlob

def analyze_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

if __name__ == "__main__":
    print(analyze_sentiment("Great video! Loved the quality."))
    