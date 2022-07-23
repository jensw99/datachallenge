import pyexasol
import pandas as pd
import re
from germansentiment import SentimentModel
import demoji
from deep_translator import GoogleTranslator
import spacy


conn = pyexasol.connect(dsn='192.168.56.101:8563', user='sys', password='exasol')
translator = GoogleTranslator(source='auto', target='de')
all_emojis = dict()
nlp = spacy.load('de_core_news_sm')
model = SentimentModel()


def replace_emojis(text):
    """
    Replaces emojis with german text representation
    :param text: The text where to replace the emojis
    :return: Pure text without emojis
    """
    emojis = demoji.findall(text)
    for key in emojis:
        if key not in all_emojis:
            # Translate english text to german
            all_emojis[key] = translator.translate(emojis[key]).lower().replace("gesicht", "")
        emojis[key] = all_emojis[key]
    for key, value in emojis.items():
        text = text.replace(key, value)
    return text


def clear_text(text):
    """
    Replaces emojis, tokenizes the text and replaces @ mentions etc
    :param text: The text to be cleared
    :return: Cleared text
    """
    text = replace_emojis(text)
    text = " ".join([token.text for token in nlp.tokenizer(text)])
    text = " ".join(re.sub("(@[A-Za-z0-9_üöäÜÖÄß]+)|([^0-9A-Za-z \tüöäÜÖÄß])|(\w+:\/\/\S+)", "", text).split(" "))
    return text


def sentiment_analyse(list_with_sentences):
    """
    Performs the sentiment analysis
    :param list_with_sentences: The list of sentences
    :return: The list of ratings for each sentence
    """
    results = {
        "negative": -1.0,
        "positive": 1.0,
        "neutral": 0.0
    }
    r = list(map(lambda x: results[x], model.predict_sentiment(map(clear_text, list_with_sentences))))

    return sum(r)/len(r)

# Get all unrated posts
df = conn.export_to_pandas('SELECT * FROM DATACHALLENGE.DATACHALLENGE d WHERE RATING IS NULL')

df = df[["ID", "LIKES", "REPLIES", "SOCIAL_MEDIA", "RATING", "CREATED_AT", "TEXT", "COMPANY"]]
df = df.drop_duplicates(subset=['ID', 'COMPANY'])

df_bahn = df[(df.COMPANY == 'bahn') & df.TEXT.str.contains('ticket|fahre|fährt|fuhr|verzichten|verspätung|verspätet|pünktlich', flags=re.IGNORECASE, regex=True)]
df_bahn = df_bahn[df_bahn.TEXT.str.contains('DB|Bahn', flags=re.IGNORECASE, regex=True)]
df_ryanair = df[(df.COMPANY == 'ryanair') & df.TEXT.str.contains('ticket|fliegen|fliege|flog|verzichten|verspätung|pünktlich', flags=re.IGNORECASE, regex=True)]
df_ryanair = df_ryanair[df_ryanair.TEXT.str.contains('ryanair', flags=re.IGNORECASE, regex=True)]
df_auto = df[(df.COMPANY == 'auto') & df.TEXT.str.contains('stau|halten|fahre|fährt|fuhr|verzichten|verspätung|pünktlich', flags=re.IGNORECASE, regex=True)]
df_auto = df_auto[df_auto.TEXT.str.contains('auto', flags=re.IGNORECASE, regex=True)]
df_flixbus = df[(df.COMPANY == 'flixbus') & df.TEXT.str.contains('ticket|fahre|fährt|fuhr|verzichten|verspätung|verspätet|pünktlich', flags=re.IGNORECASE, regex=True)]
df_flixbus = df_flixbus[df_flixbus.TEXT.str.contains('flixbus', flags=re.IGNORECASE, regex=True)]
df_oeffis = df[(df.COMPANY == 'oeffis') & df.TEXT.str.contains('ticket|fahre|fährt|fuhr|verzichten|verspätung|verspätet|pünktlich', flags=re.IGNORECASE, regex=True)]
df_oeffis = df_oeffis[df_oeffis.TEXT.str.contains('öpnv|öffis|nahverkehr', flags=re.IGNORECASE, regex=True)]

df = pd.concat([
    df_bahn,
    df_ryanair,
    df_auto,
    df_flixbus,
    df_oeffis
])

# Split text into sentences
sentences = df["TEXT"].apply(lambda x: re.split("[\;\?\!\.]+\ |\n", x.lower()))
df["SPLITTED_TEXT"] = [[x for x in sentence if x] for sentence in sentences]

# Rate each sentence, sum and devide by number of sentences
df["RATING"] = [sentiment_analyse(text_with_sentences) for text_with_sentences in df["SPLITTED_TEXT"]]

df = df[["ID", "LIKES", "REPLIES", "SOCIAL_MEDIA", "RATING", "CREATED_AT", "TEXT", "COMPANY"]]
# Mark the posts as rated
conn.execute('UPDATE DATACHALLENGE.DATACHALLENGE SET RATING = -2')
# Store them in different table
conn.import_from_pandas(df, ('DATACHALLENGE', 'DATACHALLENGE_RATED'))
df.to_csv('classified.csv')

