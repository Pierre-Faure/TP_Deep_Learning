
def preprocess(sentence):
    import nltk
    from nltk.tokenize import RegexpTokenizer
    from nltk.stem import WordNetLemmatizer, PorterStemmer
    from nltk.corpus import stopwords
    import re

    #nltk.download('wordnet')
    #nltk.download('omw-1.4')
    #nltk.download('stopwords')
    lemmatizer = WordNetLemmatizer()
    stemmer = PorterStemmer()

    sentence = str(sentence)
    # Lowercase text
    sentence = sentence.lower()
    # Remove whitespace
    sentence = sentence.replace('{html}', "")
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', sentence)
    # Remove weblinks
    rem_url = re.sub(r'http\S+', '', cleantext)
    # Remove numbers
    rem_num = re.sub('[0-9]+', '', rem_url)
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(rem_num)
    filtered_words = [w for w in tokens if len(w) > 2 if not w in stopwords.words('english')]
    stem_words=[stemmer.stem(w) for w in filtered_words]
    lemma_words=[lemmatizer.lemmatize(w) for w in stem_words]

    return " ".join(filtered_words)
