from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.manifold import TSNE
from itertools import combinations
from typing import List
import numpy as np
import pandas as pd
import spacy
from spacy.tokens import Doc


def calculate_odds(freq1, freq2, type, freq_count_1):
    """
    this function does that.
    """

    if freq2 == 0:
        if freq1 == 0:
            return 0
        else:
            if type == "C":
                return freq1 / freq_count_1
            else:
                return freq1 / freq_count_1

    else:
        if freq1 == 0:
            return 0
        else:
            return freq1 / freq2


def map_rank_to_bin(df, column):
    num_bins = 20
    le = int(len(df) / num_bins)
    column = int(column)

    for i in range(1, num_bins + 1):
        if (column > (i - 1) * le) and (column <= i * le):
            return i

    # If column is greater than the maximum rank, assign it to the last bin
    return num_bins


def word_to_vec_dsl(word, word_vec_model):
    try:
        return word_vec_model.word_vec(word)

    except KeyError:
        return None


def choose_odds_percentiles(percentile, df_C, df_NC, word_vec_model):
    unique_words_above_1_C_percentile = df_C[df_C["odds_C_percentile"] == percentile]
    unique_words_above_1_NC_percentile = df_NC[
        df_NC["odds_NC_percentile"] == percentile
    ]

    unique_words_above_1_C_percentile[
        "word_vec_dsl"
    ] = unique_words_above_1_C_percentile.apply(
        lambda row: word_to_vec_dsl(row["word"], word_vec_model), axis=1
    )
    unique_words_above_1_NC_percentile[
        "word_vec_dsl"
    ] = unique_words_above_1_NC_percentile.apply(
        lambda row: word_to_vec_dsl(row["word"], word_vec_model), axis=1
    )

    unique_words_above_1_C_percentile = unique_words_above_1_C_percentile[
        unique_words_above_1_C_percentile["word_vec_dsl"].notna()
    ]
    unique_words_above_1_NC_percentile = unique_words_above_1_NC_percentile[
        unique_words_above_1_NC_percentile["word_vec_dsl"].notna()
    ]

    unique_words_above_1_C_percentile["label"] = "C"
    unique_words_above_1_NC_percentile["label"] = "NC"

    unique_words_above_1_C_percentile_combined = pd.concat(
        [unique_words_above_1_C_percentile, unique_words_above_1_NC_percentile]
    )
    return unique_words_above_1_C_percentile_combined


def k_means(df, nclusters, perplex):
    wordvectors = df["word_vec_dsl"].to_numpy()
    wordvectors = np.array([np.array(arr) for arr in wordvectors])
    scaler = StandardScaler()
    wordvectors = scaler.fit_transform(wordvectors)

    kmeans = KMeans(n_clusters=nclusters, random_state=42)

    tsne = TSNE(n_components=2, perplexity=perplex, random_state=42)
    embedded_data = tsne.fit_transform(wordvectors)
    df["embedded_data1"] = [e[0] for e in embedded_data]
    df["embedded_data2"] = [e[1] for e in embedded_data]

    kmeans.fit(embedded_data)
    df["k_means_group_tsne"] = kmeans.labels_
    return df


def cosine_similarities(df, group, word_vec_model):
    try:
        cosine_similarities = [
            (
                current_word,
                next_word,
                word_vec_model.similarity(current_word, next_word),
            )
            for current_word, next_word in combinations(
                df[df["label"] == group]["word"], 2
            )
        ]
        return np.mean([e[2] for e in cosine_similarities])

    except KeyError:
        return 0


def tokenize_text(df):
    nlp = spacy.load("da_core_news_sm")
    exception_list = ["CO2", "co2"]

    def preprocess_text(doc: Doc) -> List[str]:
        processed_tokens = [
            token.text.lower()
            for token in doc
            if not token.is_stop and token.is_alpha or token.text in exception_list
        ]
        return str(processed_tokens)

    # Use spaCy's pipe method to process text in batches
    texts = df.speech_item_text.values.tolist()
    docs = list(nlp.pipe(texts, n_process=16))  # Use multiprocessing

    # Apply the text processing functions
    df["speech_item_text"] = [preprocess_text(doc) for doc in docs]
    df.rename(columns={"speech_item_text": "speech_item_tokenized"}, inplace=True)
    return df


def vectorize_speech(tokens, word_vec_model):
    vec_representation = np.zeros((500))
    for token in tokens:
        vec = word_vec_model.word_vec(token)
        vec_representation += vec / len(tokens)
    return vec_representation


def add_features(df, C_word_set, NC_word_set, g_word_set, word_vec_model):
    count_list = []
    speech_list = []
    word_vec_list = []

    tokenized_list = df.speech_item_tokenized.to_list()
    for i in tokenized_list:
        C_counts = 0
        NC_counts = 0
        average_vec = 0

        tokenized = eval(i)
        token_len = len(tokenized)
        for word in tokenized:
            if word in g_word_set:
                token_len -= 1
            else:
                if word in C_word_set:
                    C_counts += 1
                elif word in NC_word_set:
                    NC_counts += 1

                try:
                    vec = word_vec_model.get_vector(word)
                    average_vec += vec
                except:
                    token_len -= 1

        count_list.append([C_counts, NC_counts, token_len])
        speech_list.append(tokenized)
        if token_len != 0:
            average_vec = average_vec / token_len
        else:
            average_vec = np.zeros((500,), dtype=int)
        word_vec_list.append(average_vec)

    df[["C_counts", "NC_counts", "num_tokens"]] = count_list
    df["speech_item_tokenized"] = speech_list

    df["C_percent"] = 100 * df["C_counts"] / df["num_tokens"]
    df["NC_percent"] = 100 * df["NC_counts"] / df["num_tokens"]

    df["average_vec"] = word_vec_list
    return df


    def add_custom_features(df, C_word_set, word_vec_model):
    count_list = []
    speech_list = []
    word_vec_list = []

    df["speech_item_tokenized"] = df["speech_item_tokenized"].apply(lambda x: eval(x))
    tokenized_list = df["speech_item_tokenized"].to_list()
    for i in tokenized_list:
        C_counts = 0
        C_words = []
        average_vec = 0

        tokenized = i
        for word in tokenized:
            if word in C_word_set:
              C_counts += 1
              C_words.append(word)
              try:
                vec = word_vec_model.get_vector(word)
                average_vec += vec
              except:
                C_counts -= 1
                C_words.pop()

        count_list.append(C_counts)
        speech_list.append(C_words)
        if C_counts != 0:
            average_vec = average_vec / C_counts
        else:
            average_vec = np.zeros((500,), dtype=int)
        word_vec_list.append(average_vec)

    df["C_counts"] = count_list
    df["C_words"] = speech_list

    df["average_vec_C"] = word_vec_list
    return df


def calculate_custom_odds(freq1, freq2, freq_count_1):
    """
    this function does that.
    """
    if freq2 == 0:
        if freq1 == 0:
            return 0
        else:
            return freq1 / freq_count_1
    else:
        if freq1 == 0:
            return 0
        else:
            return freq1 / freq2