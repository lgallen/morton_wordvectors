#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  4 06:59:55 2017

@author: lucas
"""
import pandas as pd
import numpy as np
import gensim


def vectorize_tweet(tweet, model, keyed_vec):
    """Given the text of a new tweet, returns an average word embedding.
    :tweet: string, contents of a tweet
    :model: word2vec model
    :keyed_vec: boolean, is the model a keyed vector
    """
    tweet = gensim.utils.simple_preprocess(tweet)
    embedding_list = []
    for word in tweet:
        try:
            if keyed_vec:
                embedding_list.append(model.word_vec(word))
            else:
                embedding_list.append(model.wv[word])
        except:
            pass
    embedding_average = np.average(np.array(embedding_list), axis=0)
    return(embedding_average)


def create_word_embeddings(df, model, keyed_vec, text_col='text'):
    """Creates a new dataframe of word embeddings from a dataframe
    with a text column
    :df: dataframe with text column to vectorize
    :model: word2vec model
    :keyed_vec: boolean, is the model a keyed vector
    :text_col: dataframe to vectorize
    """
    embeddings = df[text_col].apply(
            lambda x: vectorize_tweet(x, model, keyed_vec))
    embeddings = pd.DataFrame(np.array(list(embeddings)))
    new_column_names = ["wv" + str(col) for col in embeddings.columns]
    embeddings.columns = new_column_names
    return(embeddings)


def append_word_vector_cols(df, model, keyed_vec=False, text_col='text'):
    """Adds new columns to a dataframe with word embeddings
    :df: dataframe with text column to vectorize
    :text_col: dataframe to vectorize
    :model: word2vec model
    :keyed_vec: boolean, is the model a keyed vector
    """
    embeddings = create_word_embeddings(df, model, keyed_vec, text_col)
    appended_df = pd.concat([df, embeddings], axis=1)
    return(appended_df)


def vectorized_cosine_similarity(df, row, first_col='wv0', last_col='wv299'):
    """Uses numpy to return cosine similarity for every word vector with no loops
    :df: dataframe
    :row: entry to check similarity
    :first_col: first word vector column
    :last_col: last word vector column
    """
    matrix = df.ix[:, first_col:last_col]
    matrix = np.array(matrix)
    current_vector = matrix[row, :]
    rowmask = np.array(range(matrix.shape[0])) != row
    other_vectors = matrix[rowmask]
    numerator = np.dot(other_vectors, current_vector)
    current_vector_l2 = np.sqrt(np.sum(current_vector**2))
    other_vectors_l2 = np.sqrt(np.sum(other_vectors**2, axis=1))
    denominator = other_vectors_l2 * current_vector_l2
    similarities = numerator / denominator
    return(similarities)


def most_similar_one_class(df, hour, hour_col="hour", text_col='text'):
    """Given a class period, find another student with the most similar tweet
    :df: dataframe with tweets already embedded
    :hour: class period to check
    :hour_col: name of class period column
    """
    best_score = 0
    for row in df.index[df['hour'] == hour]:
        current_student = vectorized_cosine_similarity(df, row)
        best_position = np.argmax(current_student)
        score = current_student[best_position]
        if score > best_score:
            current_hour_index = row
            current_tweet = df[text_col][row]
            best_score = score
            best_index = df.index[df.index != row][best_position]
            best_tweet = df[text_col][best_index]
    return(current_hour_index, current_tweet, best_index,
           best_score, best_tweet)
