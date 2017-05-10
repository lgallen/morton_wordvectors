#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  4 06:54:31 2017

@author: lucas
"""

import pandas as pd
import numpy as np
from gensim import utils
from gensim.models.word2vec import LineSentence
from gensim.models import Word2Vec
import gensim
import re
import string


VECTOR_SIZE = 300
ITER = 15  # epochs to iterate over training data
WORKER_COUNT = 3  # number of parallel processes
PRETRAINED_EMB = "./private_data/GoogleNews-vectors-negative300.bin"


def remove_links(text):
    """Remove any links from a string, as defined by substrings
    beginning with 'http'
    :text: string to clean
    """
    text = re.sub(r'http\S+', '', str(text))
    return(text)


def remove_punctuation(text):
    '''Removes punctuations from a string
    :text: string to clean
    '''
    exclude = set(string.punctuation)
    text = ''.join(ch for ch in text if ch not in exclude)
    return(text)


def preprocess_profiles(profiles_df, text_col, language_col='language'):
    """Preprocesses profiles dataframe
    :profiles_df: dataframe of twitter profiles with text column
    to preprocess and language column
    """
    profiles_df[text_col] = profiles_df[text_col].apply(
            lambda x: remove_links(x))
    profiles_df[text_col] = profiles_df[text_col].apply(
            lambda x: remove_punctuation(x))
    return(profiles_df)


def create_sentences(profiles_df, text_col):
    """Given a dataframe of documents, return sentences for a word2vec model
    :profiles_df: Pandas dataframe containing the text to model with
    :text_col: Name of column in dataframe containing text
    """
    sentences = profiles_df[text_col].apply(
            lambda x: utils.simple_preprocess(x))
    return(sentences)


def create_wordvec_model(profiles_df, text_col="text"):
    """Returns doc2vec model given dataframe of twitter profiles
    :profiles_df: dataframe of twitter profiles with columns of
    twit_id, language, and description
    """
    profiles_df = preprocess_profiles(profiles_df, text_col)
    sentences = create_sentences(profiles_df, text_col)
    model = gensim.models.Word2Vec(sentences, size=VECTOR_SIZE,
                                   iter=ITER, workers=WORKER_COUNT)
    return(model)
