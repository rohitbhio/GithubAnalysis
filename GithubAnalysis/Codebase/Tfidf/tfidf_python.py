import re
import string
import nltk
from nltk.tree import Tree
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import numpy as np
import glob
import collections
from sklearn.feature_extraction.text import TfidfVectorizer
from pdb import set_trace as trace
import time
import pandas as pd
import itertools
import os,sys
from datetime import datetime, timedelta, date
from config import *
import subprocess

Stemming = 'No'   ## Assign the value of stemming as 'Yes' if you choose to stem the words else 'No'
stemmer = PorterStemmer()

#************ Process stopwords in global space ***********#
stopwords = ''
with open(DATA_LOCATION + 'stopwords.txt','r') as f:
    stopwords = f.read().splitlines()

stopwords = str(stopwords)
stopwords = stopwords.replace("'","").replace("[","").replace("]","").replace("\\t"," ").replace("/"," ").replace("."," ")

stopwords_new = nltk.word_tokenize(stopwords)
stopwords_new= list(set(stopwords_new))
print "Stopwords processing done"
#***********************************************************#

def recencyFrequencyCsv():
    """
    This method computes the recency and frequency relationship for the top_words.
    """
    df = pd.DataFrame()
    for source in ['dashboard_2ndpage_high_scalablity.csv', 'dashboard_2ndpage_regular_geek.csv', 'dashboard_2ndpage_dzone.csv']: 
        temp = pd.read_csv(OUTPUT_LOCATION + source, encoding='utf-8')
        tp = temp.sort('date_published',ascending=False).groupby('top_words').first().reset_index()[['top_words','date_published']].sort('top_words')
        tp['frequency'] = temp.groupby('top_words').size().reset_index().sort('top_words')[0]
        tp['source'] = re.search('dashboard_2ndpage_(.+?).csv', source).group(1)
        df = df.append(tp)
    df.to_csv(OUTPUT_LOCATION+'recencyFrequency.csv',index = False, encoding='utf-8')  ##Dump it to output location
    #debug_log.info("Recency vs Frequency relationship established for top_words")
    return True

def getPairs(words):
    """
    This method is useful to get the pair of (top_words, serach_results).
    """
    items = words.top_words
    return [[i,j] for i in items for j in items if i!=j]

def freqSearch(df_new, df_train, output_file_name):
    """
    This method gets the frequency and search results for all the top_words.
    """
    df_train = df_train[df_train['top_words'].str.len()!=0].reset_index(drop=True)  ##remove articles with empty top_words list.
    if output_file_name == 'dzone':
        df_train = df_train[df_train['date_published']>='2014-01-01']
        df_new = df_new[df_new['date_published']>='2014-01-01']        
    unique_words = df_new.groupby(['top_words', 'date_published'],sort=True).first()
    unique_words = unique_words.reset_index()
    
    search_list = []
    for index, row in unique_words.iterrows():
        lis = map(lambda line: [x for x in line if row['top_words'] in line and x!=row['top_words']], df_train['top_words'])
        top = list(itertools.chain(*lis))
        search_list += [[row['top_words'],row['date_published'], word.strip()] for word in top]
    search = pd.DataFrame(search_list, columns = ['top_words','date_published','search_results'])
        
    frequency = search.groupby(['top_words', 'date_published', 'search_results']).size().reset_index()
    frequency.columns = ['top_words', 'date_published', 'search_results', 'frequency']
    counts = frequency.groupby('top_words').size().reset_index()
    frequency_final =  pd.merge(frequency, counts, how='left')
    frequency_final = frequency_final.sort(['date_published', 0], ascending=[0, 0])
    frequency_final[['top_words', 'date_published', 'search_results', 'frequency']].to_csv(OUTPUT_LOCATION+'dashboard_search_results_frequency_'+output_file_name+'.csv',index=False, encoding='utf-8')
    return 'Success'

def combineGroupElements(g,column):
    """
    This method helps in concatenating the rows of the dataframe with appropriate joiners.
    """
    s = []
    s += [each for each in g[column] if pd.notnull(each)]
    return s

def for_dashboard(df_train,source):
    """
    This method stores the elementary transformation results in two files.
    """
    df_train = df_train[pd.notnull(df_train['date_published'])]  ##To remove the rows with NaN date_published
    df_train['date_published'] = pd.to_datetime(df_train['date_published'])
    output_file_name = re.search('(.+?).csv', source).group(1)
    final = pd.DataFrame()
    for i in range(0,30):
        benchmark_date = date.today() - timedelta(days = 1)
        df = df_train[((df_train['date_published'] > benchmark_date+pd.DateOffset(months=(-6),days=(-i))) & (df_train['date_published'] <= benchmark_date+pd.DateOffset(days=(-i))))]
        df_new = pd.DataFrame([(row['date_published'], d.strip()) for index, row in df.iterrows() for d in row['top_words']])
        if df_new.empty:
            continue        
        df_new.columns=['date_published','top_words']
        df_new['score'] = (benchmark_date+pd.DateOffset(days=(-i)) - df_new['date_published']).astype('timedelta64[D]')
        df_new['score'] = (180-df_new['score'])/180
        df_new['no_of_links'] = 1
        df_new = df_new[['top_words','score','no_of_links']] ###include link if you want'link' to get all links
        groups = df_new.groupby(df_new['top_words'])
        groupFun = groups.apply(lambda g: pd.Series([combineGroupElements(g,col) for col in g.columns],index=g.columns))
        temp = groupFun.drop('top_words',1).reset_index()
        
        temp['score'] = map(lambda line: sum(line),temp['score'])
        temp['no_of_links'] = map(lambda line: sum(line),temp['no_of_links'])
        temp['benchmark_date'] = benchmark_date+pd.DateOffset(days=(-i))
        final = final.append(temp)

    final.to_csv(OUTPUT_LOCATION+'dashboard_1stpage_'+output_file_name+'.csv', index=False, encoding='utf-8')   ##Dump it to output location
    df_new = pd.DataFrame([(row['date_published'], row['title'], row['link'], d.strip()) for index, row in df_train.iterrows() for d in row['top_words'] if row['top_words']], columns=['date_published','title','link','top_words'])
    page2 = df_new.sort(['top_words','date_published'])
    page2['rank_first_seen'] = page2.groupby(page2['top_words'])['date_published'].rank(method='min')  ##First seen ranks
    page2['score'] = page2.apply(lambda x: float(1)/((datetime.now()-pd.to_datetime(x['date_published'])).days), axis=1)  ##score based on latest occurencefillna(0).
    page2.to_csv(OUTPUT_LOCATION+'dashboard_2ndpage_'+output_file_name+'.csv', index=False, encoding='utf-8')  ##Dump it to output location
    freq_success = freqSearch(df_new, df_train, output_file_name)
    return 'Success'


def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        if Stemming == 'Yes':
            stemmed.append(stemmer.stem(item))
        else:
            stemmed.append(item)
    return stemmed

def tokenize(text):
    tokens = nltk.word_tokenize(text)
    filtered_words = [str(w.encode('utf-8')) for w in tokens]
    filtered_words = [w for w in tokens if not w in stopwords_new and len(w)> 1]
    stems = filtered_words
    stems = stem_tokens(filtered_words, stemmer)
    return stems

def pos_tag(text):
    tokens  = nltk.word_tokenize(text)
    pos_text = nltk.pos_tag(tokens)
    return " ".join([i[0] for i in pos_text if i[1].startswith('N')])

def top_n(tfs,feature_names):
    new_list = []
    Top_Words = {}
    counter = 0
    for x in tfs:
        x = x.todense()
        new_old = np.array(x).flatten().tolist()
        new = np.array(x).flatten().tolist()
        new.sort(reverse=True)
        feature_ind = [i for i,j in enumerate(new_old) if j in new[0:10] and j > 0]  ## Here 10 signifies the number of top words
        feature_set = [j for i,j in enumerate(feature_names) if i in feature_ind]
        new_list = new_list + feature_set

        ##**************** Keep only the Noun features ************
        pos_tags = nltk.pos_tag(feature_set)
        feature_set = [i[0] for i in pos_tags if i[1].startswith('N')]

        ##**************** Remove words ending with 'ing' and 'ity' ***************
        Top_Words[counter] = [str(feature) for feature in feature_set if (feature.find('ing')<0 and feature.find('ity')<0)]
        counter = counter + 1
    return new_list,Top_Words
    
def calc_tfidf(filename):
    t1 = time.time()
    count = 0
    token_list = []
    content_other = pd.read_csv(filename)
    for index, file in content_other.iterrows():
        text_read = str(file['title']).replace('\xa0',' ')+' '+ str(file['content'])
        text_read  = text_read.split()
        lowers = ' '.join(text_read[:100]).lower()
        lowers = lowers.replace('//www.',' ').replace('.com/',' ').replace('www.',' ').replace('.com',' ')
        lowers = lowers.replace('.',' ').replace('/',' ')
        lowers = re.sub(r'^-', '', lowers)
        lowers = re.sub(r'^_', '', lowers)
        lowers = re.sub(r'-$', '', lowers)
        lowers = re.sub(r'_$', '', lowers)
        lowers = re.sub(r'^/', '', lowers)
        lowers = re.sub(r'/$', '', lowers)
        lowers = lowers.replace('-',' ')
        lowers = lowers.decode('ascii', 'ignore')
        lowers = [i.strip() for i in lowers.splitlines()]
        lowers = " ".join([i for i in lowers if len(i)>=2])
        token_list.append(lowers)
        count+=1
        if(count%20000==0):
            print count

    tfidf = TfidfVectorizer(tokenizer=tokenize, stop_words='english')
    tfs = tfidf.fit_transform(token_list)
    t2 = time.time()
    print t2 - t1
    #tfs = tfidf.fit_transform(token_dict.values())
    feature_names = tfidf.get_feature_names()

    new_list,Top_Words = top_n(tfs,feature_names)

    #**************Write the output to the csv file ***********************#
    final_lst=[]
    for index, row in content_other.iterrows():
        lst = []
        lst.extend([row['date_published'],row['title'],row['link'],Top_Words[index]])
        final_lst.append(lst)
    file_df = pd.DataFrame(final_lst, columns=['date_published', 'title', 'link', 'top_words'])    
    file_df.to_csv(OUTPUT_LOCATION + 'tfidf_result_' + filename.split('/')[-1], index=False, encoding='utf-8')
    dashboard_pages = for_dashboard(file_df, filename.split('/')[-1])
    return "Top words calculated for "+filename.split('/')[-1]

if __name__ == "__main__":
    now = datetime.now()
    print now
    #remove files from feeds directory
    remove_files = "rm -rf " + DATA_LOCATION + "*"
    os.system(remove_files)
    print "Feeds files removed"
    #copy from hdfs
    copy_from_hdfs = "hadoop fs -get " + HDFS_LOCATION1 + "GithubAnalysis/TechTrends/feeds/* " + DATA_LOCATION
    os.system(copy_from_hdfs)
    print "Feeds files copied from HDFS" 
    
    for filename in glob.glob(DATA_LOCATION + '*.csv'):  ##Give the path where data files are located
        #filename = filename.replace('\\','/')   ##This step is required if you wish to run the script on both windows and unix systems.
	to =  datetime.now()
        print calc_tfidf(filename)
	print "time taken for "+filename.split('/')[-1]+" is: ", datetime.now()-to
    print "Time taken for TFIDF Model is ", datetime.now()-now
    success = recencyFrequencyCsv() ##Get the Frequency_recency sheet
    print "recency frequency computed"
