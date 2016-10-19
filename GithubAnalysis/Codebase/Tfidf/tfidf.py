from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import pandas as pd
import itertools
import re
import os,sys
import nltk
from nltk.tree import Tree
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from glob import glob
from datetime import datetime, timedelta, date
import time
from config import *
import subprocess
import logging
from logging.config import fileConfig


conf  = (SparkConf()
         .setAppName("TFIDF App") # Name of App
         .set("spark.executor.memory", "30g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

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
    

def writeResults(lis,docs,source):
    """
    This method writes the results.
    """
    docs = docs.map(lambda line: [line[0],line[1].encode('ascii','ignore'),line[3]])  ##To add the content to the output include line[2].encode('ascii','ignore') in the row
    docs_list = docs.collect()
    df = pd.DataFrame(docs_list[1:],columns=docs_list[:1][0])
    lis_df = pd.Series(lis[1:])
    df['top_words'] = lis_df
    df.to_csv(OUTPUT_LOCATION+'tfidf_result_'+source, index=False, encoding='utf-8')   ##Write TFIDF result for debugging purpose
    dashboard_pages = for_dashboard(df,source)
    return True

def readStopwords():
    """
    This method reads the stopwords from the file
    """
    stopwords = sc.textFile(HDFS_LOCATION1+"GithubAnalysis/TechTrends/feeds_working_set/stopwords.txt")
    stopwords_list = list(set(" ".join(stopwords.collect()).split(" ")))
    return stopwords_list

def cleanDoc(document,stopwords):
    """
    This method loads and cleans the data(other sources)
    """
    docs = document.map(lambda line: (line[1]+' '+line[2]).encode('ascii','ignore').lower())
    docs = docs.map(lambda line: line.split(" ")[:100])
    docs = docs.map(lambda line: " ".join(line).replace('//www.',' ').replace('.com/',' ').replace('www.',' ').replace('.com',' ').replace('.',' ').replace('/',' '))
    docs = docs.map(lambda line: re.sub(r'\d*pb|\d*ms|\d*cores|\d*servers|\d*milliseconds|\d*minutes|\d*years|\d*fixes|\d*petabytes|\d*bytes|\d*bits|\d*yrs|\d*cents|\d*hours|\d*hrs|\d*megs|\d*mins|\d*months|\d*seconds|\d*kbps|\d*weeks|v\d+', '', line))
    docs = docs.map(lambda line: re.sub(r'^-*|^_*|-*$|_*$|^/*|/*$|[+]*|[+]*\w*', '', line))
    docs = docs.map(lambda line: re.sub(r'\?|\!|:|;|<|>|\(|\)|\[|\]|\{|\}|\*|\~|\`|\%|\$|\^|\&|@|#|=|_|\\\\|[|]', ' ', line))
    docs = docs.map(lambda line: re.sub(r"'", " ", line))
    docs = docs.map(lambda line: re.sub(r'"', ' ', line))
    docs = docs.map(lambda line: re.sub(r'\b\d+\b|^[+]*', ' ', line))
    docs = docs.map(lambda line: line.replace('-',' '))
    docs = docs.map(lambda line: re.sub(r' +', ' ', line))   ##substitute multiple spaces with single space
    docs = docs.map(lambda line: [i.strip() for i in line.split(" ") if i not in stopwords and len(i)>=2])
    return docs

def posTag(text):
    """
    This method performs part-of-speech tagging to select only nouns.
    """
    pos_text = nltk.pos_tag(text)
    return [i[0] for i in pos_text if i[1].startswith('N')]  #N --> Noun and J--> Adjective   or i[1].startswith('J')

def calcTfidf(doc, source):
    """
    This method computes TF-IDF scores for the given document.
    While applying HashingTF only needs a single pass to the data, applying IDF needs two passes: first to compute the IDF vector and second to scale the term frequencies by IDF.
    """
    hashingTF = HashingTF(200000)
    tf = hashingTF.transform(doc)
    print "TF calculated for "+source.split('/')[-1]
    tf.cache()
    idf = IDF().fit(tf)  ##idf = IDF(minDocFreq=2).fit(tf)
    tfidf = idf.transform(tf)
    print "TF-IDF calculated for "+source.split('/')[-1]  
    return hashingTF, tfidf

def getTopWords(doc,hashingTF,tfidf,source):
    """
    This method parses the hashingTF and tfidf to get the mapping between words and its tf-idf value.
    """
    for_map = doc.map(lambda line: list(set(line)))    ##This will create list of words.
    taf_index = tfidf.map(lambda line: str(line).split(',[')[1].replace("]","").split(',')).collect()   ##later typecast to int
    taf_values = tfidf.map(lambda line: str(line).split(',[')[2].replace("])","").split(',')).collect()   ##later typecast to float
    
    lisTopWords = []
    for i, word_list in enumerate(for_map.collect()):
        temp = []
        for word in word_list:
            temp.append([word, float(taf_values[i][taf_index[i].index(str(hashingTF.indexOf(word)))])])
        temp.sort(key = lambda row: -row[1])   ##(-ve sign) to sort in decreasing order
        lisTopWords.append(posTag([word[0] for word in temp[:10]]))  ##To take only top 10 words
    print "Mapping between Top_words and TF-IDF established for "+source.split('/')[-1]    
    return lisTopWords

def funcMain(stopwords,source):
    """
    This method is the base method which calls other methods to -- load, clean the data; load stopwords; calculate TF-IDF; write the results.
    """
    documents = sc.textFile(source) #, minPartitions=14)
    docs = documents.filter(lambda line: 'Sponsored Post:' not in line).map(lambda line: line.split(","))
    try:
        doc = cleanDoc(docs,stopwords)
    except:
        print "Error while cleaning "+source.split('/')[-1]
        return False
    hashingTF, tfidf = calcTfidf(doc, source)
    
    try:
        lis = getTopWords(doc,hashingTF,tfidf,source)
    except:
        print "Error while getting the top words for "+source.split('/')[-1]
        return False
	    
    try:
        writeResults(lis,docs,source.split('/')[-1])
    except:
        print "Error while writing the result for "+source.split('/')[-1]
        return False

    print "Result written for "+source.split('/')[-1]
    return True

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
    print "Recency vs Frequency relationship established for top_words"
    return True    
    
if __name__ == "__main__":
    now = datetime.now()
    """ 
    copy_feeds = ["hadoop fs -cp -f " + HDFS_LOCATION1 + "GithubAnalysis/TechTrends/feeds " + HDFS_LOCATION1 + "GithubAnalysis/TechTrends/feeds_working_set", "hadoop fs -cp -f " + HDFS_LOCATION2 + "GithubAnalysis/TechTrends/feeds " + HDFS_LOCATION2 + "GithubAnalysis/TechTrends/feeds_working_set"]    
    for hadoop_copy_feeds in copy_feeds:
        try:
            subprocess.call(hadoop_copy_feeds, shell=True)
	    print "Feeds copied to the working set"
            break
        except OSError:
            continue
    
    stopwords = readStopwords()
    for source in ['high_scalablity.csv', 'regular_geek.csv', 'dzone.csv']: 
        print "Source in progress : ",source
	if funcMain(stopwords, HDFS_LOCATION1 + "GithubAnalysis/TechTrends/feeds_working_set/" + source):
            print "Finished processing for "+source+"\n\n\n"
        else:
    	    print "Error while processing "+source+"\n\n\n"         
            continue
    """
    success = recencyFrequencyCsv() ##Get the Frequency_recency sheet
    print "recency frequency computed"
    print "Time taken for TFIDF Model is ", datetime.now()-now
