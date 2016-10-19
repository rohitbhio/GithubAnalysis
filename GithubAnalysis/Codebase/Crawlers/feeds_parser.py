import feedparser
from bs4 import BeautifulSoup
import pprint
from dateutil import parser
import ConfigParser
import urllib2
import pandas as pd
from datetime import datetime, timedelta
import time
from glob import glob
import subprocess
import sys, os
import re
import logging
from logging.config import fileConfig
from BigQuery_config import *
from pdb import set_trace as trace

config = ConfigParser.ConfigParser()
config.read(SCRIPT_LOCATION + 'feeds_config.cfg')

def crawlRSS():
    """
    This method will crawl the RSS feeds of the sources configured and update configuration of the latest status
    """
    for source in config.sections():
        print "Pulling articles from %s"%source
        try:
            #latest = config[source]['latest']
            latest = config.get(source,'latest')
            print latest
            feed = feedparser.parse(config.get(source,'url'))
            posts = []
            for i in range(0,len(feed['entries'])):
                try:
                    if parser.parse(feed['entries'][i].published) > parser.parse(latest):
                        content = ""
                        if config.get(source,'feedburner') == "TRUE":
                            url = feed['entries'][i]['feedburner_origlink']
                            if config.get(source,'getcontent') == "TRUE":
                                content = BeautifulSoup(urllib2.urlopen(url + config.get(source,'addtolink')).read()).get_text()
                        else:
                            url = feed['entries'][i].link
                            if config.get(source,'getcontent') == "TRUE":
                                content = BeautifulSoup(urllib2.urlopen(url + config.get(source,'addtolink')).read()).get_text()
                        
                        content = content.strip('\n').encode('ascii','ignore').replace('\n','').replace('\'','')
                        content = BeautifulSoup(feed['entries'][i].summary).get_text().encode('ascii','ignore').replace('\n','').replace('\'','') + content  #Add 'content' as 2nd operand only
                        content = re.sub(' - High Scalability.*}  ','',content)
                                
                        posts.append({
                        'date_published': feed['entries'][i].published.encode('utf-8'),
                        'title': feed['entries'][i].title.strip('\n').encode('ascii','ignore').replace('\n','').replace('\'','').replace(',',' '),
                        'content': content.replace(',',' '),
                        'link': url.encode('utf-8').replace(',','COMMA')
                        })
                except:
                    print "There is an error parsing the post"

            ## *************** Writing in the csv file ******************** ##
            df = pd.DataFrame.from_dict(posts)
            if df.shape[0]!=0:
                df['date_published'] = pd.to_datetime(df['date_published'])
                df = df[['date_published','title','content','link']]
            if source+".csv" in [os.path.basename(x) for x in glob(DATA_LOCATION + "feeds/" + "*.csv")]:
                source_file = open(DATA_LOCATION + "feeds/" + source + '.csv', "a")
                df.to_csv(source_file, index=False, header=False, encoding='utf-8')
            else:
                source_file = open(DATA_LOCATION + "feeds/" + source + '.csv', "w+")
                df.to_csv(source_file, index=False, header=True, encoding='utf-8')
            source_file.close()
            #save latest timestamp in config file, next run will get only new values 
            config.set(source,'latest',feed['entries'][0].published)
        except Exception as e:
            print "There is an error running for : %s"%source
            print "Error :=======>", e

    #uncomment to allow configfile to save latest timestamp
    cfgfile = open(SCRIPT_LOCATION + 'feeds_config.cfg','r+')
    config.write(cfgfile)
    cfgfile.close()
    print "\n"
    return True

if __name__ == '__main__':
    success = crawlRSS()
