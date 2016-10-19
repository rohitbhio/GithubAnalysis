import requests
import os.path
import json
import math
from datetime import datetime
import time
import traceback
from pdb import set_trace as trace
from multiprocessing import Pool
import pandas as pd
from glob import glob
import re
import nltk
from post_processing_config import *
import logging
from logging.config import fileConfig
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

def getStopwords():
    """
    This method is useful to get the stopwords list 
    """
    stopwords = ''
    #with open(STOPWORDS_LOCATION+'stopwords.txt','r') as f:
        #stopwords = f.read().splitlines()
    with open(STOPWORDS_LOCATION + 'stopwords.txt','r') as f:
        stopwords = f.read().splitlines()
    stopwords = str(stopwords)
    stopwords = stopwords.replace("'","").replace("[","").replace("]","").replace("\\t"," ").replace("/"," ").replace("."," ")
    stopwords_new = nltk.word_tokenize(stopwords)
    stopwords_new= list(set(stopwords_new))
    print "Stopwords processing done"
    return stopwords_new
    
def processDF(df_train,stopwords):
    """
    This method processes repository data to get repository-repository relationship based on search.
    """
    df_train['other_repo_count'] = 0
    df_train['other_repos'] = ''
    for i in range(df_train.shape[0]):
        key = str(df_train['full_name'][i].encode('ascii','ignore')).split('/')[1].lower()
        if ((len(key)<=2) or (key in stopwords)):
            continue
        search = df[df.description.str.contains(key)]['repository_name']  #.str.encode('utf-8').str ##include 'repository_name' to get only names else 'full_name'
        df_train['other_repos'][i] = [''.join(d) for d in search]  ##search.apply(','.join)
        df_train['other_repo_count'][i] = df.description.str.contains(key).sum()

    df_train = df_train[pd.notnull(df_train['other_repo_count'])]
    final = df_train[['full_name','repository_name','created_at','description','other_repo_count','other_repos']]

    #final.to_csv('repo_names_in_other_repos.csv',encoding='utf-8',index=False)  ##Wrapped version
    ##Unwrapping
    search = pd.DataFrame([(row['full_name'],row['created_at'], d.strip()) for index, row in final.iterrows() for d in row['other_repos']])    
    search.columns = ['full_name','created_at','repository_name']
    frequency = search.groupby(['full_name','repository_name','created_at']).size().reset_index()
    frequency.columns = ['repo1','repository_name','created_at','frequency']

    counts = frequency.groupby('repo1').size().reset_index()
    frequency_final =  pd.merge(frequency,counts,how='left')
    frequency_final = frequency_final.sort([0, 'frequency'],ascending=False)

    ##******To include the repo1 repo2 mapping*******##
    fre=  pd.merge(frequency_final,df_train[['full_name','repository_name']],how='left') ##joining to get full_name vs full_name file.
    fre.columns = ['repo1','repository_name','created_at','frequency', 'sort', 'repo2']
    fre = fre[fre['repo1']!=fre['repo2']]
    fre = fre.sort(['created_at','repo1','repository_name'],ascending=[0,1,1])    
    fre[['repo1','repository_name','created_at','frequency', 'repo2']].to_csv(OUTPUT_LOCATION+'repo_repo_github_graph.csv',encoding='utf-8',index=False) ##Unwrapped version
    print "Results written"
    return True

def recency(df):
    """
    This method gets the recency frequency relationship for the repositories.
    """    
    df = df[pd.notnull(df['owner_type'])]
    df = df[['owner_type','full_name','watchers_count','created_at']]
    df.to_csv(OUTPUT_LOCATION+'project_recency.csv',index=False)
    return True

def repoDetailsApiCall(args):
    """
    This method makes API calls to github to get details for a repository.
    """
    repo, clientid, clientsecret = args
    for i in range(0,4):
        try:
            url = 'https://api.github.com/repos/'+repo[0]+'?client_id='+clientid+'&client_secret='+clientsecret
            r = requests.get(url)
	    if(r.ok):
                repoItem = json.loads(r.text or r.content)
                name =  repoItem['name']
		owner_login = repoItem['owner']['login']            
                owner_type = repoItem['owner']['type']            
                try:
                    description = repoItem['description'].encode('utf-8').replace(',',' ').replace("'"," ").replace('"',' ')
                except:
                    description = ""
                #created_at = datetime.strptime(repoItem['created_at'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
                size = repoItem['size'] if 'size' in repoItem else ''
                language = repoItem['language'] if 'language' in repoItem else ''
                watchers_count = repoItem['watchers_count'] if 'watchers_count' in repoItem else ''
                forks_count= repoItem['forks_count'] if 'forks_count' in repoItem else ''
                open_issues_count  = repoItem['open_issues_count'] if 'open_issues_count' in repoItem else ''
                default_branch =  repoItem['default_branch']  if 'default_branch' in repoItem else ''

                return DELIMETER.join([owner_login, name, repo[0], owner_type, str(description), repo[1], str(size), language, str(watchers_count), str(forks_count), str(open_issues_count), default_branch, repo[2]])
            else:
                return DELIMETER.join(['', '', repo[0], '', '', repo[1], '', '', '', '', '', '', repo[2]])
        except Exception as e:
            if i>=1:
                return DELIMETER.join(['', '', repo[0], '', '', repo[1], '', '', '', '', '', '', repo[2]])
            else:
                continue

def getRepoInfo():
    """
    This method calls repoDetailsApiCall() method for all the repositories.
    """
    total_requests = 5000
    requests_per_resource = 2
    calls_per_token  = int(total_requests/requests_per_resource)
    num_tokens = 1
    
    model_result = pd.DataFrame()
    for source in glob(MODEL_OUTPUT_LOCATION+'*.csv'):    
        print "getRepoInfo().source : ",source
        result = pd.read_csv(source)
        model_result = model_result.append(result[['Model_score','repo_owner','repo_name','repo_created_at']])
    
    #repo_full_name = model_result[model_result['Model_score']>=MODEL_CUTOFF].apply(lambda x:([x['repo_owner']+'/'+x['repo_name'],str(x['Model_score'])]),axis = 1).values.tolist()
    repo_full_name = model_result[model_result['Model_score']>=MODEL_CUTOFF].apply(lambda x:([x['repo_owner']+'/'+x['repo_name'],str(x['repo_created_at'])[0:10],str(x['Model_score'])]),axis = 1).values.tolist()
    repo_info_df = pd.DataFrame()
    step = 500
    l1, l2 = 0, step
    print "Total number of repos : ", len(repo_full_name)

    start = datetime.now()
    print "Start time is: ",start

    for i in range(int(math.ceil(float(len(repo_full_name))/step))):
        if l2 > len(repo_full_name):
            l2 = len(repo_full_name)
        #print l1, l2
        lis = repo_full_name[l1:l2]
        args = [(li, clientid, clientsecret) for li in lis]
        pool = Pool(processes = 10)
        q0 = pool.map(repoDetailsApiCall, args)
        pool.close()
        pool.join()
        api_output = ",".join(q0)
        api_output = api_output.split(',')
        api_output = [i.split(DELIMETER) for i in api_output]
        repo_info_df = repo_info_df.append(api_output)
        l1, l2 = l2, l2+step

        if (l1 % (num_tokens * calls_per_token) == 0): ##Sleeping condition
            end = datetime.now()
            print "End time is: ",end
            time_spent = (end-start).total_seconds()
            if time_spent < 3600:
                print "Time spent: ", time_spent
                print "Waiting for the reset of the tokens....\n"
                time.sleep(3660 - time_spent)
            else:
                print "Tokens already reset.........\n"
            start = datetime.now()
            print "Start time is: ",start
        
    repo_info_df.columns=['repository_owner', 'repository_name', 'full_name', 'owner_type', 'description', 'created_at', 'size', 'language', 'watchers_count', 'forks_count', 'open_issues_count', 'default_branch', 'score']
    repo_info_df[['repository_owner', 'repository_name', 'full_name', 'owner_type', 'description', 'created_at', 'size', 'language', 'watchers_count', 'forks_count', 'open_issues_count', 'default_branch', 'score']].to_csv(INTERMEDIATE_AND_PREVIOUS_RESULTS+'repository_info_'+str(datetime.now().year)+'.csv',index=False)                
    end = datetime.now()
    print "End time is: ",end
    time.sleep(3600)  ##Sleep for 1 hour in order to reset the token
    return True

def consolidate():
    """
    This method is useful to consolidate all the repository data in order to generate recency-frequency-chart and repo-repo-graph
    """
    df_train = pd.DataFrame()
    for source in glob(INTERMEDIATE_AND_PREVIOUS_RESULTS+'repository_info_*.csv'):
        print "consolidate().source : ",source
        df = pd.read_csv(source,error_bad_lines=False)
        df['description'] = map(lambda body: str(body).lower(), df['description'])
        df_train = df_train.append(df)
    df_train = df_train.reset_index().drop_duplicates(['repository_owner', 'repository_name']).reset_index()  ##add full_name
    print "data loaded"
    return df_train

def divide(df):
    """
    This method is useful to divide into 2 files: younger repos AND repos older than 18 months  
    """
    count = 0
    cur_year,cur_mon = datetime.now().year,datetime.now().month+1
    while cur_mon<18:
        cur_mon = cur_mon+12
        count = count+1
    cur_mon = cur_mon%18
    if cur_mon==0:
        cur_mon = 12
        count = count+1
    df_lt18 = df[df['created_at']>str(datetime((cur_year-count),cur_mon,1))]
    df_gt18 = df[~(df['created_at']>str(datetime((cur_year-count),cur_mon,1)))]
    df_lt18[['repository_owner', 'repository_name', 'full_name', 'owner_type', 'description', 'created_at', 'size', 'language', 'watchers_count', 'forks_count', 'open_issues_count',     'default_branch', 'score']].to_csv(OUTPUT_LOCATION+'repository_info_lessthan18months.csv',index=False)                        
    df_gt18[['repository_owner', 'repository_name', 'full_name', 'owner_type', 'description', 'created_at', 'size', 'language', 'watchers_count', 'forks_count', 'open_issues_count',     'default_branch', 'score']].to_csv(OUTPUT_LOCATION+'repository_info_greaterthan18months.csv',index=False)
    return True

def combineGroupElements(g,column):
    """
    This method is useful to concatenate rows in a dataframe
    """
    col =g[column]
    joiner = "," if column == "contri" else ","
    s = joiner.join([str(each) for each in col if pd.notnull(each)])
    s = re.sub("(?<=&)"+joiner," ",s)
    s = re.sub("(?<=-)"+joiner,"",s)
    s = re.sub(joiner*2,joiner,s)
    return s

def getContributors():
    """
    This method is useful to consolidate all the api call results
    """
    df=pd.DataFrame()
    for source in glob(INTERMEDIATE_AND_PREVIOUS_RESULTS+'result_contributors_*.csv'):
        print "getContributors().source : ",source
        df=df.append(pd.read_csv(source))
    return df

def contributorGraph(df):
    """
    This method constructs contributors graph.
    """
    df = df.drop_duplicates(['full_name','author_login']).reset_index(drop=True)
    df1 = df[pd.notnull(df['author_login'])]  ##Remove NULL valued author_login
    df1 = df1[['full_name','author_login']]

    repo_list = df1['full_name'].unique()
    contributor_list = df1['author_login'].unique()
    links=[]
    for repo1 in repo_list:
        related_contributor_list = df1[df1['full_name'] == repo1]['author_login'].unique()
        for contributor in related_contributor_list:
            related_repo_list = df1[df1['author_login'] == contributor]['full_name'].unique()
            for repo2 in related_repo_list:
                if repo2 != repo1:
                    links.append([repo1, repo2, contributor])

    data = pd.DataFrame(links,columns=['repo1','repo2','contributors'])
    data = data.groupby(['repo1','repo2']) 
    data = data.apply(lambda g: pd.Series([combineGroupElements(g,col) for col in g.columns],index=g.columns))['contributors'].reset_index()
    data['no_common_contributors']= map(lambda line:len(line.split(',')),data['contributors'])
    
    df_with_date = df[['full_name','created_at']].drop_duplicates().reset_index(drop=True)
    df_with_date.columns = ['repo1','created_at']
    data_with_date = pd.merge(data,df_with_date,how='left')  ##to include created_at
    data_with_date[['repo1','repo2','contributors','no_common_contributors','created_at']].to_csv(OUTPUT_LOCATION+'contributors_graph.csv',index=False)
    print "Results written"    
    return True


def contributorApiCall(args):
    """
    This method makes API calls to github to get all the contributors for a repository.
    """
    repo, clientid,clientsecret = args
    for i in range(0,4):
        try:
            url = 'https://api.github.com/repos/'+repo[0]+'/stats/contributors?client_id='+clientid+'&client_secret='+clientsecret
            r = requests.get(url)
            contributor_list = []
            if(r.ok):
                item = json.loads(r.text or r.content)
                for repoItem in item:
                    try:
                        author_login = repoItem['author']['login']
                    except:
                        author_login = ''
                    try:
                        contributions = repoItem['total']
                    except:
                        contributions = ''
                    contributor_list.append((
                        DELIMETER.join([repo[0],str(author_login),str(contributions),repo[1]])
                        ))
                return_string = (",".join([str(i) for i in contributor_list]))
                if return_string == '':
                    return DELIMETER.join([repo[0],'','',repo[1]])
                else:
                    return return_string
            else:
                return DELIMETER.join([repo[0],'','',repo[1]])
        except:
            if i>=1:        
                return DELIMETER.join([repo[0],'','',repo[1]])
            else:
                continue                

def getRepoContributors():
    """
    This method calls contributorApiCall() method for all the repositories.
    """
    total_requests = 5000
    requests_per_resource = 2
    calls_per_token  = int(total_requests/requests_per_resource)
    num_tokens = 1

    model_result = pd.DataFrame()
    for source in glob(MODEL_OUTPUT_LOCATION+'*.csv'):
        print "getRepoContributors() : ",source
        result = pd.read_csv(source)
        model_result = model_result.append(result[['Model_score','repo_owner','repo_name','repo_created_at']])
    repo_full_name = model_result[model_result['Model_score']>=MODEL_CUTOFF].apply(lambda x:([x['repo_owner']+'/'+x['repo_name'],str(x['repo_created_at'])[0:10]]),axis = 1).values.tolist()                 
    repo_contributor_df = pd.DataFrame()
    step = 500
    l1, l2 = 0, step
    print "Total number of repos : ", len(repo_full_name)
    
    start = datetime.now()
    print "Start time is: ",start

    for i in range(int(math.ceil(float(len(repo_full_name))/step))):
        if l2 > len(repo_full_name):
            l2 = len(repo_full_name)
        #print l1, l2
        lis = repo_full_name[l1:l2]
        args = [(li, clientid, clientsecret) for li in lis]
        pool = Pool(processes=10)
        q0 = pool.map(contributorApiCall, args)
        pool.close()
        pool.join()
        api_output = ",".join(q0)
        api_output = api_output.split(',')
        api_output = [i.split(DELIMETER) for i in api_output]
        repo_contributor_df = repo_contributor_df.append(api_output)
        l1, l2 = l2, l2+step

        if (l1 % (num_tokens * calls_per_token) == 0): ##Sleeping condition
            end = datetime.now()
            print "End time is: ",end
            time_spent = (end-start).total_seconds()
            if time_spent < 3600:
                print "Time spent: ", time_spent
                print "Waiting for the reset of the tokens....\n"
                time.sleep(3660 - time_spent)
            else:
                print "Tokens already reset.........\n"
            start = datetime.now()
            print "Start time is: ",start

    repo_contributor_df.columns=['full_name','author_login','contributions','created_at']
    repo_contributor_df[['full_name','author_login','contributions','created_at']].to_csv(INTERMEDIATE_AND_PREVIOUS_RESULTS+'result_contributors_'+str(datetime.now().year)+'.csv',index=False)
    end = datetime.now()
    print "End time is: ",end
    return True


def checkMonth(date1):
    """
    This method computes the number of months between today's date and repo_created_at.
    """
    date1 = datetime.strptime(date1, '%Y-%m-%d %H:%M:%S')
    date2 = datetime.now()
    if date1>date2:
        date1,date2=date2,date1
    m1=date1.year*12+date1.month
    m2=date2.year*12+date2.month
    months=m2-m1
    if date1.day>date2.day:
        months-=1
    elif date1.day==date2.day:
        seconds1=date1.hour*3600+date1.minute+date1.second
        seconds2=date2.hour*3600+date2.minute+date2.second
        if seconds1>seconds2:
            months-=1
    return months

def updateDerivedMetrics():
    """
    This method is useful to consolidate all the derived metrics data.
    """
    df=pd.DataFrame()
    for source in glob(INTERMEDIATE_AND_PREVIOUS_RESULTS+'derived_metrics*.csv'):
        df=df.append(pd.read_csv(source))
    df = df.drop_duplicates(['repo_owner','repo_name','month'])
    df = df.sort(['repo_owner','repo_name','month'])
    df['diff'] = map(lambda x: checkMonth(x),df['repo_created_at'])
    df = df[df['month']<=df['diff']+1]
    df[['repo_owner', 'repo_name', 'user_type', 'month', 'repo_index', 'repo_created_at', 'watchers', 'forks', 'pulls_opened', 'pulls_closed', 'pulls_review_comments', 'issues_opened', 'issues_closed', 'issues_comments', 'commits_comments', 'downloads', 'gists', 'pushes', 'releases', 'pulls_avg_response_time', 'issues_avg_response_time', 'contributors']].to_csv(OUTPUT_LOCATION+'project_details.csv', index=False)
    return "Project_details.csv updated"

def lastUpdatedAt():
    """
    This method updates the last_updated_at.csv with the latest created date for each file present in OUTPUT_LOCATION.
    """
    update_list = []
    for source in glob(OUTPUT_LOCATION+'*.csv'):
        print "lastUpdatedAt().source ",source
        #exclude last_modified_at.csv file
        if source.split('/')[-1] == 'last_updated_at.csv':
            continue
        update_list.append([source.split('/')[-1],datetime.fromtimestamp(int(os.path.getmtime(source))).strftime('%Y-%m-%d %H:%M:%S')])
    pd.DataFrame(update_list,columns=['file_name','last_modified']).to_csv(OUTPUT_LOCATION+'last_updated_at.csv',index=False)
    return "Last updated file has been updated"

if __name__ == '__main__':
    """
    This method calls various post-processing methods.
    """
    print "getRepoInfo() : ",getRepoInfo()
    df = consolidate()
    print "divide(df) : ",divide(df)  #based on >18 and <=18
    print "recency(df) : ",recency(df)      ##Make a call for recency and frequency computation.
    stopwords = getStopwords()
    print "processDF(..) : ",processDF(df,stopwords)
    print "getRepoContributors() : ", getRepoContributors()
    df = getContributors()
    print "contributorGraph() : ",contributorGraph(df)
    print "updateDerivedMetrics() : ",updateDerivedMetrics()
    print "lastUpdatedAt() : ",lastUpdatedAt()
