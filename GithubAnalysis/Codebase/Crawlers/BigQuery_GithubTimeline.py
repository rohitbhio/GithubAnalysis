#!/usr/bin/env python
import httplib2
import pprint
import sys
import os
import argparse
import pandas as pd
from pdb import set_trace as trace
import simplejson as json
from datetime import datetime, timedelta, date
import time
from apiclient.discovery import build
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client import client
from BigQuery_config import *
from glob import glob
import subprocess
import re
import logging
from logging.config import fileConfig

def saveResult(query_response):
    """
    This method will take the BigQuery response as input, parse it and save the result to CSV
    """
    result_row_final2 = []
    new_repo_list = []
    for row in query_response['rows']:
        try:
            result_row = []
            for field in row['f']:
                result_row.append('' if field['v'] is None else field['v'])
            repo_name = result_row[2]
            repository_owner, repository_name = [str(i) for i in repo_name.split('/')]
            event = str(result_row[0])
            actor_login = str(result_row[3])
            created_at = str(result_row[4])
            # If there is no repo name or no event or nocreated time then skip this row.
            if repo_name is '' or event is '' or created_at is '':
                continue
            # If the event is not needed then we will skip the row
            untracked_events = ['DeleteEvent','DeploymentEvent','DeploymentStatusEvent','FollowEvent','ForkApplyEvent',
                                'GollumEvent','MemberEvent','MembershipEvent','PageBuildEvent','StatusEvent','TeamAddEvent'
                               ]
            if event in untracked_events: 
                continue
            created_at = datetime.strptime(created_at,'%Y-%m-%d %H:%M:%S.000000').strftime('%Y-%m-%d %H:%M:%S')
            user_type_id = str(result_row[5])
            user_type_login = str(result_row[6])
            if user_type_id =='' and user_type_login =='':
                user_type = 'User'
            else:
                user_type = 'Organization'
            jason = json.loads(result_row[1])  ##Load the payload into json object
    
            if event == 'CreateEvent':
                if jason['ref_type'] == 'repository':
                    new_repo_list.append([repository_owner, repository_name, created_at, user_type])
                continue
            if event == 'PublicEvent':
                new_repo_list.append([repository_owner, repository_name, created_at, user_type])
                continue
            # Pull Request Event Block
            event_number, event_action, event_created_at, event_closed_at = '', '', '', ''
            if event == 'PullRequestEvent':
                event_number = str(jason['number'])
                event_created_at = str(jason['pull_request']['created_at']).replace('T', ' ').replace('Z', '') if 'created_at' in jason['pull_request'].keys() else ''
                if jason['action'] == 'opened' or jason['action'] == 'reopened':
                    event_action = 'opened'
                elif jason['action'] == 'closed':
                    event_action = 'closed'
                    event_closed_at = str(jason['pull_request']['closed_at']).replace('T', ' ').replace('Z', '') if 'closed_at' in jason['pull_request'].keys() else ''
                else:
                    continue
            #Issue Event Block
            if event == 'IssuesEvent':
                event_created_at = str(jason['issue']['created_at']).replace('T', ' ').replace('Z', '') if 'created_at' in jason['issue'].keys() else ''
                event_number = str(jason['issue']['number'])
                if jason['action'] == 'opened' or jason['action'] == 'reopened':
                    event_action = 'opened'
                elif jason['action'] == 'closed':
                    event_action = 'closed'
                    event_closed_at = str(jason['issue']['closed_at']).replace('T', ' ').replace('Z', '') if 'closed_at' in jason['issue'].keys() else ''
                else:
                    continue
            if event == 'CommitCommentEvent':
                event_number = str(jason['comment']['commit_id']) if jason['comment'].get('commit_id') else ''
    
            result_row_final = [repository_owner, repository_name, actor_login, created_at, event, 
                                event_action, event_number, event_created_at, event_closed_at, user_type]
            result_row_final2.append(result_row_final)
	except:
	    print "Error while saving the row"
	    continue
    df = pd.DataFrame(result_row_final2)
    repo_info = pd.DataFrame(new_repo_list)
    if len(new_repo_list)==0:
        print "length of the list is 0"
    else:
        repo_info.columns = ['repository_owner','repository_name','repo_created_at','user_type']  ##This step is essential for append to function properly
    return df,repo_info

def getDaysSinceLastRun():
    """
    This method will return the days since this script pulled the data from BigQuery
    """
    file_poin = open('BigQuery_config.py','r+')
    lines = file_poin.readlines()
    file_poin.close()
    last_day = "".join(lines[-1:]).split('"')[1]
    no_of_days = (datetime.now().date() - datetime.strptime(last_day,'%Y%m%d').date()).days
    return no_of_days-1 

def saveDateOfRecentRun(tablename):
    """
    This method will store the latest date of last RUN.
    """
    file_poin = open('BigQuery_config.py','r+')
    lis = file_poin.readlines()[:-1]
    file_poin.close()
    lis.append('LAST_RUN = "'+tablename+'"\n')
    file_poin = open('BigQuery_config.py','w')
    file_poin.writelines(lis)
    file_poin.close()
    return True

def main():
    """
    This method will pull the data from BigQuery and Save the result to CSV
    """
    print "Started Pulling the information"
    credentials = SignedJwtAssertionCredentials(SERVICE_ACCOUNT_EMAIL,
                                                key,
                                                scope='https://www.googleapis.com/auth/bigquery')

    http = httplib2.Http()
    auth_http = credentials.authorize(http)
    
    bigquery_service = build('bigquery', 'v2', http=auth_http)
    
    ##In the BigQuery_Config.py file, the initial RUN_FREQ is set as 1.
    num_days = getDaysSinceLastRun()
    print "The number of days for which the data needs to be pulled",num_days
    #for date_increment in range(num_days, 0, -1):
    while num_days>0:
        ##get yesterday's date
        yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days = num_days)
        filename = yesterday.strftime('gh_timeline_%Y_%b')
        tablename = yesterday.strftime('%Y%m%d')  # Variable used for the query. Also used to store the date in config file.
        reponame = yesterday.strftime('%Y')+'_repo_info'   #Variable used to name the repo_info file based on year.
        repo_info = pd.DataFrame()
        df = pd.DataFrame()   #This is the dataframe which shall store the day information
        #Estimating the number of iterations 
        num_iter = 24/STEP_SIZE_HOURS;
        try:
            start = yesterday
            for i in range(0, num_iter):
                end = start + timedelta(hours=STEP_SIZE_HOURS)
                fmt_dict = {"tablename":tablename, "start_date":start.strftime('%Y-%m-%d %H:%M:%S'), "end_date":end.strftime('%Y-%m-%d %H:%M:%S')}
                query_request = bigquery_service.jobs()
                query_data = {"query":'select type,payload,repo.name,actor.login,FORMAT_UTC_USEC(created_at) as created,org.id,org.login \
                               from [githubarchive:day.%(tablename)s] where TIMESTAMP(created_at) >= TIMESTAMP("%(start_date)s") \
                               AND TIMESTAMP(created_at) < TIMESTAMP("%(end_date)s");'%fmt_dict}
                print query_data

                try:
                    query_response = query_request.query(projectId=PROJECT_NUMBER, body=query_data).execute()
                except:
                    print "Error in returning the query_response since the response might be huge"
                    print "try again by changing the value of STEP_SIZE_HOURS to one of these(4,3,2,1)"
                
                jobReference=query_response['jobReference']
                if ('rows' in query_response):
                    df_temp, repo_info_temp = saveResult(query_response)
                    df = df.append(df_temp)
                    repo_info = repo_info.append(repo_info_temp)
                    currentRow = len(query_response['rows'])

                    # loop through each page of data using page token.
                    while ('rows' in query_response and currentRow < query_response['totalRows']):
                        try:
                            query_response = query_request.getQueryResults(projectId=jobReference['projectId'], 
                                                                           jobId=jobReference['jobId'], 
                                                                           startIndex=currentRow).execute()
                        except Exception,e:
                            print "Error in getting subsequent pages"
                            continue
                        if ('rows' in query_response):
                            df_temp, repo_info_temp = saveResult(query_response)
                            if df_temp.shape[0] != 0:
                                df = df.append(df_temp)
                            if repo_info.shape[0] != 0:
                                repo_info = repo_info.append(repo_info_temp)
                            currentRow += len(query_response['rows'])            
                start = end

        except Exception,e:
            print "=============== Connection refused ==================="
            print e
            return False

        saveColumns = ['repository_owner', 'repository_name', 'actor_login', 'created_at',
                       'event', 'event_action', 'event_number', 'event_created_at', 'event_closed_at', 
                       'user_type']
        if df.shape[0] == 0:
            continue
        num_days = num_days - 1
        df.columns = saveColumns
        #If the file exists already append the data to existing file else create one and write to it.
    
        if int(yesterday.strftime('%d')) <= 15:
            append = "_1"
        else:
            append = "_2"
	    
    	if filename + append + ".csv" not in glob(DATA_LOCATION + "timeline_files/*csv"):
    	    write_file = open(DATA_LOCATION + "timeline_files/" + filename + append + ".csv", "a")
    	    df.to_csv(write_file, index=False)
        else:
            write_file = open(DATA_LOCATION + "timeline_files/" + filename + append + ".csv", "a")
            df.to_csv(write_file, index=False, header=False)
        write_file.close()
        
        if reponame + ".csv" not in glob(DATA_LOCATION + "repo_info_files/*csv"):
            repo_file = open(DATA_LOCATION + "repo_info_files/" + reponame + ".csv", "a")
            repo_info.to_csv(repo_file, index=False)
        else:
            repo_file = open(DATA_LOCATION + "repo_info_files/" + reponame + ".csv", "a")
            repo_info.to_csv(repo_file, index=False, header=False)
        repo_file.close()
        saveDate = saveDateOfRecentRun(tablename)
    return True
	    
def checkDuplicates():
    """
    This method removes the duplicates among different *repo_info.csv files.
    """
    dfs = []
    for fname in glob(DATA_LOCATION + "repo_info_files" + "/*.csv"):
        dfs.append(pd.read_csv(fname,parse_dates=True))
    df = pd.concat(dfs,ignore_index=False)
    df = df[df['repository_owner'] != 'repository_owner']
    df = df.sort('repo_created_at')
    df = df.drop_duplicates(['repository_owner','repository_name'])
    for fname in glob(DATA_LOCATION + "repo_info_files" + "/*.csv"):
        df[(df['repo_created_at'] >= str(re.search('repo_info_files/(.+?)_repo_info.csv', fname).group(1)) + '-01-01 00:00:00') & (df['repo_created_at'] < str(int(re.search('repo_info_files/(.+?)_repo_info.csv', fname).group(1))+1) + '-01-01 00:00:00')].to_csv(DATA_LOCATION + "repo_info_files/" + str(re.search('repo_info_files/(.+?)_repo_info.csv', fname).group(1)) + "_repo_info.csv",index=False)
    return True

if __name__ == '__main__':
    try:
        success = main()
        print "Data Pull from Bigquery completed"
        repoCheck = checkDuplicates() 
        print "Duplicates removed"
    except Exception as e:
        print e
