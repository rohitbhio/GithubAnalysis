from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.mllib.tree import RandomForest, RandomForestModel
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql.types import *
import time
from functools import partial
from numpy import percentile
from config import *
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import os,sys
from pyspark.mllib.util import MLUtils
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from glob import glob

conf  = (SparkConf()
         .setAppName("Tech Trends App") # Name of App
         .set("spark.executor.memory", "30g")
         )
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

print "----------------------------Loggers set---------------------------"

def getDateDiff(from_date, to_date, diff_type = 'Month'):
    """
    Given the from and to dates this method will return the differance in months/Hourse based on the differance type
    """
    try:
        from_date = datetime.strptime(str(from_date),'%Y-%m-%d %H:%M:%S')
        to_date = datetime.strptime(str(to_date),'%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return None
    diff_time = (from_date - to_date).total_seconds()
    if diff_type == 'Month':
        diff_time = diff_time/(30*24*3600)
    else:
        diff_time = diff_time/3600
    month = int(np.ceil(diff_time)) if diff_time > 0 else int(np.floor(diff_time))
    return month

def getIndex(created_at):
    """
    Given created date this will return Index for repository, this index will be used for normalizing the repository
    metrics like watchers, forks under the assumption that repos created in 4 months period will behave similary in some way.
    """
    index_dict = {"2014-01":"1", "2014-02":"1", "2014-03":"1", "2014-04":"1",
                  "2014-05":"2", "2014-06":"2", "2014-07":"2", "2014-08":"2",
                  "2014-09":"3", "2014-10":"3", "2014-11":"3", "2014-12":"3",
		  "2015-01":"4", "2015-02":"4", "2015-03":"4", "2015-04":"4",
		  "2015-05":"5", "2015-06":"5", "2015-07":"5", "2015-08":"5",
		  "2015-09":"6", "2015-10":"6", "2015-11":"6", "2015-12":"6",
		  "2016-01":"7", "2016-02":"7", "2016-03":"7", "2016-04":"7",
		  "2016-05":"8", "2016-06":"8", "2016-07":"8", "2016-08":"8",
		  "2016-09":"9", "2016-10":"9", "2016-11":"9", "2016-12":"9"
		  }
    return index_dict.get(created_at[:7])

def findFiles(numberOfMonths):
    """
    Generates file names from current month till numberOfMonths param (going back)
    """
    current_dt = datetime.now()
    first = datetime(day=1,month=current_dt.month,year=current_dt.year)
    prev_dt = first - timedelta(days=25*numberOfMonths)    
    processFiles = []
    while prev_dt <= current_dt:
        processFiles.append('gh_timeline_'+prev_dt.strftime("%Y")+'_'+prev_dt.strftime("%b")+'*.csv')
        prev_dt = prev_dt + relativedelta(months=1)
    return processFiles

def processTimelineFiles(numberOfMonths):
    """
    This method will read the timeline and repository info files, Estimate the derived metrics, save the results monthly (one for each monthly timline file).
    """
    st = time.time()
    repo = sqlContext.load(HDFS_LOCATION1 + "repo_info_files/", source="com.databricks.spark.csv", header="true")
    repo.registerTempTable("repo")
    sqlContext.registerFunction('date_diff',getDateDiff)
    sqlContext.registerFunction('get_index', getIndex)
    for readfile in findFiles(numberOfMonths):   
        # Reading the Repository Timeline files from HDFS
	print readfile
        timeline_Data = sqlContext.load(HDFS_LOCATION1 +"timeline_files/"+readfile, source="com.databricks.spark.csv", header="true")
        timeline_Data.registerTempTable("timeline")
        
	#Joining the repository timeline and repository infor files and getting the month till the event created, reesponse time in Hours 
        query = """select timeline.repository_owner repo_owner, timeline.repository_name repo_name, get_index(repo.repo_created_at) as repo_index, 
                   date_diff(timeline.created_at,repo.repo_created_at,'Month') month, 
                   timeline.actor_login actor_login, timeline.created_at created_at, timeline.event event, timeline.event_action event_action, timeline.event_number 
	           event_number,timeline.event_created_at event_created_at, timeline.event_closed_at event_closed_at, 
	           date_diff(timeline.event_closed_at,timeline.event_created_at, 'Hour') response_time, repo.repo_created_at repo_created_at, 
	           repo.user_type user_type from timeline JOIN repo ON timeline.repository_owner = repo.repository_owner and 
	           timeline.repository_name = repo.repository_name
                """
        timeline_repo = sqlContext.sql(query)
        timeline_repo.registerTempTable("timeline_table")

        #Summarizing the various metrics like Watchers, forks etc by the month and repository
        summarize_query = """ 
                          SELECT repo_owner, repo_name, user_type, first(repo_index) repo_index, month,
	        	      first(repo_created_at) repo_created_at, sum(case when event='WatchEvent' then 1 else 0 end) watchers, 
                            sum(case when event='ForkEvent' then 1 else 0 end) forks,sum(case when event='PullRequestEvent' 
                            then 1 else 0 end) pulls_opened,sum(case when event='PullRequestEvent' and event_action='closed' 
                            then 1 else   0 end)  pulls_closed,sum(case when event='PullRequestReviewCommentEvent' then 1 else 0 end) 
                            pulls_review_comments,sum(case when event='IssuesEvent' then 1 else 0 end)  issues_opened, 
                            sum(case when event='IssuesEvent' and event_action='closed' then 1 else 0 end) issues_closed, 
                            sum(case when event='IssueCommentEvent' then 1 else 0 end)  issues_comments, 
                            sum(case when event='CommitCommentEvent' then 1 else 0 end) commits_comments,
                            sum(case when event='DownloadEvent' then 1 else 0 end) downloads, sum(case  when event='GistEvent' 
                            then 1 else 0 end) gists,sum(case when event='PushEvent' then 1 else 0 end) pushes, 
                            sum(case when event='ReleaseEvent' then 1 else 0 end)  releases, 
			    avg(case when event='PullRequestEvent' and response_time > 0 and event_action='closed' 
                            then response_time end) pulls_avg_response_time, 
			    avg(case when event='IssuesEvent' and response_time > 0 and event_action='closed' then response_time   
                            end)  issues_avg_response_time,
                            count(distinct case when event in ('PullRequestEvent','IssueCommentEvent','PullRequestReviewCommentEvent','IssuesEvent',
                            'CommitCommentEvent','PushEvent') then actor_login end) contributors FROM timeline_table where cast(month as int) > 0 
                            group by repo_owner, repo_name, user_type, month
                         """                     
    
        #monthly wise aggregation
        derived_metrics_monthly = sqlContext.sql(summarize_query)
    
        try: 
	    df = derived_metrics_monthly.toPandas()
            df.to_csv(OUTPUT_LOCATION + "monthly_derived_metrics/monthly_agg_"+readfile.split('*')[0]+".csv", index=False)
        except Exception as e:
	    print "Exception in saving monthly aggregate files", e
            return False
    
    #copy to hdfs
    copy_to_hdfs = "hadoop fs -put -f " + OUTPUT_LOCATION +"monthly_derived_metrics/* " + HDFS_LOCATION1 + "data_dump/"
    os.system(copy_to_hdfs)
    
    print "Completed derived metrics module time taken : ",(time.time() - st)/60," minutes"
    return True

def getRatio(row):
    """
    This method will calculate the ratios for all the columns with respect to 95% qunatile and return the dataframe
    """
    out_row = {i:row[i] for i in ["repo_owner", "repo_name", "user_type", "month", "repo_index", "repo_created_at"]}
    normalize_columns = ["watchers", "forks", "pulls_opened", "pulls_closed", "pulls_review_comments",
                         "issues_opened", "issues_closed", "issues_comments", "commits_comments", "downloads",
                         "gists", "pushes", "releases", "pulls_avg_response_time", "issues_avg_response_time",
                         "contributors"
                         ]
    for item in normalize_columns:
         if row[item+"_95"] == 0:
             out_row[item] = row[item]
         elif row[item]/row[item+"_95"] > 1:
             out_row[item] = 1
         else:
             out_row[item] = row[item] / row[item+"_95"]
    return pd.Series(out_row)

def normalizeData(df):
    """
    Normalize data using min - max normalization. normalized value = (value-min(column)) / (max(col)-min(col)) . Most of metrics min value is 0.
    """
    df = df.sort(["repo_owner", "repo_name", "month"]).groupby(["repo_owner", "repo_name"]).apply(lambda grp: grp.fillna(method='ffill')).fillna(0)
    getPercentile  = partial(percentile, q=95)
    #Calculate the 95% quantile on columns and fill NA's with zero
    df_95 = df[df.columns[2:]].groupby(["user_type", "month", "repo_index"], as_index=False).agg(getPercentile)
    df_95.columns = [i + "_95" if i not in ('user_type', u'month', u'repo_index') else i for i in df_95.columns]
    df = df.merge(df_95,on=["user_type", "month","repo_index"])
    df = df.apply(getRatio, axis=1)
    return df

def loadModels():
    """
    This method will load the trained models from saved file to memory
    """
    model_dict = {}
    model_location = MODEL_LOCATION + "sklearn_models/"
    for model_type in ('User', 'Organization'):
        model_dict[model_type] = {}
        for month in range (1, 19):
            model_file = "RF_" + model_type + "_" + str(month)
            model_dict[model_type][month] = joblib.load(model_location + model_file)
    return model_dict


def scoreRepositories(df, model_dict=None):
    """
    Method to score the repository using the trained models
    """
    ord_cols = ['watchers', 'forks', 'commits_comments', 'issues_opened', 'issues_comments',
                'issues_avg_response_time', 'pulls_opened', 'pulls_avg_response_time', 
		'pulls_review_comments', 'pushes', 'contributors']
    df['month'] = df.month.astype("int64")
    df = df.merge(pd.DataFrame([i for i in range(1, int(df.month.max())+1)], columns=["month"]), how='outer', on="month")
    df = df.sort("month", ascending=True).fillna(method='ffill').fillna(0)
    df_score = df[ord_cols]
    x = sum(df_score.values.tolist(), [])
    model = model_dict[df.user_type[0]][int(df.month.max())]
    out_row = {"Model_score":model.predict_proba(x)[0][1], "repo_owner":df.repo_owner[0], "repo_name":df.repo_name[0],
               "user_type": df.user_type[0], 'repo_created_at':df.repo_created_at[0], "Age of Project":df.month.max()}
    return pd.Series(out_row)

def processDataAndScore(user_type = 'User'):
    """
    This method will read the monthly aggregated files, compute the cumulative values for metrics, save the results, and additionally normalize these metrics and 
    score the repositories using the appropriate model.
    """
    st = time.time()    
    
    #Estimating the cumulative sum of the parameters
    dm_filtered_inter = sqlContext.load(path= HDFS_LOCATION1 + "data_dump/", source="com.databricks.spark.csv", header="true") 
    dm_filtered_inter.registerTempTable("dm_month_table_inter")

    #filtering
    maxMonth = sqlContext.sql("""select repo_owner as repos_owner, repo_name as repos_name, max(month) maxMonth, sum(watchers) maxWatchers, sum(forks) maxForks 
                                  from dm_month_table_inter group by repo_owner, repo_name 
                                  having (maxMonth > 12 and maxWatchers > 15 and maxForks > 15) 
                                  or (maxMonth > 1 and maxMonth <= 12 and maxWatchers > 10 and maxForks > 10) 
                                  or (maxMonth = 1 and maxWatchers > 5 and maxForks > 5)""")
    maxMonth.registerTempTable("max_month_table")

    dm_filtered_1 = sqlContext.sql("""select repo_owner,repo_name,user_type,month,first(repo_index) repo_index,max(repo_created_at) repo_created_at, 
                               sum(watchers) watchers, 
                               sum(forks) forks, sum(pulls_opened) pulls_opened, sum(pulls_closed) pulls_closed, 
                               sum(pulls_review_comments) pulls_review_comments, sum(issues_opened) issues_opened, 
                               sum(issues_closed) issues_closed, sum(issues_comments) issues_comments, sum(commits_comments) commits_comments, 
                               sum(downloads) downloads, sum(gists) gists, sum(pushes) pushes, sum(releases) releases,
                               avg(pulls_avg_response_time) pulls_avg_response_time, avg(issues_avg_response_time) issues_avg_response_time,   
                               sum(contributors) contributors from dm_month_table_inter t1 
                               join max_month_table t2  on t1.repo_owner = t2.repos_owner and t1.repo_name = t2. repos_name where t1.user_type='%s' 
                               group by repo_owner,repo_name,user_type,month"""%user_type)
    dm_filtered_1.registerTempTable("dm_month_table_1")
    
    dm_filtered_2 = sqlContext.createDataFrame(dm_filtered_1.collect())
    dm_filtered_2.registerTempTable("dm_month_table_2")

    dm_cumsum = sqlContext.sql("""select d1.repo_owner, d1.repo_name, d1.user_type, d1.month, first(d2.repo_index) repo_index, 
                               max(d2.repo_created_at) repo_created_at, sum(d2.watchers) watchers, 
                               sum(d2.forks) forks, sum(d2.pulls_opened) pulls_opened, sum(d2.pulls_closed) pulls_closed, 
                               sum(d2.pulls_review_comments) pulls_review_comments, sum(d2.issues_opened) issues_opened, 
                               sum(d2.issues_closed) issues_closed, sum(d2.issues_comments) issues_comments, sum(d2.commits_comments) commits_comments, 
                               sum(d2.downloads) downloads, sum(d2.gists) gists, sum(d2.pushes) pushes, sum(d2.releases) releases,
                               avg(d2.pulls_avg_response_time) pulls_avg_response_time, avg(d2.issues_avg_response_time) issues_avg_response_time,   
                               sum(d2.contributors) contributors from dm_month_table_1 d1 join dm_month_table_2 d2 
                               on d1.repo_owner = d2.repo_owner and d1.repo_name = d2.repo_name where 
                               cast(d1.month as int) >= cast(d2.month as int) group by d1.repo_owner,d1.repo_name,d1.user_type,d1.month""")
    dm_cumsum.registerTempTable("dm_cumsum")    
    dm_cumsum.cache()
    df = pd.DataFrame()
    try:
        print "Writing cumulative metrics to csv"
	df = dm_cumsum.toPandas()
        if user_type=='User':
            df.to_csv(OUTPUT_LOCATION + "Intermediate_and_previous_results/derived_metrics_user_"+str(datetime.now().year)+".csv", index=False)
        else:
            df.to_csv(OUTPUT_LOCATION + "Intermediate_and_previous_results/derived_metrics_org_"+str(datetime.now().year)+".csv", index=False)
    except Exception as e:
	print "Exception in cumulative metrics calculation",e
        return False

    try:
        print "starting normalize module"
        df = normalizeData(df)
        df.to_csv(OUTPUT_LOCATION + "derived_metrics_normalized_%s.csv"%user_type, index=False)
    except Exception as e:
        print "Exception during normalizing data",e
        return False

    
    #Scoring the data using the trained model
    try:
        print "starting scoring repos"
        model_dict = loadModels()
        print "models loaded"
        print df.shape
        #df = df.groupby(["repo_owner", "repo_name", "user_type"], as_index=False).apply(lambda x: x[(x.month<=18) & (x.month.max()<=18)]).reset_index(drop=True)
        df['month'] = df.month.astype("int64")
        df = df[df.month<=18]
        print df.shape
        result = df.groupby(["repo_owner", "repo_name", "user_type"], as_index=False).apply(scoreRepositories, model_dict = model_dict)
        print "repos scored"
        result.to_csv(OUTPUT_LOCATION + "score/model_score_%s.csv"%user_type, index=False)
        print "model file written"
    except Exception as e:
        print "Exception in scoring the data",e
        return False

    print "Module Completed: Cumulation, Normalize & Scoring. Time Taken : ",(time.time() - st)/(60)," minutes"
    return True

if __name__ == '__main__':
    try:
        if len(sys.argv[1]) == 0:
	    print "Provide a mandatory numberOfMonths argument"
	    sys.exit(0)
    except Exception as e:
        print "Input the valid number of months to Run the aggregation for"
	sys.exit(0)

    if processTimelineFiles(int(sys.argv[1])):
        try:
	    print "user process Data and score start"
            processDataAndScore(user_type='User')   
	    print "org process Data and score start"
	    processDataAndScore(user_type='Organization')
	    
	    #post_processing_script = "python /root/Cisco_Techtrends/Cisco_codebase/Post_Processing/post_processing_main.py"
            #os.system(post_processing_script)
        except Exception as e:
            print "Exception in cumulation and scoring module"   
