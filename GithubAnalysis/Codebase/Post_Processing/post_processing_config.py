import os 
MODEL_OUTPUT_LOCATION = os.path.dirname(os.path.dirname(os.getcwd())) + '/Data/output/score/'  ##This is the location where model output is present. 
INTERMEDIATE_AND_PREVIOUS_RESULTS = os.path.dirname(os.path.dirname(os.getcwd())) + '/Data/output/Intermediate_and_previous_results/'  ##This location contains the output for api calls and intermediate result 
OUTPUT_LOCATION = os.path.dirname(os.getcwd()) + '/WebHost/static/data/'    ##The dashboard picks the files from this location. 
#STOPWORDS_LOCATION = 'hdfs://hdfs/GithubAnalysis/TechTrends/feeds/'   ##This location contains stopwords 
STOPWORDS_LOCATION = os.path.dirname(os.getcwd()) + '/Tfidf/' 
MODEL_CUTOFF = 0.14 
DELIMETER = '___delematar___'
clientid = 'e3c7ffded7feff787328'  #username = cisco-tech
clientsecret = '21c37f56e87be7b08eef0c5ca3a63735fea9bfea'  #username = cisco-tech
