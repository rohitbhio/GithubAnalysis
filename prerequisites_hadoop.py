import os
import sys
from pdb import set_trace

######################################################
###--- Define the base directory in the HDFS under 
###--- which you would want to create techtrends file
######################################################
if len(sys.argv) != 2:
    print "The script requires a directory location at HDFS as an argument"
    print "Syntax: $ python prerequisites.py hdfs://some_directory"
    print "For Eg: $ python prerequisites.py hdfs://hdfs/data"    
    exit()
else:
    base_dir = sys.argv[1]
curr = os.getcwd()

######################################################
###--- Create TechTrends specific directories
######################################################
os.system("hadoop fs -mkdir " + base_dir + "/GithubAnalysis")
os.system("hadoop fs -mkdir " + base_dir + "/GithubAnalysis/TechTrends")
os.system("hadoop fs -mkdir " + base_dir + "/GithubAnalysis/TechTrends/timeline_files")
os.system("hadoop fs -mkdir " + base_dir + "/GithubAnalysis/TechTrends/repo_info_files")
os.system("hadoop fs -mkdir " + base_dir + "/GithubAnalysis/TechTrends/feeds")
os.system("hadoop fs -mkdir " + base_dir + "/GithubAnalysis/TechTrends/data_dump")

######################################################
###--- Assign permissions
######################################################
# This step is not necessary but still desirable because of streamsets permissions related issue
os.system("hadoop fs -chmod 777 " + base_dir + "/GithubAnalysis")
os.system("hadoop fs -chmod 777 " + base_dir + "/GithubAnalysis/TechTrends")
os.system("hadoop fs -chmod 777 " + base_dir + "/GithubAnalysis/TechTrends/timeline_files")
os.system("hadoop fs -chmod 777 " + base_dir + "/GithubAnalysis/TechTrends/repo_info_files")
os.system("hadoop fs -chmod 777 " + base_dir + "/GithubAnalysis/TechTrends/feeds")
os.system("hadoop fs -chmod 777 " + base_dir + "/GithubAnalysis/TechTrends/data_dump")

#######################################################
#--- Add the hadoop base directory to the config files
#######################################################
# Add the base dir in TF-IDF source base
with open(curr + "/Techtrends/Codebase/Tfidf/config.py","r") as f:
    content = f.readlines()
content[1] = 'HDFS_LOCATION1 = "' + base_dir + '/" #namenode1\n'
content[2] = 'HDFS_LOCATION2 = "' + base_dir + '/" #namenode2\n'
with open(curr + "/Techtrends/Codebase/Tfidf/config.py","w") as f:
    f.writelines(content)
    
# Add the base dir in Derived-metrics source base
with open(curr + "/Techtrends/Codebase/Derived_Metrics/config.py","r") as f:
    content = f.readlines()   
content[1] = 'HDFS_LOCATION1 = "' + base_dir + '/GithubAnalysis/TechTrends/" #namenode1\n'
content[2] = 'HDFS_LOCATION2 = "' + base_dir + '/GithubAnalysis/TechTrends/" #namenode2\n'
with open(curr + "/Techtrends/Codebase/Derived_Metrics/config.py","w") as f:
    f.writelines(content)
