import os
curr = os.getcwd()

######################################################
##--- Create required directory structure
######################################################
if not os.path.exists(curr + "/Techtrends/Data"):
    os.system("mkdir " + curr + "/Techtrends/Data")
if not os.path.exists(curr + "/Techtrends/Data/feeds"):
    os.system("mkdir " + curr + "/Techtrends/Data/feeds")
if not os.path.exists(curr + "/Techtrends/Data/repo_info_files"):
    os.system("mkdir " + curr + "/Techtrends/Data/repo_info_files")
if not os.path.exists(curr + "/Techtrends/Data/timeline_files"):
    os.system("mkdir " + curr + "/Techtrends/Data/timeline_files")
if not os.path.exists(curr + "/Techtrends/Data/output"):
    os.system("mkdir " + curr + "/Techtrends/Data/output")
if not os.path.exists(curr + "/Techtrends/Data/output/Intermediate_and_previous_results"):
    os.system("mkdir " + curr + "/Techtrends/Data/output/Intermediate_and_previous_results")
if not os.path.exists(curr + "/Techtrends/Data/output/monthly_derived_metrics"):
    os.system("mkdir " + curr + "/Techtrends/Data/output/monthly_derived_metrics")
if not os.path.exists(curr + "/Techtrends/Data/output/score"):
    os.system("mkdir " + curr + "/Techtrends/Data/output/score")
