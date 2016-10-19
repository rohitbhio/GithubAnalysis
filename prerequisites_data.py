import os
curr = os.getcwd()

##----------------------------------------------------##
##              Copy intermediate data
##----------------------------------------------------##
dir_switch = curr + "/Techtrends/Data/output/Intermediate_and_previous_results"
get_Intermediate_results = 'wget -r -nH --reject="index.html*" http://54.163.226.98:7555/'
try:
    os.system("cd " + dir_switch + "; " + get_Intermediate_results)
except Exception,e:
    print "Could not retrieve data from server", e
    
##----------------------------------------------------##
##               Copy Webhost Data
##----------------------------------------------------##
dir_switch = curr + "/Techtrends/Codebase/WebHost/static/data"
get_Intermediate_results = 'wget -r -nH --reject="index.html*" http://54.163.226.98:7556/'
try:
    os.system("cd " + dir_switch + "; " + get_Intermediate_results)
except Exception,e:
    print "Could not retrieve data from server", e    


##----------------------------------------------------##
##               Copy Crawler Data
##----------------------------------------------------##
dir_switch = curr + "/Techtrends/Data"
get_Intermediate_results = 'wget -r -nH --reject="index.html*" http://54.163.226.98:7557/'
try:
    os.system("cd " + dir_switch + "; " + get_Intermediate_results)
except Exception,e:
    print "Could not retrieve data from server", e
