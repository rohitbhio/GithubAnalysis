import os
PROJECT_NUMBER='decent-micron-803' #'turing-clover-817' #Google  developer Project Number
SERVICE_ACCOUNT_EMAIL='612668830024-mu6a56c4b19d9t8aoovbrlh2l3btqvfi@developer.gserviceaccount.com'#'892247925786-lerfsd75oj39lkfb66icdkq2eb0b3ap2@developer.gserviceaccount.com'
RUN_FREQ = 1     #variable to tweak the frequency of running the code.
STEP_SIZE_HOURS = 3;  #Used to create the day partition by hours. Note: STEP_SIZE_HOURS should be in (1, 2, 3, 4, 6, 8, 12, 24) i.e., divisors of 24.
SCRIPT_LOCATION = os.getcwd() + "/"
f = file(SCRIPT_LOCATION + 'google-api-client-key.pem', 'rb')
key = f.read()
f.close()
DATA_LOCATION = os.path.dirname(os.path.dirname(os.getcwd()))+ "/Data/"
LAST_RUN = "20160605"
