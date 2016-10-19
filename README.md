# GithubAnalysis

## Deploying the project

In order to deploy the project, the following steps should be taken in order:

1. Download/Clone the repository at any location in your machine.

2. Open terminal, go the project folder "GithubAnalysis" and execute the python script "prerequisites.py". This script creates the required directory structure.

  ```python
  $ python prerequisites.py
  ```

3. Execute "prerequisites_data.py" from the project folder "GithubAnalysis". This script downloads all the old data from a file server.

  ```python
  $ python prerequisites_data
  ```

4. Execute "prerequisites_hadoop.py" from the project folder "GithubAnalysis". This script creates the all the required directories at HDFS.

  ```python
  $ python prerequisites_data hdfs://some_directory
  ```

5. Install python dependencies using pip utility:

  ```
  dateutil, numpy, scipy, pandas, httplib2, MySQL-python, sqlalchemy, simplejson, config, requests, google-api-python-client, scikit-learn, feedparser, beautifulsoup4, lxml, Flask, Flask-Login, PyOpenSSL, pycrypto, oauth2client==1.5.2, ntlk (After installing this package, run command "sudo python -m ntlk.downloader -d /usr/share/ntlk_data all" to install ntlk-data)
  ```

6. Edit system settings as below:
  * Add following entry in /etc/security/limits.conf:

    ```
    * hard nofile 128000
    * soft nofile 128000
    root hard nofile 128000
    root soft nofile 128000
    ```

  * logout from the machine and login back. Check for "ulimit -n". It should show 128000

7. Crawler prerequisites:
Our data extractors gather data. Both of these sources allow restricted/authenticated access in order to balance load on their servers. For authentication purpose, we are required to create OAuth tokens in order to run our crawlers.
  * Creating OAuth tokens for GitHub
    * [Login](www.GitHub.com) to your GitHub account
    * Go to: Settings -> OAuth applications
    * Click on Developer applications -> "Register a new application"
    * After registering an application, OAuth tokens "Client ID" and "Client Secret" would be generated.
    * Copy these token values to the Post_Processing configuration file: "post_processing_config.py" at "GitHubAnalysis/Techtrends/Codebase/Post_Processing"

  * Creating OAuth tokens for Google BigQuery
    * [SIGN IN](https://cloud.google.com/bigquery/) to google bigquery
    * Click on "Try it free" and fill your details.
    * Click on "Create a project..." by going to "Go to project" on the upper-right bar
    * Create the project by entering the required fields. Note down the "project ID" and "project Name" for future reference.
    * Go to "Manage all projects" -> "Service accounts" -> "Create service account". Note "Service account ID" and download P12(private key) and create a "Service account"
    * [Convert](https://www.icts.uiowa.edu/confluence/pages/viewpage.action?pageId=32735365) p12 file to pem file and rename the pem file as "google-api-client-key.pem"
    * Move the pem file to "GitHubAnalysis/Techtrends/Codebase/Crawlers"
    * Also modify the following parameters at "BigQuery_config.py" file:  PROJECT_NUMBER = "project id" and SERVICE_ACCOUNT_EMAIL="Service account ID"

8. Run crontab files by modifying the location of the directories
