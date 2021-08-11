# soda-jira
## Install
To run this as a lambda function you need to install the pip package
- pip install --target ./package jira
- cd package
- zip -r ../soda-jira.zip .
- cd ..
- zip -g soda-jira.zip sodacloud.py
- zip -g soda-jira.zip lambda_function.py
