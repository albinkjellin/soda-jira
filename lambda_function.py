from collections import Counter
from typing import cast

from jira import JIRA
from jira.client import ResultList
from jira.resources import Issue
from sodacloud import SodaCloud
from datetime import datetime, timedelta
import os


def lambda_handler(event, context):
    print('Start')
    jira = JIRA(server="https://soda-demo.atlassian.net", basic_auth=(os.environ['JIRA_USER'], os.environ['JIRA_TOKEN']))  # a username/password tuple


    soda = SodaCloud(
            'cloud.soda.io',
            api_key_id=os.environ['API_PUBLIC'],
            api_key_secret=os.environ['API_PRIVATE']
        )

    testResults = soda.tests_with_dependants('66ccd003-7fe5-4ee9-8dad-34171114c22f')
    print(len(testResults))
    print(testResults[6])
    oneDayAgo = datetime.today() - timedelta(days=1)
    print(oneDayAgo)

    for tr in testResults:
        #2021-08-10T12:14:33Z

        if 'lastTestResult' in tr and tr['lastResult'] and tr['lastTestResultTime'] :
            testtime = datetime.strptime(tr['lastTestResultTime'], '%Y-%m-%dT%H:%M:%SZ')
            print(testtime > oneDayAgo)
            if (testtime > oneDayAgo) :
                if 'metric' in tr and 'columnName' in tr['metric'] :
                    issue_dict = {
                        'project': {'id': '10001'},
                        'summary': str(tr['name']) + ' for Column ' + str(tr['metric']['columnName']),
                        'description': 'Value is ' + str(tr['lastTestResult']['value']),
                        'issuetype': {'id': '10006'},
                        }
                else:
                    issue_dict = {
                        'project': {'id': '10001'},
                        'summary': str(tr['name']) + ' for Column ',# + str(tr['metric']['columnName']),
                        'description': 'Value is ' + str(tr['lastTestResult']['value']),
                        'issuetype': {'id': '10006'},
                        }
                print(issue_dict)
                new_issue = jira.create_issue(fields=issue_dict)
                print(new_issue)
