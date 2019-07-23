#!/usr/bin/env python3
##
# Connect to mongodb with all the bells and whistles
##
import sys
import random
import time
import datetime
import pymongo


####
# Main start function
####
def main():
    print('')

    if len(sys.argv) < 4:
        print('Error: Insufficient command line parameters provided')
        print_usage()
    else:
        username = sys.argv[1].strip()
        password = sys.argv[2].strip()
        host = sys.argv[3].strip()
        retry = False

        if (len(sys.argv) >= 5):
            retry = True if (sys.argv[4].strip().lower() == 'retry') else False

        peform_inserts(username, password, host, retry)


####
# Perform the continuous database insert workload, sleeping for 10 milliseconds
# between each insert operation
####
def peform_inserts(username, password, host, retry):
    mongodb_url = f'mongodb+srv://{username}:{password}@{host}/test'\
                  f'?retryWrites={(str(retry)).lower()}'
    print(f'Connecting to:\n {mongodb_url}\n')
    connection = pymongo.MongoClient(mongodb_url)
    connection.mongo_connect.records.drop()


    connect_problem = False
    count = 0

    try:
        if (retry == True):
            connection.mongo_connect.records.insert_one({'date': datetime.datetime.now(), 'desc': 'successful insert into mongodb!', 'retry_writes': True, 'key': random.randint(1, 10000000)})
        else:
            connection.mongo_connect.records.insert_one({'date': datetime.datetime.now(), 'desc': 'successful insert into mongodb!', 'retry_writes': False, 'key': random.randint(1, 1000000)})

    except KeyboardInterrupt:
        print
        sys.exit(0)
    except Exception as e:
        print(f'*** **** *** {datetime.datetime.now()} - DB-CONNECTION-PROBLEM!: '
              f'{str(e)} *** **** ***')


####
# Print out how to use this script
####
def print_usage():
    print('\nUsage:')
    print('$ ./mongo_connect.py <username> <password> <host> <retry>')
    print('\nExample: (run script WITHOUT retryable writes enabled)')
    print('$ .mongo_connect.py main_user mypsswd '
          'testcluster-abcd.mongodb.net')
    print('\nExample: (run script WITH retryable writes enabled):')
    print('$ ./mongo_connect.py main_user mypsswd '
          'testcluster-abcd.mongodb.net retry')
    print()


####
# Main
####
if __name__ == '__main__':
    main()
