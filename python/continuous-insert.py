#!/usr/bin/env python3
##
# Script to continuously inserts new documents into the MongoDB database/
# collection 'test.records'
#
# Prerequisite: Install latest PyMongo driver, e.g:
#   $ sudo pip3 install pymongo
#
# For usage details, run with no params (first ensure script is executable):
#   $ ./continuous-insert.py
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
    connection.test.records.drop()


    print('Inserting 1 record continuously every 10 milliseconds...')
    time.sleep(2.55)
    print('\n3')
    time.sleep(0.65)
    print('2')
    time.sleep(0.65)
    print('1')
    time.sleep(0.39)
    print('\n\nGo!!!\n\n')
    connect_problem = False
    count = 0

    while True:
        try:
            if (retry): 
                retry_writes = 'true'
            else:
                retry_writes = 'false'

            if (count % 15 == 0):
                print(f'{datetime.datetime.now()} - retry_writes: {retry_writes} - Inserted document number: {count}')

            if (retry == True):
                connection.test.records.insert_one({'date': datetime.datetime.now(), 'desc': 'This is a muti-region failover test.', 'retry_writes': True, 'key': random.randint(1, 10000000)})
            else:
                connection.test.records.insert_one({'date': datetime.datetime.now(), 'desc': 'This is a muti-region failover test.', 'retry_writes': False, 'key': random.randint(1, 1000000)})

            count += 1

            if (connect_problem):
                print(f'=== ==== === {datetime.datetime.now()} - RECONNECTED-TO-DB === ==== ===')
                connect_problem = False
            else:
                time.sleep(0.01)
        except KeyboardInterrupt:
            print
            sys.exit(0)
        except Exception as e:
            print(f'*** **** *** {datetime.datetime.now()} - DB-CONNECTION-PROBLEM!: '
                  f'{str(e)} *** **** ***')
            connect_problem = True


####
# Print out how to use this script
####
def print_usage():
    print('\nUsage:')
    print('$ ./continuous-insert.py <username> <password> <host> <retry>')
    print('\nExample: (run script WITHOUT retryable writes enabled)')
    print('$ ./continuous-insert.py main_user mypsswd '
          'testcluster-abcd.mongodb.net')
    print('\nExample: (run script WITH retryable writes enabled):')
    print('$ ./continuous-insert.py main_user mypsswd '
          'testcluster-abcd.mongodb.net retry')
    print()


####
# Main
####
if __name__ == '__main__':
    main()
