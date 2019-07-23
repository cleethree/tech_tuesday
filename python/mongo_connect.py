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

        connect_to_mongo(username, password, host, retry)


####
# Connect to MongoDB and insert 1 document.
####
def connect_to_mongo(username, password, host, retry):
    mongodb_url = f'mongodb+srv://{username}:{password}@{host}/test'\
                  f'?retryWrites={(str(retry)).lower()}'
    print(f'Connecting to:\n {mongodb_url}\n')
    
    ####
    #Additional Options
    ####
    
    #maxPoolSize (optional): The maximum allowable number of concurrent connections to each connected server. Requests to a server will block if there are maxPoolSize outstanding connections to the requested server. Defaults to 100. Cannot be 0.
    max_pool_size = 250
    
    #minPoolSize (optional): The minimum required number of concurrent connections that the pool will maintain to each connected server. Default is 0.
    min_pool_size = 1
    
    #maxIdleTimeMS (optional): The maximum number of milliseconds that a connection can remain idle in the pool before being removed and replaced. Defaults to None (no limit).
    max_idle_time_ms = 1
    
    #socketTimeoutMS: (integer or None) Controls how long (in milliseconds) the driver will wait for a response after sending an ordinary (non-monitoring) database operation before concluding that a network error has occurred. Defaults to None (no timeout).
    
    #connectTimeoutMS: (integer or None) Controls how long (in milliseconds) the driver will wait during server monitoring when connecting a new socket to a server before concluding the server is unavailable. Defaults to 20000 (20 seconds).
    
    #serverSelectionTimeoutMS: (integer) Controls how long (in milliseconds) the driver will wait to find an available, appropriate server to carry out a database operation; while it is waiting, multiple server monitoring operations may be carried out, each controlled by connectTimeoutMS. Defaults to 30000 (30 seconds).

    #waitQueueTimeoutMS: (integer or None) How long (in milliseconds) a thread will wait for a socket from the pool if the pool has no free sockets. Defaults to None (no timeout).

    #waitQueueMultiple: (integer or None) Multiplied by maxPoolSize to give the number of threads allowed to wait for a socket at one time. Defaults to None (no limit).

    #heartbeatFrequencyMS: (optional) The number of milliseconds between periodic server checks, or None to accept the default frequency of 10 seconds.
    
    #retryWrites: (boolean) Whether supported write operations executed within this MongoClient will be retried once after a network error on MongoDB 3.6+. Defaults to False.
    
    #compressors: Comma separated list of compressors for wire protocol compression. The list is used to negotiate a compressor with the server. Currently supported options are “snappy” and “zlib”. Support for snappy requires the python-snappy package. zlib support requires the Python standard library zlib module. By default no compression is used. Compression support must also be enabled on the server. MongoDB 3.4+ supports snappy compression. MongoDB 3.6+ supports snappy and zlib.

    #zlibCompressionLevel: (int) The zlib compression level to use when zlib is used as the wire protocol compressor. Supported values are -1 through 9. -1 tells the zlib library to use its default compression level (usually 6). 0 means no compression. 1 is best speed. 9 is best compression. Defaults to -1.
    zlib_compression_level=9
    
    #uuidRepresentation: The BSON representation to use when encoding from and decoding to instances of UUID. Valid values are pythonLegacy (the default), javaLegacy, csharpLegacy and standard. New applications should consider setting this to standard for cross language compatibility.
    
    #localThresholdMS
    local_threshold_ms = 35
    
    ####
    #Write Concern Options
    ####
    
    #w: (integer or string) If this is a replica set, write operations will block until they have been replicated to the specified number or tagged set of servers. w=<int> always includes the replica set primary (e.g. w=3 means write to the primary and wait until replicated to two secondaries). Passing w=0 disables write acknowledgement and all other write concern options.
    w="majority"
    
    #wtimeout: (integer) Used in conjunction with w. Specify a value in milliseconds to control how long to wait for write propagation to complete. If replication does not complete in the given timeframe, a timeout exception is raised.
    
    #j: If True block until write operations have been committed to the journal. Cannot be used in combination with fsync. Prior to MongoDB 2.6 this option was ignored if the server was running without journaling. Starting with MongoDB 2.6 write operations will fail with an exception if this option is used when the server is running without journaling.
    
    #fsync: If True and the server is running without journaling, blocks until the server has synced all data files to disk. If the server is running with journaling, this acts the same as the j option, blocking until write operations have been committed to the journal. Cannot be used in combination with j.
    
    ####
    #Read Preference/Concern Options
    ####
    
    #readPreference: The replica set read preference for this client. One of primary, primaryPreferred, secondary, secondaryPreferred, or nearest. Defaults to primary.
    read_preference='secondaryPreferred'
    
    #readPreferenceTags: Specifies a tag set as a comma-separated list of colon-separated key-value pairs. For example dc:ny,rack:1. Defaults to None.
    
    #maxStalenessSeconds: (integer) The maximum estimated length of time a replica set secondary can fall behind the primary in replication before it will no longer be selected for operations. Defaults to -1, meaning no maximum. If maxStalenessSeconds is set, it must be a positive integer greater than or equal to 90 seconds.
    
    
    #readConcernLevel: (string) The read concern level specifies the level of isolation for read operations. For example, a read operation using a read concern level of majority will only return data that has been written to a majority of nodes. If the level is left unspecified, the server default will be used.
    
  
    
    connection = pymongo.MongoClient(mongodb_url,
                                     localThresholdMS=local_threshold_ms, 
                                     readPreference=read_preference,
                                     maxPoolSize=max_pool_size,
                                     minPoolSize=min_pool_size,
                                     zlibCompressionLevel=zlib_compression_level,
                                     w=w
                                    )
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
        
        
    return connection


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
