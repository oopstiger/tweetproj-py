#!/usr/bin/python
# usage: txt_import.py FILE1 [FILE2 [FILE3] ...]
#   A helper script to import Sina Weibo data. This script supports text files
# only. To import rar files directly, use tweet_import.py instead.

import os
import sys
import thread
import threading
import json

# Thrift files for HBase should be in the same directory with this script
import hbase

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import *
from hbase.ttypes import *

# Target thrift server.
# A thrift server is not necessarily run on HBase/Hadoop master node.
THRIFT_SERVER = 'master.hadoop.lab'
THRIFT_PORT = 9090

print_lock = thread.allocate_lock()

def usage():
  print('Usage: txt_import.py FILE1 [FILE2 [FILE3] ...]')
  print('  A worker thread will be created for each of input file.')
  print('  DO NOT pass too many files.')

def add_field(mlist, t, name, cf=''):
  if not name in t:
    mlist.append(Mutation(column=cf+name+':', value=json.dumps(None)))
  else:
    mlist.append(Mutation(column=cf+name+':', value=json.dumps(t[name]).encode('utf8')))

def create_mutations(t):
  mlist = []
  
  # basic fields
  add_field(mlist, t, 'idstr')
  add_field(mlist, t, 'text')
  add_field(mlist, t, 'created_at')
  add_field(mlist, t, 'geo')
  add_field(mlist, t, 'source')
  add_field(mlist, t, 'truncated')
  add_field(mlist, t, 'comments_count')
  add_field(mlist, t, 'reposts_count')
  
  # user fields
  user = t['user']
  add_field(mlist, user, 'idstr', 'user:')
  add_field(mlist, user, 'name', 'user:')
  add_field(mlist, user, 'gender', 'user:')
  add_field(mlist, user, 'location', 'user:')
  add_field(mlist, user, 'province', 'user:')
  add_field(mlist, user, 'city', 'user:')
  add_field(mlist, user, 'created_at', 'user:')
  add_field(mlist, user, 'description', 'user:')
  add_field(mlist, user, 'url', 'user:')
  add_field(mlist, user, 'bi_followers_count', 'user:')
  add_field(mlist, user, 'friends_count', 'user:')
  add_field(mlist, user, 'followers_count', 'user:')
  add_field(mlist, user, 'statuses_count', 'user:')
  
  # retweet fields
  if 'retweeted_status' in t:
    rt = t['retweeted_status']
    # basic fields of retweeted status
    add_field(mlist, rt, 'idstr', 'rt:')
    add_field(mlist, rt, 'text', 'rt:')
    add_field(mlist, rt, 'created_at', 'rt:')
    add_field(mlist, rt, 'geo', 'rt:')
    add_field(mlist, rt, 'source', 'rt:')
    add_field(mlist, rt, 'truncated', 'rt:')
    add_field(mlist, rt, 'comments_count', 'rt:')
    add_field(mlist, rt, 'reposts_count', 'rt:')
    
    # user fields of retweeted status
    if 'user' in rt:
      rt_user = rt['user']
      add_field(mlist, rt_user, 'idstr', 'rt:user:')
      add_field(mlist, rt_user, 'name', 'rt:user:')
      add_field(mlist, rt_user, 'gender', 'rt:user:')
      add_field(mlist, rt_user, 'location', 'rt:user:')
      add_field(mlist, rt_user, 'province', 'rt:user:')
      add_field(mlist, rt_user, 'city', 'rt:user:')
      add_field(mlist, rt_user, 'created_at', 'rt:user:')
      add_field(mlist, rt_user, 'description', 'rt:user:')
      add_field(mlist, rt_user, 'url', 'rt:user:')
      add_field(mlist, rt_user, 'bi_followers_count', 'rt:user:')
      add_field(mlist, rt_user, 'friends_count', 'rt:user:')
      add_field(mlist, rt_user, 'followers_count', 'rt:user:')
      add_field(mlist, rt_user, 'statuses_count', 'rt:user:')
  return mlist

def do_import(n, filename):
  try:
    transport = TBufferedTransport(TSocket(THRIFT_SERVER, THRIFT_PORT))
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    
    fp = open(filename)
    tweets = json.load(fp)
    fp.close()
    for t in tweets:
      try:
        mutations = create_mutations(t)
        client.mutateRow('tweets', t['idstr'], mutations, None)
      except Exception, e:
        with print_lock:
          print('[WARNING]Tweet id:%s (file: %s) caused an exception: %s' % (t['idstr'], filename, e))
    try:
      open(filename + '.done', 'w').close()
    except:
      # ignored
      pass
  except Exception, e:
    with print_lock:
      print('[FATAL]Thread %d aborting, file: %s, exception: %s' % (n, filename, e))


# Program starts here
#
if len(sys.argv) <= 1 or sys.argv[1] == '--help' or sys.argv[1] == '-help':
  usage()
  exit()

threads = []
for n in range(1, len(sys.argv)):
  th = threading.Thread(target=do_import, args=(n, sys.argv[n]))
  th.start()
  threads.append(th)

for th in threads:
  th.join()

print('Mission complete.')
