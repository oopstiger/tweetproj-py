#!/usr/bin/python
# Usage: tweet_import.py ThriftServer[:Port] PATH
#   A helper script to import Sina Weibo data.
#   This script supports rar/text files. PATH can be a directory containing
# rar/text files.

import sys
import os
import thread
import threading
import json
import time

# Thirdparty rarfile library
import rarfile

# Thrift files for HBase should be placed in the same directory with this script
import hbase

# TaskTracker
import gem

from os.path import *

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import *
from hbase.ttypes import *

def usage():
  print('Usage: tweet_import.py <ThriftServer[:Port]> <Source> [TrackerPort]')
  print('  Default port of HBase Thrift server is 9090.')
  print('  Source can be path to a .txt/.rar file or path to a directory containing .txt/.rar files.')
  print('  TrackerPort is optional. Default set to 10086')
  print('Example:')
  print('  tweet_import.py master.hadoop.lab Part1')
  print('')

class TweetsImportWorker(object):
  def __init__(self, tracker, server, port=9090, ignores=[]):
    # Task tracker
    self._tracker = tracker

    # Files that should be ignored
    self._ignores = set(ignores)

    # Progress log
    self._progf = open('log/%d.done' % os.getpid(), 'a')
    
    # By default check stop flag every 2000 records
    self._chkstep = 2000

    # HBase Thrift connection
    self._transport = TBufferedTransport(TSocket(server, port))
    self._transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
    self._client = Hbase.Client(protocol)
  
  def dispose(self):
    self._transport.close()
    self._tracker.stop()
    self._progf.close()

  def log(self, msg):
    self._tracker.log(msg)
  
  def prog(self, fname):
    ''' Log import progress. For each file, or file within a rar archive, we
    log its path after importing it. This makes it possible to recover the
    import process from interruption.
    '''
    self._progf.write(fname + '\n')
    self._progf.flush()
    #os.fsync(self._progf.fileno())

  def _af(self, mlist, t, name, cf=''):
    ''' Adds a field to mutation list.
    @mlist  target mutation list
    @t      source tweet struct
    @name   filed name
    @cf     column family, should ends with a semicolon
    '''
    if not name in t:
      mlist.append(Mutation(column=cf+name+':', value=json.dumps(None)))
    else:
      mlist.append(Mutation(column=cf+name+':', value=json.dumps(t[name]).encode('utf8')))
      
  def _create_mutations(self, t):
    ''' Creates a mutation list based on the tweet struct. '''
    mlist = []
    # basic fields
    self._af(mlist, t, 'idstr')
    self._af(mlist, t, 'text')
    self._af(mlist, t, 'created_at')
    self._af(mlist, t, 'geo')
    self._af(mlist, t, 'source')
    self._af(mlist, t, 'truncated')
    self._af(mlist, t, 'comments_count')
    self._af(mlist, t, 'reposts_count')
    
    # user fields
    user = t['user']
    self._af(mlist, user, 'idstr', 'user:')
    self._af(mlist, user, 'name', 'user:')
    self._af(mlist, user, 'gender', 'user:')
    self._af(mlist, user, 'location', 'user:')
    self._af(mlist, user, 'province', 'user:')
    self._af(mlist, user, 'city', 'user:')
    self._af(mlist, user, 'created_at', 'user:')
    self._af(mlist, user, 'description', 'user:')
    self._af(mlist, user, 'url', 'user:')
    self._af(mlist, user, 'bi_followers_count', 'user:')
    self._af(mlist, user, 'friends_count', 'user:')
    self._af(mlist, user, 'followers_count', 'user:')
    self._af(mlist, user, 'statuses_count', 'user:')
    
    # retweet fields
    if 'retweeted_status' in t:
      rt = t['retweeted_status']
      # basic fields of retweeted status
      self._af(mlist, rt, 'idstr', 'rt:')
      self._af(mlist, rt, 'text', 'rt:')
      self._af(mlist, rt, 'created_at', 'rt:')
      self._af(mlist, rt, 'geo', 'rt:')
      self._af(mlist, rt, 'source', 'rt:')
      self._af(mlist, rt, 'truncated', 'rt:')
      self._af(mlist, rt, 'comments_count', 'rt:')
      self._af(mlist, rt, 'reposts_count', 'rt:')
      
      # user fields of retweeted status
      if 'user' in rt:
        rt_user = rt['user']
        self._af(mlist, rt_user, 'idstr', 'rt:user:')
        self._af(mlist, rt_user, 'name', 'rt:user:')
        self._af(mlist, rt_user, 'gender', 'rt:user:')
        self._af(mlist, rt_user, 'location', 'rt:user:')
        self._af(mlist, rt_user, 'province', 'rt:user:')
        self._af(mlist, rt_user, 'city', 'rt:user:')
        self._af(mlist, rt_user, 'created_at', 'rt:user:')
        self._af(mlist, rt_user, 'description', 'rt:user:')
        self._af(mlist, rt_user, 'url', 'rt:user:')
        self._af(mlist, rt_user, 'bi_followers_count', 'rt:user:')
        self._af(mlist, rt_user, 'friends_count', 'rt:user:')
        self._af(mlist, rt_user, 'followers_count', 'rt:user:')
        self._af(mlist, rt_user, 'statuses_count', 'rt:user:')
    return mlist
  
  def set_chk_step(self, step = 1000):
    self._chkstep = step

  def stop_flag_is_set(self):
    return self._tracker.fstop()

  def _do_import(self, tweets, fname):
    ''' Imports data from a single text file '''
    self.log('[INFO] Processing %s, %d tweets' % (fname, len(tweets)))
    self._tracker.update_task_status(current=fname)
    self._tracker.update_progress(value=0, max=len(tweets))
    finished = True
    try:
      processed = 0
      for t in tweets:
        processed += 1
        if processed % self._chkstep == 0 and self.stop_flag_is_set():
          self.log('[INFO] Stop command detected, stop importing ...')
          finished = False
          break
        try:
          mutations = self._create_mutations(t)
          self._client.mutateRow('tweets', t['idstr'], mutations, None)
          # update record counter
          self._tracker.update_task_status(records=1)
        except Exception, e:
          self.log('[WARNING] Tweet: %s(file: %s), Exception: %s' % (t['idstr'], fname, e))
        self._tracker.update_progress(value=processed)
    except Exception, e:
      self.log('[FATAL] File: %s, Exception: %s' % (fname, e))
      finished = False

    if finished:
      # update file counter
      self._tracker.update_task_status(files=1)
    return finished

  def import_rar_file(self, fname):
    ''' Imports data from an rar file '''
    # path to status data within a rar file
    status_prefix = 'weibo_datas\\SinaNormalRobot\\Statuses\\'
    status_suffix = '.txt'
    # Firstly we check whether the rar archive has been imported.
    # Later we'll check whether the file within the archive has been imported.
    self.log('[INFO] Processing file: %s' % fname)
    if fname in self._ignores:
      self.log('[INFO] Ignored rar file: %s' % fname)
      return False
    
    finished = True
    try:
      rf = rarfile.RarFile(fname)
      for f in rf.infolist():
        if f.filename.endswith(status_suffix) and f.filename.startswith(status_prefix):
          fullname = fname + ':' + f.filename
          if fullname in self._ignores:
            self.log('[INFO] Ignored file: %s' % fullname)
          else:
            fdata = rf.read(f)
            tweets = json.loads(fdata)
            if self._do_import(tweets, fullname):
              self.prog(fullname)
            self._tracker.update_task_status(bytes=len(fdata))
        # Check for break
        if self.stop_flag_is_set():
          self.log('[INFO] Stop command detected, leaving file %s ...' % fname)
          finished = False
          break
      rf.close()
    except Exception, e:
      finished = False
      self.log('[FATAL] File: %s, Exception: %s' % (fname, e))
    
    # The whole rar file has been processed, then we log the rar file name
    if finished:
      self.prog(fname)
    return finished

  def import_txt_file(self, fname):
    ''' Imports data from an uncompressed text file '''
    self.log('[INFO] Processing file: %s' % fname)
    if fname in self._ignores:
      self.log('[INFO] Ignored file: %s' % fname)
      return False
    
    finished = True
    try:
      with open(fname) as fp:
        tweets = json.load(fp)
      if self._do_import(tweets, fname):
        self.prog(fname)
      self._tracker.update_task_status(bytes=os.stat(fname).st_size)
    except Exception, e:
      finished = False
      self.log('[FATAL] File: %s, Exception: %s' % (fname, e))
    return finished
      
  def import_file(self, fname):
    if fname.endswith('.rar'):
      return self.import_rar_file(fname)
    elif fname.endswith('.txt'):
      return self.import_txt_file(fname)
    self.log('[INFO] Unrecognized file: %s' % fname)
    return False

  def import_directory(self, dname):
    self.log('[INFO] Processing directory: %s' % dname)
    try:
      for file in os.listdir(dname):
        if self.stop_flag_is_set():
          self.log('[INFO] Stop command detected, leaving directory %s ...' % dname)
          break
        fname = join(dname, file)
        if isfile(fname):
          self.import_file(fname)
    except Exception, e:
      self.log('[FATAL] Directory: %s, Exception: %s' % (dname, e))
      return False
    return True


if __name__ == "__main__":
  numarg = len(sys.argv)
  if numarg != 3 and numarg != 4:
    usage()
    exit(1)
  
  thrift_server = sys.argv[1]
  thrift_port = '9090'
  if thrift_server.find(':') > 0:
    (thrift_server, thrift_port) = sys.argv[1].split(':')
  
  data_source = sys.argv[2]

  tracker_port = '10086'
  if numarg == 4:
    tracker_port = sys.argv[3]

  ignores = []
  try:
    with open('log/ignores', 'r') as f:
      for line in f:
        ignores.append(line.strip(' \r\n'))
  except:
    pass

  tracker = gem.TaskTracker(port=int(tracker_port))
  worker = TweetsImportWorker(tracker, thrift_server, int(thrift_port), ignores)
  
  if isdir(data_source):
    tracker.run(worker.import_directory, [data_source])
  else:
    tracker.run(worker.import_file, [data_source])

