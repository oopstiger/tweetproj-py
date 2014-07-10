#!/usr/bin/python
# This file is part of tweetproj

import sys
import os
import thread
import threading
import time
import socket
import cgi

from threading import Thread
from threading import Event

def getcmdline():
  ''' Get command line of current python program '''
  cmdline = ''
  for argv in sys.argv:
    cmdline += argv
    cmdline += ' '
  return cmdline

def closesocket(sock):
  ''' Shutdown and close the socket. '''
  try:
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
  except Exception, e:
    pass

def get_http_target(requestline):
  requestline = requestline.replace('\t', ' ')
  parts = requestline.split(' ')
  method = ''
  target = ''
  cursor = 0
  for e in parts:
    if cursor == 0:
      if e != '':
        method = e
        cursor += 1
    elif cursor == 1:
      if e != '':
        target = e
        cursor += 1
    else:
      break
  return [method, target]

# See: http://stackoverflow.com/questions/1094841
# Origin:  http://blogmag.net/blog/read/38/Print_human_readable_file_size
def filesizefmt(size):
  for x in ['bytes','KB','MB','GB','TB']:
      if size < 1024.0:
          return "%3.1f %s" % (size, x)
      size /= 1024.0

class TaskInformation(object):
  def __init__(self):
    self.pid = os.getpid()
    self.cmdline = getcmdline()
    self.startup_at = ''
    self.updated_at = ''
    self.files_processed = 0
    self.records_processed = 0
    self.bytes_processed = 0L
    self.current_file = ''
    self.output = ''
    self.prog_max = 100 # progress max
    self.prog_val = 0   # progress value


class TaskTracker(object):
  def __init__(self, addr = '0.0.0.0', port = 10086):
    self._taskth = None  # task thread
    self._tasklog = []
    self._tasklog_lock = thread.allocate_lock()
    self._tasklogf = open('log/gem.%d.log' % os.getpid(), 'w')
    self.taskinf = TaskInformation()

    self._waddr = addr
    self._wport = port
    self._sock = None    # listening socket
    self._ctlth = None   # controller thread
    self._ctlf = Event()

    self._html = ''
    with open('gem.htm') as h:
      self._html = h.read()
  
  def _ctl_event_loop(self):
    self.log('[INFO] Starting gem control interface at: http://%s:%s' % (self._waddr, self._wport))
    conn = None
    while self._taskth.is_alive():
      try:
        (conn, addr) = self._sock.accept()
      except socket.timeout, e:
        continue
      except Exception, e:
        # exit event loop on server side network errors
        self.log('[ERROR] gem control interface down! Network error: %s' % e)
        break

      try:
        conn.settimeout(3)
        requestline = conn.recv(1024).splitlines()[0]
        conn.settimeout(None)
        [method, target] = get_http_target(requestline)
        if method == 'GET':
          if target == '/':
            conn.send(self.response_summary())
          else:
            conn.send(self.response_file('.' + target))
        elif method == 'POST':
          if target == '/ctl/stop':
            self.log('[INFO] gem control interface got a STOP command.')
            self._ctlf.set()
            conn.send('HTTP/1.1 200 OK\r\nServer: gem\r\n\r\n')
          else:
            conn.send(self.response_400())
        else:
          conn.send(self.response_400())
      except Exception, e:
        # ignore client side network errors
        pass
      finally:
        closesocket(conn)
    
  def run(self, target, args):
    ''' Run the task and tracker '''
    if self._taskth != None or self._ctlth != None:
      self.log('[ERROR] TaskTracker is already running.')
      return 1

    try:
      self._sock = socket.socket()
      self._sock.bind((self._waddr, self._wport))
      self._sock.listen(4)
      self._sock.settimeout(3)
    except Exception, e:
      self.log('[ERROR] Failed to start task tracker, network error: %s' % e)
      return 1

    self.taskinf.pid = os.getpid()
    self.taskinf.cmdline = getcmdline()
    self.taskinf.startup_at = time.strftime('%Y-%m-%d %H:%M:%S')
    self.taskinf.updated_at = time.strftime('%Y-%m-%d %H:%M:%S')
    self.log('[INFO] Starting TaskTracker PID=%d, running command %s' % (self.taskinf.pid, self.taskinf.cmdline))
    self._taskth = Thread(target=target, args=args)
    self._taskth.start()

    self._ctlf.clear()
    self._ctlth = Thread(target=self._ctl_event_loop)
    self._ctlth.start()
    self._taskth.join()
    
    # controller thread exits automatically after task thread is terminated
    self._ctlth.join()
    self._sock.close()

    if self.fstop():
      self.log('[INFO] Mission aborted.')
    else:
      self.log('[INFO] Mission complete.')
    return 0

  def compose_200_header(self, clen, ctype):
    header = 'HTTP/1.1 200 OK\r\n'\
             'Server: gem\r\n'\
             'Connection: close\r\n'\
             'Content-Length: %d\r\n'\
             'Content-Type: %s\r\n\r\n' % (clen, ctype)
    return header

  def response_400(self):
    header = 'HTTP/1.1 400 Bad Request\r\n'\
             'Server: gem\r\n'\
             'Connection: close\r\n\r\n'
    return header

  def sync_output(self):
    self.taskinf.output = ''
    with self._tasklog_lock:
      for l in self._tasklog:
        self.taskinf.output += l
    self.taskinf.output = cgi.escape(self.taskinf.output, True)

  def response_summary(self, autorefresh = 5):
    refresh_meta = ''
    if autorefresh > 0:
      refresh_meta = '<meta http-equiv="refresh" content="%d">' % autorefresh

    task_status = '<label>Running</label>'
    if self.fstop():
      task_status = '<label class="attention">Waiting to stop</label>'

    self.sync_output()
    inf = self.taskinf

    loganchor = '<a href="/log/gem.%d.log">log/gem.%d.log</a>' % (inf.pid, inf.pid)

    body = self._html % (refresh_meta, inf.pid, inf.cmdline, inf.startup_at, \
           inf.updated_at, inf.files_processed, inf.records_processed, \
           filesizefmt(inf.bytes_processed), inf.current_file, inf.prog_val, \
           inf.prog_max, task_status, inf.output, loganchor)
    header = self.compose_200_header(len(body), 'text/html')
    return header + body

  def response_file(self, fname, ctype=None):
    if ctype == None:
      if fname.endswith('.htm') or fname.endswith('.html'):
        ctype = 'text/html'
      elif fname.endswith('.txt') or fname.endswith('.log') or \
           fname.endswith('.py') or fname.endswith('.sh') :
        ctype = 'text/plain'
      else:
        ctype = 'application/octet-stream'
    response = ''
    try:
      with open(fname) as f:
        body = f.read()
      header = self.compose_200_header(len(body), ctype)
      response = header + body
    except:
      response = 'HTTP/1.1 404 Not Found\r\nServer: gem\r\n\r\n'
    return response

  def fstop(self):
    ''' Check if the stop flag is set '''
    return self._ctlf.is_set()

  def log(self, msg):
    try:
      logline = time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + msg + '\n'
      with self._tasklog_lock:
        self._tasklog.append(logline)
        if len(self._tasklog) > 80:
          self._tasklog.pop(0)
      self._tasklogf.write(logline)
      self._tasklogf.flush()
    except:
      pass

  def update_task_status(self, files = None, records = None, bytes = None, current = None):
    ''' Updates task status '''
    if files != None:
      self.taskinf.files_processed += files
    if records != None:
      self.taskinf.records_processed += records
    if bytes != None:
      self.taskinf.bytes_processed += bytes
    if current != None:
      self.taskinf.current_file = current
    self.taskinf.updated_at = time.strftime('%Y-%m-%d %H:%M:%S')

  def update_progress(self, value, max = None):
    self.taskinf.prog_val = value
    if max != None:
      self.taskinf.prog_max = max
    self.taskinf.updated_at = time.strftime('%Y-%m-%d %H:%M:%S')


# This is for debug only
def task_for_test(tracker):
  while not tracker.fstop():
    tracker.log('Put a log message')
    tracker.update_task_status(files=1, records=2)
    tracker.update_progress(tracker.taskinf.files_processed, 100)
    time.sleep(3)
  pass

if __name__ == "__main__":
  try:
    tracker = TaskTracker()
    args = [tracker]
    tracker.run(task_for_test, args)
    print('TaskTracker exited.')
  except Exception, e:
    print('Exception caught: %s' % e)
