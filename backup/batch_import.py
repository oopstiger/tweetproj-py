#!/usr/bin/python
# usage: batch_import.py DIR LIST [NTH]
#   A helper script to import Sina Weibo data. This script supports text files
# only. To import rar files directly, use tweet_import.py instead.
#   This script invokes 'txt_import.py' to import data into HBase. Please
# investigate txt_import.py for more information.

import sys
import os

def usage():
  print('Usage: batch_import.py DIR LIST [NTH]')
  print('  DIR   Path to the directory that contains data to be imported.')
  print('  LIST  A file that contains a list of names of files to be imported,')
  print('        one file name per line.')
  print('  NTH   A number that specifies how many threads to use')
  print('        default 4. Should be in range [1, 8]')
  print('')
  print('  Files in LIST that nonexist or that had been imported will be ignored.')
  print('  For each file \'X\', a file named \'X.done\' will be created if ')
  print('    file \'X\' is imported successfully.')
  print('Example:')
  print('  To import all files in data1 directory, invoke')
  print('    ls data1/ > files ; python batch_import.py data1 files')


# Program starts here
#
if len(sys.argv) < 3:
  usage()
  exit()

# number of threads to use
nth = 4
if len(sys.argv) == 4:
  try:
    nth = int(sys.argv[3])
  except:
    nth = 0
  if nth < 1 or nth > 8:
    print('Invalid argument, NTH must be a number in range [1,8]')
    usage()
    exit()

datadir = sys.argv[1]
if not datadir.endswith('/'):
  datadir += '/'

# ignore files that nonexist or that have been imported
# for each FILE there will be a FILE.done if the FILE has been imported
fp = open(sys.argv[2])
flist = fp.readlines()
fp.close()
files = []
for f in flist:
  f = datadir + f.replace('\r', '').replace('\n', '')
  if len(f) == 0:
    continue
  if not os.path.exists(f):
    print('-file %s not exist, ignored...' % f)
    continue
  if os.path.exists(f + '.done'):
    print('-file %s already imported, ignored...' % f)
    continue
  print('+file %s pushed back...' % f)
  files.append(f)

if len(files) == 0:
  print('Nothing to do.')
else:
  print('-- %d files in task queue, using %d threads --' % (len(files), nth))

n = 0
j = (len(files) + nth - 1)/nth

# dummy names
for d in range(0, j*nth - len(files)):
  files.append(' ')

while n < j:
  cmd = 'python txt_import.py'
  for s in range(0, nth):
    cmd += ' '
    cmd += files[n*nth+s]
  print(cmd)
  os.system(cmd)
  n += 1

