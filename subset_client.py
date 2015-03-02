#!/usr/bin/python2.7
# -*- Mode:python; c-file-style:"gnu"; indent-tabs-mode:nil -*- */
#
# Copyright (c) 2013-2014 Regents of the University of California.
#
# This file is part of ndn-cxx library (NDN C++ library with eXperimental eXtensions).
#
# ndn-cxx library is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later version.
#
# ndn-cxx library is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
#
# You should have received copies of the GNU General Public License and GNU Lesser
# General Public License along with ndn-cxx, e.g., in COPYING.md file.  If not, see
# <http://www.gnu.org/licenses/>.
#
# See AUTHORS.md for complete list of ndn-cxx authors and contributors.
#
# @author Wentao Shang <http://irl.cs.ucla.edu/~wentao/>
# @author Steve DiBenedetto <http://www.cs.colostate.edu/~dibenede>
# @author Susmit Shannigrahi <http://www.cs.colostate.edu/~susmit>
#/

import sys
import time
import os
import random
import subprocess
import glob
import datetime

from pyndn import Interest
from pyndn import Name
from pyndn import Face
from pyndn.encoding.tlv.tlv_encoder import TlvEncoder
from pyndn.util.blob import Blob


class Consumer(object):

    def __init__(self, prefix, tmp_filename, pipeSize, onDataReceived=None, mustBeFresh=True):
        self.prefix = prefix
        self.pipeSize = pipeSize
        self.nextSegment = 0
        self.totalSize = 0
        self.mustBeFresh = mustBeFresh
        self.face = Face("127.0.0.1")
        self.retry = 0
        self.isDone = False
        self.onDataReceived = onDataReceived
        tmp_filename = ('_').join(tmp_filename.split('/'))
        self.write_data = open(tmp_filename, "wb")
#        self.write_data.close()
#        self.write_data = open(tmp_filename, "ab")


    def run(self):
        try:
            for i in range(self.pipeSize):
                self._sendNextInterest1(self.prefix)
                #print("prefix", self.prefix)
                self.nextSegment += 1

            while not self.isDone:
                self.face.processEvents()
                time.sleep(0.01)
            self.write_data.close()

        except RuntimeError as e:
            print(("ERROR: %s" %  e))

        

    def _onData(self, interest, data):
        self.retry = 0
        content = data.getContent()
        name = data.getName()
        self.write_data.write(content.toBuffer())

        #print("received %s" % data.getName().toUri())
        self.totalSize += len(content)
        #print("\rBytes received so far:", self.totalSize)

        if self.onDataReceived is not None:
            self.onDataReceived(data)

        if data.getMetaInfo().getFinalBlockID() == data.getName()[-1]:
           print("\nLast segment received for %s" %data.getName().getPrefix(interest.getName().size() - 1).toUri())
           print(("Total %s bytes of content received" % self.totalSize))
           self.isDone = True
        else:
           self.nextSegment += 1
           self._sendNextInterest1(self.prefix)
           #self._sendNextInterestWithSegment(self.prefix, False)
           
            
    def _sendNextInterest1(self, name, exclude=None):
        '''send out the next interest, after setting the actual parameters'''
        nextName = Name(name).appendSegment(self.nextSegment)
        interest1 = Interest(nextName)
        interest1.setMustBeFresh(False);
        interest1.setInterestLifetimeMilliseconds(1000)
        self.face.expressInterest(interest1, self._onData, self._onTimeout)


    def _onTimeout(self, interest):
        self.retry += 1
        print(("TIMEOUT: segment #%s" % interest.getName()[-1].toNumber()))

        if self.retry < 3:
            self.face.expressInterest(interest, self._onData, self._onTimeout)
        else:
            self.isDone = True

def usage(progname):
  print("Usage: %s /ndn/name pipe_size" % progname)
  return 1


def check_input(year, month, day):
    if int(month) > 12 or int(month) < 0 or int(day) < 0 or int(day) > 31:
        print("Error: enter valid date")
        sys.exit(1);

    if(year != "1902" and year != "1901"):
        print("Error: Datafiles has only 1901 or 1902's data")
        sys.exit(1);

if __name__ == "__main__":

    pipeSize = 4
    namespace = "/ndn/colostate.edu/netsec"
    
    print '\n Data files have readings for January and February, 1902 \n'

    start_date = raw_input("Start Date in YYYY/MM/DD?")
    year, month, day = start_date.split("/")
    check_input(year, month, day)
    end_date = raw_input("End Date in YYYY/MM/DD? ")
    year_end, month_end, day_end = end_date.split("/")
    check_input(year_end, month_end, day_end)

    month = int(month)
    day = int(day)

    month_end = int(month_end)
    day_end = int(day_end)


    #there is no zero-th month, make index 0 = 0
    months = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if( int(year) % 4 == 0):
        if int(year[2]) + int(year[3]) != 0:
            months[1] = 29;
        elif (int(year) % 400 == 0 ):
            months[1] = 29;

    year = int(year)
    year_end = int(year_end)
    

    #enumerate rest of the dates
    if(month_end > month):
        for item in range(day, months[month]+1):
            #time.sleep(1)
            filename =  namespace + ("/pr_%d/%02d/%02d") %(year, month, item)
            tmp_filename = ("pr_%d_%02d_%02d.subset.nc") %(year, month, item)
            x = time.time()
            print("\n\nAsking for %s, Saving to %s" %(filename, tmp_filename))
            Consumer(filename, tmp_filename, pipeSize).run()
            print("Time for %s %s  " %(tmp_filename, time.time() - x))


        for item in range(1, day_end+1):
            #time.sleep(1)
            filename =  namespace + ("/pr_%d/%02d/%02d") %(year, month_end, item)
            tmp_filename = ("pr_%d_%02d_%02d.subset.nc") %(year_end, month_end, item)
            x = time.time()
            print("\n\nAsking for %s, Saving to %s" %(filename, tmp_filename))
            Consumer(filename, tmp_filename,  pipeSize).run()
            print("Time for %s.tmp.nc %s  " %(tmp_filename, time.time() - x))
    else:

        for item in range(day, day_end+1):
            filename =  namespace + ("/pr_%d/%02d/%02d") %(year, month, item)
            tmp_filename = ("pr_%d_%02d_%02d.subset.nc") %(year, month, item)
            
            x = time.time()
            print("\n\nAsking for %s, Saving to %s" %(filename, tmp_filename))
            #command1 = subprocess.Popen(["./client", filename, tmp_filename+".tmp.nc"], stdout=subprocess.PIPE)
            #out, err = command1.communicate()
            print("Time for %s.tmp.nc %s " %(tmp_filename, time.time() - x))

            Consumer(filename, tmp_filename, pipeSize).run()

