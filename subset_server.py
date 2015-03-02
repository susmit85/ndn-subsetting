#!/usr/bin/python2.7
# -*- Mode:python; c-file-style:"gnu"; indent-tabs-mode:nil -*- */
#
# Copyright (C) 2014 Regents of the University of California.
# Author: Jeff Thompson <jefft0@remap.ucla.edu>
# Author: Susmit Shannigrahi <susmit@cs.colostate.edu>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# A copy of the GNU General Public License is in the file COPYING.

import time
import sys
import traceback
from pyndn import Name
from pyndn import Data
from pyndn import Face
from pyndn.security import KeyChain
import netCDF4
import os
import argparse

filename = ''
def dump(*list):
    result = ""
    for element in list:
        result += (element if type(element) is str else repr(element)) + " "
    print(result)

class Sign(object):
    global filename
    def __init__(self, keyChain, certificateName):
        try:
           self.tmp_filename = filename +'_tmp.nc'

        except:
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)

        self._keyChain = keyChain
        self._certificateName = certificateName
        self._responseCount = 0
        self.first_read = 0
        self.total_size = 0
        self.remaining_size = 0
        
        self.f = open(self.tmp_filename, 'wb')
        self.f.close()


    def onInterest(self, prefix, interest, transport, registeredPrefixId):
        self._responseCount += 1
        #FILE *f;

        # Make and sign a Data packet.
        data = Data(interest.getName())
#`        content = "Sign " + interest.getName().toUri()
        interest_name = interest.getName().getPrefix(interest.getName().size() - 1).toUri()
#        interest_name =  interest.getName().toUri()
        last_component = interest.getName().toUri().split('/')[-1]

        print("interest name", interest_name, last_component, filename)
        
        #if first interest, generate files
        if last_component == '%00%00':
            var_name, var_value= interest_name.split('_')
            var_name = var_name.split('/')[-1]
            year, month, day = var_value.split('/')
            print("\n\nReceived interest ", interest_name)

            #there are four values for each day interest name, respectively
            #then extract the value and write to a file
            try:
                nc_f =  filename
                nc_fid = netCDF4.Dataset(nc_f, 'r')
                pr = nc_fid.variables['pr']
                nc_time = nc_fid.variables['time']
                pr_new = []
                #print("pr", pr, len(pr))
		#each day has 4 readings, for day 31, the indexes are 120-124
                for i in range((int(day)-1)*4, int(day)*4):
                 #   print("values = ",  i, pr[i])
                    pr_new = pr[i]

                print("Extracting subset..")
                w_nc_fid = netCDF4.Dataset(self.tmp_filename, 'w', format='NETCDF4')

                # Using our previous dimension info, we can create the new time dimension
                # Even though we know the size, we are going to set the size to unknown
                w_nc_fid.createDimension('time', None)
                w_nc_dim = w_nc_fid.createVariable('time', nc_fid.variables['time'].dtype,\
                                                         ('time',))

                w_nc_fid.variables['time'][:] = nc_time
                # Assign the dimension data to the new NetCDF file.
                w_nc_var = w_nc_fid.createVariable('pr', 'f8', ('time'))
                w_nc_fid.variables['pr'][:] = pr_new
                w_nc_fid.close()  # close the new file
                print("Extraction finished...returning data")

                ##todo:open the new file and send first chunk back
                #self.first_read = 1
                self.total_size = os.path.getsize(self.tmp_filename)

                self.f = open(self.tmp_filename, 'rb')
                    
            except:
                traceback.print_exc(file=sys.stdout)
                sys.exit(1)
#        else:
#            #todo:send the subsequent chunks
#            return_bytes = f.read(8)
#            data.setContent(content)
        

        #print("self.first_read", self.first_read)
                #send back 8 bytes
        return_bytes = self.f.read(8192)
        print('\rSent so far:', self.f.tell())
        if self.f.tell() == self.total_size:
            #print("at", self.f.tell())
            data.getMetaInfo().setFinalBlockID(interest.getName()[-1])
            print("\rSending final pipelined packet for ", interest_name)
#            print("Sent total %s bytes" %(self.total_size))
        data.setContent(return_bytes)

        self._keyChain.sign(data, self._certificateName)
        encodedData = data.wireEncode()
#        dump("Sent content", return_bytes)
        transport.send(encodedData.toBuffer())

    def onRegisterFailed(self, prefix):
        self._responseCount += 1
        dump("Register failed for prefix", prefix.toUri())



def main():
    global filename
    parser = argparse.ArgumentParser()  
    parser.add_argument("-f ", "--filename", help="file for subsetting", required=True)
    args = parser.parse_args()
    filename =  args.filename

    # The default Face will connect using a Unix socket, or to "localhost".
    face = Face()

    # Use the system default key chain and certificate name to sign commands.
    keyChain = KeyChain()
    face.setCommandSigningInfo(keyChain, keyChain.getDefaultCertificateName())
    
    # Also use the default certificate name to sign data packets.
    echo = Sign(keyChain, keyChain.getDefaultCertificateName())
    prefix = Name("/ndn/colostate.edu/netsec")
    #prefix = Name("/ndn/atmos")
    dump("Register prefix...registration done", prefix.toUri())
    face.registerPrefix(prefix, echo.onInterest, echo.onRegisterFailed)

    while True:
        face.processEvents()
        # We need to sleep for a few milliseconds so we don't use 100% of the CPU.
        time.sleep(0.01)    

    #face.shutdown()

if __name__ == '__main__':
    main()

