# logmung.py
#
# An apache log processing pipeline with coroutines.  based on http://www.dabeaz.com/coroutines/
# To run this, you will need a log file.

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

# A data source.  This is not a coroutine, but it sends
# data into one (target)

import time
def follow(thefile, target):
    thefile.seek(0,2)      # Go to the end of the file
    while True:
         line = thefile.readline()
         if not line:
             time.sleep(0.1)    # Sleep briefly
             continue
         target.send(line)
         
# Another data source.  This is not a coroutine, but it sends
# data into one (target)
         
def readfile(thefile, target):
    while True:
         line = thefile.readline()
         if not line:
             break
         target.send(line)

# A filter that matches text

@coroutine
def match(pattern,target):
    while True:
        line = (yield)           # Receive a line
        if pattern in line:
            target.send(line)    # Send to next stage

# A filter that outputs when text doesn't match

@coroutine
def nomatch(pattern,target):
    while True:
        line = (yield)           # Receive a line
        if pattern not in line:
            target.send(line)    # Send to next stage
            
# A filter that turns apache log lines into tab separated ones
import re
from dateutil.parser import parse
logpattern = re.compile(r'^([0-9.]+)[^\[]*\[([^\]]*)\]\s"([^"]*)"\s([0-9]+)\s([0-9]+)\s"([^"]*)"\s"([^"]*)"')
        
@coroutine
def logtotsv(target):
    while True:
        line = (yield)           # Receive a line
        result = logpattern.match(line)
        fields = list(result.groups())
        #turn apache's godawful date format into ISO, then mung that into spreadsheet compatible datetime by removing TZ
        datefield = fields[1]
        isodate = str(parse(datefield[:11]+" "+datefield[12:]))
        fields[1] = isodate.split('+')[0]
        if result:
            target.send("\t".join(fields)+'\n')    # Send to next stage

# A sink.  A coroutine that receives data

@coroutine
def printer():
    while True:
         line = (yield)
         print line,

# Example use
if __name__ == '__main__':
    logfile  = open("access.log.1")
    readfile(logfile,
        match("client-config.json", #pass on lines that have this in
        nomatch("retaildemo", #pass on lines that don't have this in
        logtotsv( #change to Tab-separated for spreadsheets
        printer() #output
        ))))
