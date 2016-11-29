#!/usr/bin/python

import getopt
import sys
import datetime
from multiprocessing import Pool

def usage():
    print "task01_naive.py -i <input> -o <output>"
    sys.exit(1)

def ToDateEpochString(t):
    dt = datetime.datetime.strptime(t, '%Y%m%d%H%M%S%f')
    epoch = datetime.datetime.utcfromtimestamp(0)
    us = t[14:]
    result = str(int((dt - epoch).total_seconds())) + us.zfill(6)
    #return t
    return result
    
def ComputeLimitRangeString(lp, hp):
    #return lp
    if (lp == "NULL" or hp == "NULL"):
        return "NULL"
    if (lp[-8] != '.'):
        assert check(), "Bug in input, num decimal places != 7"
    if (hp[-8] != '.'):
        assert check(), "Bug in input, num decimal places != 7"

    #print "Upper: "+ lp[-7:]
    #print "Lower: "+ lp[:-8]

    # use python's floating pointer operations and format to 6 decimal places
    range = float(hp) - float(lp)
    return "{:.6f}".format(range)
    
    #llp = int(lp[:-8] + lp[-7:])
    #lhp = int(hp[:-8] + hp[-7:])
    #lr = lhp - llp
    #range = str(lr)
    #result = range[:-7] + "." + range[-7:]
    #return result

def process(line):
    # This sb variable holds the string that contains the formatted message
    # Which will be written into the output file
    sb = ""
    
    # these special variables are used to formate the final output
    tag48 = ""
    r48 = False
    tag55 = ""
    r55 = False
    tag779 = ""
    r779 = False
    tag1148 = ""
    r1148 = False
    tag1149 = ""
    r1149 = False
    tag1150 = ""
    r1150 = False
    
    # Each Message contains multiple fields
    # Each Field is terminated by an SOH character (0x01)
    fields = line.split('\x01')
    
    # Each Field is in the Tag=Value format
    for f in fields:
        # Tag is an integer from 1 to 99999
        # Value can have many different types
        if (f != "\n"):
            pps = f.split('=')                
            t = pps[0].rstrip()
            v = pps[1].rstrip()
            if (t == "48"):
                tag48 = v
                r48 = True
            elif (t == "55"):
                tag55 = v
                r55 = True
            elif (t == "779"):
                tag779 = v
                r779 = True
            elif (t=="1148"):
                tag1148 = v
                r1148 = True
            elif (t=="1149"):
                tag1149 = v
                r1149 = True
            elif (t=="1150"):
                tag1150 = v
                r1150 = True
            if (r48 and r55 and r779 and r1148 and r1149 and r1150):
                #if (flags[48] and flags[55] and flags[779] and flags[1148] and flags[1149] and flags[1150]):
                break
        
    dt = datetime.datetime.strptime(tag779, '%Y%m%d%H%M%S%f')
    epoch = datetime.datetime.utcfromtimestamp(0)
    us = tag779[14:]
    datestring = str(int((dt - epoch).total_seconds())) + us #.zfill(6)

    rangestring = ""
    if (tag1148 != "" and tag1149 != ""):
        if (tag1148[-8] != '.'):
            assert check(), "Bug in input, num decimal places != 7"
        if (tag1149[-8] != '.'):
            assert check(), "Bug in input, num decimal places != 7"
        # use python's floating pointer operations and format to 6 decimal places
        rangestring = "{:.6f}".format(float(tag1149) - float(tag1148))

    # format the output strings in the string buffer
    sb = "{}:{}\n\tLastUpdateTime={}\n\tLowLimitPrice={}\n\tHighLimitPrice={}\n\tLimitPriceRange={}\n\tTradingReferencePrice={}\n".format(tag48, tag55, datestring, tag1148, tag1149, rangestring, tag1150)
    
    # write the string buffer to the output
    return sb

#def Parse(i,o):
    # The file is a text file with some special characters
    # Each line of the file corresponds to a message
    # The for loop below is used to iterate through the messages in the file
    #for line in i:
#    pool = Pool(8)
#    with Pool(8) as p:
#        o.write(p.map(process, i, 8))

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:", ["help", "input=", "output="])
    except getopt.GetoptError as err:
        # print help information and exit:
        usage()
    inputpath = "secdef.dat"
    outputpath = "secdef_parsed.txt"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("-i", "--input"):
            inputpath = a
        elif o in ("-o", "--output"):
            outputpath = a
        else:
            assert False, "unhandled option"
    # ...

    pool = Pool(8)
    start=datetime.datetime.now()
    with open(inputpath) as i:
        # chunk the work into batches of 4 lines at a time
        results = pool.map(process, i, 8)
    o = open(outputpath, 'w')
    for r in results:
        o.write(r)
    i.close()
    o.close()
    stop = (datetime.datetime.now()-start).total_seconds()

    print "Code executed in {} ms".format(stop*1000)

if __name__ == "__main__":
    main()
