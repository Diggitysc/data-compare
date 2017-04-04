from csv import reader
import sys


def main():

    oldexport = sys.argv[1]
    newexport = sys.argv[2]
    mapin = sys.argv[3]
    mapvals = mapin.split('=')
    map = {}
    map[mapvals[0]] = mapvals[1]
    make_map(oldexport, newexport, map)


def make_map(old, new, primekey):

    olddata = []
    newdata = []
    filename = old.split('_V27')[0]
    filename = filename.split('/')[-1]

    with open(old, 'r') as oinfi:
        olddatagrab = reader(oinfi, dialect='excel-tab')
        for line in olddatagrab:
            olddata.append(line)
    oinfi.close()

    with open(new, 'r') as ninfi:
        newdatagrab = reader(ninfi, dialect='excel')
        for line in newdatagrab:
            newdata.append(line)
    ninfi.close()

    fulldict = map_from_data(olddata, newdata, primekey, filename)
    dictname = filename + 'dictionary.txt'
    fulldict = str(fulldict)
    write_log(fulldict, dictname)


def make_report(old, new, align, fulldict, filename):
    for k, v in fulldict.iteritems():
        oldpos = find_pos(old[0], k)
        newpos = find_pos(new[0], v)
        for o, n in align.iteritems():
            consect = 0
            fail = 0
            success = 0
            if ((old[o][oldpos] != new[n][newpos]) and (consect < 5)):
                message = "Data misalignement position " + str(o) + '' + str(n)
                message = message + " on values " + k + " vs " + v
                consect = consect + 1
                fail = fail + 1
            else:
                consect = 0
                success = success + 1


def align_data(old, new, prime):
    alignment = {}
    headerold = old[0]
    headernew = new[0]
    for k, v in prime.iteritems():
        oldprime = k
        newprime = v

    primeposold = find_pos(headerold, oldprime)
    primeposnew = find_pos(headernew, newprime)

    for i in range(1, len(old)):
        for j in range(1, len(new)):
            if (old[i][primeposold] == new[j][primeposnew]):
                alignment[i] = j
                break

    return alignment


def map_from_data(old, new, primekey, filename):
    def map_left(oldleft, newleft, new, old, primeinfo):
        moredict = {}
        for newkey in newleft:
            if(has_num(newkey)):
                repblock = newkey.split('_')
                tearend = '_' + repblock[-1]
                check2 = repblock[-1]
                check1 = newkey.replace(tearend, '')
                for oldkey in oldleft:
                    checkend = verifyend(check2, oldkey)
                    if((check1 in oldkey) and (checkend)):
                        samekey = check_diff_key(
                            newkey, oldkey, new, old, primeinfo)
                        if (samekey is True):
                            checkfilled = moredict.get(oldkey)
                            if (checkfilled is None):
                                moredict[oldkey] = newkey
                            else:
                                fillval = resolveconflict(
                                    newkey, checkfilled, oldkey)
                                moredict[oldkey] = fillval
        return moredict

    primeinfo = {}
    oldkeylist = primekey.keys()
    primeold = oldkeylist[0]
    headersold = old[0]
    newexportkey = primekey[primeold]
    headersnew = new[0]
    primenew = newexportkey
    primeinfo['primeold'] = primeold
    primeinfo['primenew'] = primenew
    #find column position of prime key from header
    primecolold = find_pos(headersold, primeold)
    primecolnew = find_pos(headersnew, primenew)
    primeinfo['primecolold'] = primecolold
    primeinfo['primecolnew'] = primecolnew

    ignorelist = [0]

    maplineold, maplinenew = get_nextline(
        new, old, primeinfo, ignorelist)

    firstmap = findmap(maplineold, maplinenew, headersold, headersnew)
    addition = map_left(headersold, headersnew, new, old, primeinfo)

    fin = dict(firstmap.items() + addition.items())

    for k, v in fin.iteritems():
        if k in headersold:
            headersold.remove(k)
        if v in headersnew:
            headersnew.remove(v)

    for k, v in primekey.iteritems():
        headersold.remove(k)
        headersnew.remove(v)

    for item in headersold:
        notfound = True
        for keyval in headersnew:
            if keyval in item:
                message = item + " possibly " + keyval + \
                    " but data type misaligned"
                outfile = filename + 'missingkeyreport.txt'
                write_log(message, outfile)
                notfound = False
        if (notfound):
            message = "Missing or unable to map field" + item
            outfile = filename + 'missingkeyreport.txt'
            write_log(message, outfile)

    return fin


def get_nextline(new, old, primedict, ignorelist):
    '''
    returns the line with most filled in values that exists in both
    old and new export files
    '''
    def find_full_line(data, ignoreline):
        '''
            returns the line with most filled in values for data
        '''
        fullestline = 0
        volume = 0

        for line in range(1, len(data)):
            fill = 0
            if (volume < len(data[line])):
                for i in range(0, len(data[line])):
                    if (data[line][i] != ''):
                        fill = fill + 1
                if ((fill > volume)) and (line not in ignoreline):
                    volume = fill
                    fullestline = line
        return fullestline
    primecolold = primedict['primecolold']
    primecolnew = primedict['primecolnew']
    startline = find_full_line(old, ignorelist)
    findlinekey = old[startline][primecolold]
    lineinnew = False
    while lineinnew is False:
        for i in range(1, len(new)):
            if (new[i][primecolnew] == findlinekey):
                lineinnew = i
        if (lineinnew is False):
            oldline = startline
            ignorelist.append(oldline)
            startline = find_full_line(old, ignorelist)
            findlinekey = old[startline][primecolold]
    ignorelist.append(startline)
    maplineold = old[startline]
    maplinenew = new[lineinnew]
    return maplineold, maplinenew


def check_diff_key(newkey, oldkey, new, old, primeinfo):
    '''
        checks for keys with different names but same data over short iteration
    '''
    newkeypos = find_pos(new[0], newkey)
    oldkeypos = find_pos(old[0], oldkey)
    badcheck = 0
    match = 0

    for i in range(1, len(old)):
        lineupoldval = old[i][primeinfo['primecolold']]
        for j in range(1, len(new)):
            lineupnewval = new[j][primeinfo['primecolnew']]
            if (lineupoldval == lineupnewval):
                oldcheck = (old[i][oldkeypos])
                newcheck = (new[j][newkeypos])
                if (oldcheck == newcheck):
                    match = match + 1
                    if(match > 100):
                        return True
                else:
                    badcheck = badcheck + 1
                    if (badcheck > 5):
                        return False


def findmap(old1, new1, headersold, headersnew):
    '''
        for one-one correlation of keys make mapping
    '''
    map = {}
    for headnewkey in headersnew:
        for headoldkey in headersold:
            if headnewkey in headoldkey:
                newpos = find_pos(headersnew, headnewkey)
                oldpos = find_pos(headersold, headoldkey)
                if(new1[newpos] == old1[oldpos]):
                    map[headoldkey] = headnewkey
    return map


def find_pos(list, val):
    '''
        given a list and a val, return position of value in list
    '''
    for i in range(0, len(list)):
        if (val == list[i]):
            return i


def resolveconflict(newkey, checkfilled, oldkey):
    '''
        Determine correct key value when more than one key is associated
    '''
    newkeyend = newkey.split('_')[-1]
    checkfillend = checkfilled.split('_')[-1]
    if (int(newkeyend) > int(checkfillend)):
        if (verifyend(newkeyend, oldkey)):
            return newkey
    else:
        if (verifyend(checkfillend, oldkey)):
            return checkfilled


def verifyend(endcheck, oldkey):
    '''
        Verify end compares a trailing number to a dictionary key input for
        mapping (new export vs old)
        ex) hiv_nat_7 vs 34s4_hiv_nat_34b8_007 (passed in 7 compare to 007)
    '''
    valgrab = len(endcheck)
    valgrab = -valgrab
    endold = oldkey[valgrab:]
    if (endcheck == endold):
        return True
    else:
        return False


def has_num(check):
    '''
        Check if number is in string
    '''
    return any(char.isdigit() for char in check)


def write_log(msg, filename):
    with open(filename, 'ab') as fl:
        fl.write(msg)
        fl.write('\n')
    fl.close()

if __name__ == '__main__':
    main()
