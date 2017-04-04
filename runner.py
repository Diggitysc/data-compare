from os import getcwd
from glob import glob


def main():
    path = getcwd()
    oldexport = path + '/oldexports/*.txt'
    newexport = path + '/newexports/*.csv'
    oldfileslist = glob(oldexport)
    newfileslist = glob(newexport)

    for txt in oldfileslist:
        checkold = txt.replace('_V27.txt', '')
        checkold = checkold.split('/')[-1]
        for csv in newfileslist:
            checknew = csv.replace('.csv', '')
            checknew = checknew.split('/')[-1]
            if (checkold == checknew):
                print txt, csv
                break

if __name__ == '__main__':
    main()
