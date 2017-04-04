from os import getcwd
from glob import glob
from csv import reader


def main():
    path = getcwd()
    oldexport = path + '/oldexports/*.txt'
    oldfileslist = glob(oldexport)

    for txt in oldfileslist:
        olddata = []
        with open(txt, 'r') as oinfi:
            olddatagrab = reader(oinfi, dialect='excel-tab')
            for line in olddatagrab:
                olddata.append(line)
        oinfi.close()
        if 'form_id' not in olddata[0]:
            print txt

if __name__ == '__main__':
    main()
