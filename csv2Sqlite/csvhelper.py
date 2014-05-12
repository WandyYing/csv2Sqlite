import sys
import csv
import operator

class csvhelper(object):
    """docstring for csvhelper"""
    def __init__(self):
        super(csvhelper, self).__init__()
        #self.arg = arg

    def open_csv(self, csvPath):
        """example csv file:
            ID,Name,Age
            1,Wandy,12
            2,Ying,11"""
        csvfile = open(csvPath, 'rb')
        self.DictReader =csv.DictReader(csvfile) #, delimiter=',', quotechar=','

    def _fields(self):
        """['ID', 'Name', 'Age']"""
        if self.DictReader.fieldnames[-1] =='':
            return self.DictReader.fieldnames[:-1]
        return self.DictReader.fieldnames

    def _str_fields(self, delimiter=','):
        #return delimiter.join(self._fields())
        return delimiter.join(str('['+i+']') for i in self._fields())

    def _csv_content(self):
        """[('1', 'Wandy', '12'), ('2', 'Ying', '11')]"""
        getfields = operator.itemgetter(*self._fields())
        li=[]
        for row in self.DictReader:
            li.append(getfields(row))
        return li

if __name__ == '__main__':
    csvhelper = csvhelper()
    dirPath = "C:\\work\\SVN\\Robort for BW\\ExcelDataSource\\Conver\\D_Project Mgmt Summary Reports.csv"#.replace("\\","\\\\")
    # tablename = _file[:-4]
    csvhelper.open_csv(dirPath)
    #print csvhelper._fields()
    fields = csvhelper._str_fields()
    #print fields
    csvhelper._csv_content()