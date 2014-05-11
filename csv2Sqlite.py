# -*- encoding=utf8 -*-
# author Wandy Ying
import sys, csv, sqlite3, operator, string

class CSV2Sqlite(object):
    """docstring for CSV2Sqlite"""
    def __init__(self):
        super(CSV2Sqlite, self).__init__()
        #self.arg = arg

    def execute_sql_string(self, sqlString, parameters=None):
        cur = None
        try:
            cur = self._dbConn.cursor()
            self.__executemany_sql(cur, sqlString, parameters)
            self._dbConn.commit()
        finally :
            if cur :
                self._dbConn.rollback() 

    def execute_sql_script(self, sqlScript):
        cur = None
        try:
            cur = self._dbConn.cursor()
            self.__execute_script(cur, sqlScript)
            self._dbConn.commit()
        finally :
            if cur :
                self._dbConn.rollback() 

    def create_table(self, tableName, fields):
        #fields = ",".join(self._fields())
        sqlString = """DROP TABLE IF EXISTS {0};
                CREATE TABLE {0} ({1});""".format(tableName, fields)
        self.execute_sql_script(sqlString)     

    def import_Data_From_CSV(self, tablename, csv_data):
        sqlStringFormat = self.__fields_String(tablename)
        print sqlStringFormat
        self.execute_sql_string(sqlStringFormat, csv_data)
        

    def _connect(self, dbPath):
        self._dbConn = sqlite3.connect(dbPath)

    def _disconnect(self):
        self._dbConn.close()


    def __execute_sql(self, cur, sqlStatement):
        return cur.execute(sqlStatement)

    def __executemany_sql(self, cur, sqlStatement, parameters=None):
        return cur.executemany(sqlStatement, parameters)

    def __execute_script(self, cur, sqlscript):
        return cur.executescript(sqlscript)

    def __fields_String(self, tableName):
        #fields = ",".join(a._fields())
        fields = self._str_fields()
        return "INSERT INTO {0} ({1}) VALUES ({2});".format(tableName, fields, ','.join('?'*len(self._fields())))

    #csv function.
    def open_csv(self, csvPath):
        """example csv file:
            ID,Name,Age
            1,Wandy,12
            2,Ying,11"""
        csvfile = open(csvPath, 'rb')
        self.DictReader =csv.DictReader(csvfile, delimiter=',', quotechar=',')

    def _fields(self):
        """['ID', 'Name', 'Age']"""
        return self.DictReader.fieldnames

    def _str_fields(self, delimiter=','):
        return delimiter.join(self._fields())

    def _csv_content(self):
        """[('1', 'Wandy', '12'), ('2', 'Ying', '11')]"""
        getfields = operator.itemgetter(*self._fields())
        li=[]
        for row in self.DictReader:
            li.append(getfields(row))
        return li

if __name__ == '__main__':
    import os
    dirPath = "D:/Dev/Code/RFS/lib/csv"

    csv2db = CSV2Sqlite()
    csv2db._connect("../db/BWDB.db")

    for root, dirs, files in os.walk(dirPath): 
        _root = root
        _files = files
    
    for _file in files:
        tablename = _file[:-4]
        csv2db.open_csv(_root+'/'+_file)
        fields = csv2db._str_fields()
        csv2db.create_table(tablename,fields)
        content = csv2db._csv_content()
        print content
        #sql = csv2db.__fields_String()
        csv2db.import_Data_From_CSV(tablename,content)
        
    csv2db._disconnect()



    # csv2db = CSV2Sqlite()

    # csv2db.open_csv('wkDB.csv')
    # csv2db._connect("../db/BWDB.db")

    # fields = csv2db._str_fields()
    # csv2db.create_table("tableName",fields)

    # content = csv2db._csv_content()
    # print content
    
    # #sql = csv2db.__fields_String()
    # csv2db.import_Data_From_CSV("tableName",content)
    # csv2db._disconnect()


