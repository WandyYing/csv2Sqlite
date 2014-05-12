import sqlite3
import string

class sqlitehelper(object):
    """docstring for sqlitehelper"""
    def __init__(self):
        super(sqlitehelper, self).__init__()
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
        sqlString = """DROP TABLE IF EXISTS [{0}];
                CREATE TABLE [{0}] ({1});""".format(tableName, fields)
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
        return "INSERT INTO [{0}] ({1}) VALUES ({2});".format(tableName, fields, ','.join('?'*len(self._fields())))