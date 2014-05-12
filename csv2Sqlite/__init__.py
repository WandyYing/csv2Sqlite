#  Copyright (c) 2010 Wandy Ying
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from csvhelper import csvhelper
from sqlitehelper import sqlitehelper

__version__ = '0.1'

class csv2Sqlite(csvhelper, sqlitehelper):
    pass

if __name__ == '__main__':
    import os

    dirPath = "C:\work\SVN\ExcelDataSource\Conver".replace("\\","/")

    csv2db = csv2Sqlite()
    csv2db._connect("../BWDB.db")

    for root, dirs, files in os.walk(dirPath): 
        _root = root
        _files = files
    
    for _file in files:
        tablename = _file[:-4]
        csv2db.open_csv(_root+'/'+_file)
        fields = csv2db._str_fields()
        csv2db.create_table(tablename,fields)
        content = csv2db._csv_content()
        #sql = csv2db.__fields_String()
        #print content
        csv2db.import_Data_From_CSV(tablename,content)
        
    csv2db._disconnect()
