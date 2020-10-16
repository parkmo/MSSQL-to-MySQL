#!/usr/bin/python
#import MySQLdb
import pyodbc
import sys
import configparser
from optparse import OptionParser

g_szVersion = "0.9"
g_OptParser = OptionParser(version="%%prog %s" % (g_szVersion))

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def sqlite_escape_string(sz_field):
    sz_field = sz_field.replace("'","''")
    return sz_field

typesFile = open('sqlserver_datatypes.txt', 'r').readlines()
dataTypes = dict((row.split(',')[0].strip(),row.split(',')[1].strip()) for row in typesFile)

def do_convert(mycfg):
  fetch_many_cnt = 1000
  insert_log_interval = 100
  # connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
  conn = pyodbc.connect('DRIVER={FreeTDS}; %s' % (mycfg.getMSSQLinfo()))
  msCursor = conn.cursor()
  msCursor.execute("SELECT * FROM sysobjects WHERE type='U'") #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
  dbTables = msCursor.fetchall()
  noLength = [56, 58, 61] #list of MSSQL data types that don't require a defined lenght ie. datetime
  for tbl in dbTables:
    doConvert = 1 # Default Convert
    doConvert = mycfg.isDoConvert(tbl[0], doConvert)
    if doConvert == 0:
        print_err("Skip: convert")
        continue
    print_err('>>migrating {0}'.format(tbl[0]))
    msCursor.execute("SELECT * FROM syscolumns WHERE id = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = msCursor.fetchall()
    attr = ""
    for col in columns:
        colType = dataTypes[ str(col.xtype)]
        #retrieve the column type based on the data type id
	#make adjustments to account for data types present in MSSQL but not supported in MySQL (NEEDS WORK!)
        if col.xtype == 60:
            colType = "float"
            attr += mycfg.getColMapValue(col.name) +" "+ colType + "(" + str(col.length) + "),"
        elif col.xtype in noLength:
            attr += mycfg.getColMapValue(col.name) +" "+ colType + ","
        else:
            attr += mycfg.getColMapValue(col.name) +" "+ colType + "(" + str(col.length) + "),"
    attr = attr[:-1]
    print_err('>>Fetch rows from table {0}'.format(tbl[0]))
    print("CREATE TABLE " + tbl[0] + " (" + attr + ");") #create the new table and all columns
    msCursor.execute("select * from %s" % tbl[0])
#    tblData = msCursor.fetchmany(10)
    tblData = msCursor.fetchmany(fetch_many_cnt)
    
    while len(tblData) > 0:
        cnt = 0
        #populate the new MySQL table with the data from MSSQL
        for row in tblData:
            fieldList = ""
            for field in row:
                if field == None: 
                    fieldList += "NULL,"
                else:
                    field = str(field)
#                    field = MySQLdb.escape_string(field).decode('utf-8')
                    field = sqlite_escape_string(field)
                    fieldList += "'"+ field + "',"
            fieldList = fieldList[:-1]
            print("INSERT INTO " + tbl[0] + " VALUES (" + fieldList + ");" )
            cnt += 1
            if cnt%insert_log_interval== 0:
                print_err('>>inserted {0} rows into table {1}'.format(insert_log_interval,tbl[0]))
        tblData = msCursor.fetchmany(fetch_many_cnt)

class CConfigConvert():
  def __init__(self, cfgfile):
     self.loadConfig(cfgfile)
     self.m_dic4migtable_in = dict()
     self.m_dic4migtable_ex = dict()
     self.m_mapcolumn = dict()
  def getMSSQLinfo(self):
     result = ""
     section_opts = self.m_ini_cfg.items("MSSQL")
     for cur_val in section_opts:
       result += "=".join(cur_val)+";"
     return result
  def loadConfig(self, cfgfile):
     self.m_ini_cfg = configparser.ConfigParser()
     self.m_ini_cfg.optionxform = str
     self.m_ini_cfg.read(cfgfile)
  def getColMapValue(self, colname):
     if ( colname in self.m_mapcolumn ):
       return self.m_mapcolumn[colname]
     return colname
  def addColMapTable(self, input_str):
     if ( input_str == None ):
       return
     for cur_item in input_str.split(","):
       splited = cur_item.split("=")
       self.m_mapcolumn[splited[0]] = splited[1]
  def addTable(self, table, mydict):
     if (table == None ):
        return
     for cur_table in table.split(","):
        mydict[cur_table] = 1
  def addInTable(self, intable):
     return self.addTable(intable, self.getDictInTable())
  def addExTable(self, extable):
     return self.addTable(extable, self.getDictExTable())
  def getDictInTable(self):
     return self.m_dic4migtable_in
  def getDictExTable(self):
     return self.m_dic4migtable_ex
  def isDoConvert(self, tablename, orgConvert):
     doConvert = orgConvert
     if len(self.getDictInTable()) > 0 :
        if tablename in self.getDictInTable():
            doConvert = 1
        else:
            doConvert = 0
     if len(self.getDictExTable()) > 0:
        if tablename in self.getDictExTable():
            doConvert = 0
        else:
            doConvert = 1
     return doConvert

def main():
  requiredOpts = "cfgfile".split()
  g_OptParser.add_option("-T", "--type", dest="type",
  help="target DB-type", default="sqlite3")
  g_OptParser.add_option("-i", "--intables", dest="intables",
  help="include table names. ex) aaa,bbb")
  g_OptParser.add_option("-e", "--extables", dest="extables",
  help="exinclude table names. ex) aaa,bbb")
  g_OptParser.add_option("-m", "--mapcolumn", dest="mapcolumn",
  help="map column names. ex) aaa=bbb,ddd=eee")
  g_OptParser.add_option("-c", "--cfgfile", dest="cfgfile",
    help="config filename")
  g_OptParser.add_option("-w", "--writefile", dest="cfgfile", metavar="FILE",
    help="config filename")
  (options, args) = g_OptParser.parse_args()
  mycfg = CConfigConvert(options.cfgfile)
  mycfg.addInTable(options.intables)
  mycfg.addExTable(options.extables)
  mycfg.addColMapTable(options.mapcolumn)
  do_convert(mycfg)

if __name__ == "__main__":
  main()
