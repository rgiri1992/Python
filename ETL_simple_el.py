#import needed libraries
import petl as etl, psycopg2 as pg, sys , pyodbc
from sqlalchemy import *
from importlib import reload


reload(sys)
# declare connection properties 
server = ''
Sdatabase = 'AdventureWorks'
Ddatabase = 'Process_Demo'
username = 'ragiri'
password = ''
driver = '{SQL Server}' # Driver you need to connect to the database
port = '1433'
SourceConn = pyodbc.connect(
	'DRIVER='+driver+';PORT=port;SERVER='+server+';PORT=1443;DATABASE='+Sdatabase+';UID='+username+
               ';PWD='+password)
TargetConn = pyodbc.connect(
	'DRIVER='+driver+';PORT=port;SERVER='+server+';PORT=1443;DATABASE='+Ddatabase+';UID='+username+
               ';PWD='+password)

TargetConn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
TargetConn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
TargetConn.setencoding(encoding='utf-8')
#set my cursor
SourceCursor = SourceConn.cursor()
TargetCursor = TargetConn.cursor()
SourceCursor.execute("select s.name +'.'+t.name [Tablewithschema], t.name [name] from sys.tables t inner join sys.schemas s  on t.schema_id =s.schema_id where t.name like 'person%'")


SourceTable = SourceCursor.fetchall()

for t in SourceTable:
	#print(t)
	#TargetCursor.execute("select * from %s" % (t[0]) )
	qStr = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_NAME = '%s' and TABLE_SCHEMA = '%s') DROP TABLE %s" % (t[1],t[1],t[0])
	#print(qStr)
	TargetCursor.execute(qStr)
	SourceDataset =etl.fromdb(SourceConn,'select * from %s' % t[0])
	etl.todb(SourceDataset, TargetCursor, t[1],  create=true, dialect='mssql', sample=10000, schema ='Person')

TargetCursor.close
SourceCursor.close
