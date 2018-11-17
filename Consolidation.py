import MySQLdb, telegram

conn=MySQLdb.connect(host='localhost', user='root',passwd='###', db='datadb')
cursor = conn.cursor()

class Consolidation:
