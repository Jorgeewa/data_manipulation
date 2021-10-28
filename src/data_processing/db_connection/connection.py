import mysql.connector
import os
from mysql.connector import errorcode
from dotenv import load_dotenv
load_dotenv()


class DbConnection():
	
	def __init__(self):
		self.user = os.getenv("dbuser")
		self.password = os.getenv("password")
		self.host = os.getenv("host")
		self.database = os.getenv("database")

	def connect_to_db(self):
		try:
			cnx = mysql.connector.connect(
				user=self.user, 
				password=self.password, 
				host=self.host, 
				database=self.database, 
				buffered=True, 
				connection_timeout=1000)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("Something is wrong with your user name or password")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exist")
			else:
				print(err)
		else:
			return cnx

	def close_cnx(self, cnx) -> None:
		cnx.close()
		
		
	def create_table(self, cursor, TABLES):
		for table_name in TABLES:
			table_description = TABLES[table_name]
			try:
				print("Creating table {}: ".format(table_name))
				cursor.execute(table_description)
			except mysql.connector.Error as err:
				if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
					print("already exists.")
				else:
					print(err.msg)
			else:
				print("OK")