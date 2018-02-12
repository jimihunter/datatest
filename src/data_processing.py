import pandas as pd
from sqlalchemy import create_engine
import datetime as dt 
from . database_connection import MyDB


class DataProcessing(MyDB):
	'''
		This class handles the data ETL in this project
	'''

	def __init__(self,conn_string, chunksize, file_name):
		MyDB.__init__(self, conn_string)
		self.chunksize = chunksize
		self.file_name = file_name
		
		
	def get_database_connection(self):
		'''
		connects and returns database connection engine
		'''
		self.engine = self.connect_db()
		return self.engine
		
	def data_cleansing_and_persistent(self, disk_engine, table_name):
		'''
		cleans and persist data into sqlite file database	
		'''
		start = dt.datetime.now()
		chunksize_indicator = 0
		index_start = 1
		if self.chunksize > 1:
		
			for chunk in pd.read_table(self.file_name, header=None, chunksize=self.chunksize,\
                           iterator=True, sep="\t", error_bad_lines=False):
				chunk.columns = ['userID', 'timestamp', 'artistId', 'artistName',
                     'trackId', 'trackName']
				chunk = chunk.dropna(subset=['userID', 'trackId', 'artistName'], how='any')
				chunk.index += index_start

				chunksize_indicator += 1
				print('{} seconds: completed {} rows'.format((dt.datetime.now() \
                                                  - start).seconds, chunksize_indicator * self.chunksize))
				chunk.to_sql(table_name, disk_engine, if_exists='append')
				index_start = chunk.index[-1] + 1
			print('Done Processing Data ...')
		else:
			chunk = pd.read_table(self.file_name, header=None, sep="\t", error_bad_lines=False)
			chunk.columns = ['userID', 'timestamp', 'artistId', 'artistName',
                     'trackId', 'trackName']
			chunk = chunk.dropna(subset=['userID', 'trackId', 'artistName'], how='any')
			chunk.to_sql(table_name, disk_engine, if_exists='append')
			
		
	def retrieve_query_result(self, sqlQuery, disk_engine):
		'''
		retrieves data from the sqlite based on query
		'''
		data_frame = pd.read_sql_query(sqlQuery, disk_engine)
		return data_frame
		
	
	



