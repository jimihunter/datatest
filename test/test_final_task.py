from sqlalchemy import MetaData, Table, create_engine
from datatest.data_processing import DataProcessing
import pytest

class TestClasses():
	
	
	def testA_output(self):
		'''
			tests creation of user ID list and distinct song count
		'''
	
		CONN_STRING =  'sqlite:///tester_A.db'
		CHUNKSIZE = 1
		FILE_NAME = 'outputA.tsv'
		TABLE_NAME = 'table_a'
		dbA_obj = DataProcessing(CONN_STRING, CHUNKSIZE, FILE_NAME)
		disk_engine = dbA_obj.get_database_connection()
		
		sqlQuery = "SELECT userID, COUNT(DISTINCT trackId) as distinct_song " +\
            "FROM " + TABLE_NAME +" GROUP BY userID " +\
            "ORDER BY distinct_song DESC"
		
		engine = create_engine('sqlite:///tester_A.db')		
		metadata = MetaData()
		userId_distinct_song = Table('table_a', metadata, autoload=True, autoload_with=engine)
			
		df = dbA_obj.retrieve_query_result(sqlQuery, engine)
		
		assert df.userID[0] == 'user1' and df.distinct_song[0] == 2
		assert df.userID[1] == 'user2' and df.distinct_song[1] == 1
		
	
	def testB_output(self):
		'''
			tests creation of most popular songs by artistName and trackName
		'''
		
		CONN_STRING =  'sqlite:///tester_B.db'
		CHUNKSIZE = 1
		FILE_NAME = 'outputB.tsv'
		TABLE_NAME = 'table_b'
		dbA_obj = DataProcessing(CONN_STRING, CHUNKSIZE, FILE_NAME)
		disk_engine = dbA_obj.get_database_connection()
		
		sqlQuery = "SELECT artistName, trackName, COUNT(*) as PlayCount " +\
            "FROM " + TABLE_NAME +" WHERE artistName IS NOT NULL " +\
            "AND trackName IS NOT NULL " +\
            "GROUP BY artistName, trackName " +\
            "ORDER BY PlayCount DESC "
		
		engine = create_engine('sqlite:///tester_B.db')		
		metadata = MetaData()
		userId_distinct_song = Table('table_b', metadata, autoload=True, autoload_with=engine)
			
		df = dbA_obj.retrieve_query_result(sqlQuery, engine)
		assert df.artistName[0] == 'aName1' and df.trackName[0] == 'tName1'  and df.PlayCount[0] == 3
		assert df.artistName[1] == 'aName3' and df.trackName[1] == 'tName2'  and df.PlayCount[1] == 1
		
	
	
	

