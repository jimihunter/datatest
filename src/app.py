import pandas as pd
#from datatest 
from data_processing import DataProcessing

CHUNKSIZE = 1000000
FILE_NAME = "userid-timestamp-artid-artname-traid-traname.tsv"
CONN_STRING = 'sqlite:///testing.db'
TABLE_NAME = 'data'

def main():
	'''
	this is the client or driver application 
	'''

	data_obj = DataProcessing(CONN_STRING, CHUNKSIZE, FILE_NAME)
	
	disk_engine = data_obj.get_database_connection()
	
	data_obj.data_cleansing_and_persistent(disk_engine, TABLE_NAME)
	
	sql_query_a = "SELECT userID, COUNT(DISTINCT trackId) as distinct_song " +\
            "FROM data GROUP BY userID " +\
            "ORDER BY distinct_song DESC"

	sql_query_b = "SELECT artistName, trackName, COUNT(*) as PlayCount " +\
            "FROM data WHERE artistName IS NOT NULL " +\
            "AND trackName IS NOT NULL " +\
            "GROUP BY artistName, trackName " +\
            "ORDER BY PlayCount DESC " +\
            "LIMIT 100"

	#DOES NOT WORK AS SQLITE DOES NOT SUPPORT THE DATEDIFF
	'''
	sql_query_c = "SELECT t1.userID,t1.trackName, t1.timestamp, t2.timestamp, "+\
	    "TIMESTAMPDIFF(Minute, t1.timestamp, t2.timestamp) AS sessionDuration "+\
        "FROM data t1 INNER JOIN data t2 ON t1.userID = t2.userID "+\
        "WHERE sessionDuration < 20"
	'''

	df_a = data_obj.retrieve_query_result(sql_query_a, disk_engine)
	df_b = data_obj.retrieve_query_result(sql_query_b, disk_engine)
	
if __name__ == "__main__":
	main()
	print('Exit done properly')