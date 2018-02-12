import pandas as pd

def generate_test_data():

	print("YES I AM HERE")

	test_data_A = [["user1","1", "a1", "aName1", "t1", "tName1"],
					  ["user1","2", "a2", "aName2", "t2", "tName3"],
					  ["user1","3", "a2", "aName2", "t2", "tName3"],
					  ["user2","3", "a3", "aName3", "t3", "tName2"]]
					  
	test_data_B = [["user1","1", "a1", "aName1", "t1", "tName1"],
					  ["user4","1", "a1", "aName1", "t1", "tName1"],
					  ["user5","1", "a1", "aName1", "t1", "tName1"],
					  ["user2","3", "a3", "aName3", "t3", "tName2"],
					  ["user3","3", "a3", None, "t3", "tName2"],
					  ["user3","3", "a3", "aName3", "t3", None]
					  
					  ]
					  
	my_df_A = pd.DataFrame(test_data_A)
	my_df_B = pd.DataFrame(test_data_B)
	
	my_df_A.to_csv('outputA.tsv', index=False, header=False, sep='\t')
	my_df_B.to_csv('outputB.tsv', index=False, header=False, sep='\t')
	
	print(my_df_A)
	print('*' * 20)
	print(my_df_B)

generate_test_data()