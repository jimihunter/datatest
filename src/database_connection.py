from sqlalchemy import create_engine


class MyDB():
	'''
		database connection class
	'''
	
	def __init__(self, conn_string):
		self.conn_string = conn_string
		print("This is the connection string: {}".format(self.conn_string))
	
	
	def connect_db(self):
		try:
			print("here: {}".format(self.conn_string))
			engine = create_engine(self.conn_string, echo=True)
			return engine
		except exc.SQLAlchemyError:
			print("Database connection failure needs investigations")