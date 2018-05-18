# def get_table_name(timeframe):
# 	year, month = timeframe.split("-")
# 	table_name = "parent_reply_{}_{}".format(year, month)
# 	return table_name

import configparser

config = configparser.ConfigParser()
config.read('/etc/reddit-chatbot/settings.ini')

DATABASE_USER = config.get('database', 'DATABASE_USER')
DATABASE_PASSWORD = config.get('database', 'DATABASE_PASSWORD')
DATABASE_HOST = config.get('database', 'DATABASE_HOST')
DATABASE_NAME = config.get('database', 'DATABASE_NAME')

# db_config = {
# 	'user': 'vivek',
# 	'password': 'vivek@#129',
# 	'host': '127.0.0.1',
# 	'database': 'chatbot',
# 	'raise_on_warnings': True,
# }

db_config = {
	'user': DATABASE_USER,
	'password': DATABASE_PASSWORD,
	'host': DATABASE_HOST,
	'database': DATABASE_NAME,
	'raise_on_warnings': True,
}