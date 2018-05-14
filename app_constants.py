def get_table_name(timeframe):
	year, month = timeframe.split("-")
	table_name = "parent_reply_{}_{}".format(year, month)
	return table_name

db_config = {
	'user': 'vivek',
	'password': 'vivek@#129',
	'host': '127.0.0.1',
	'database': 'chatbot',
	'raise_on_warnings': True,
}