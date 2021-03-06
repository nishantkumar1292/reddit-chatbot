import json
import sys
from datetime import datetime
import os
import urllib.request #for downloading file
import bz2 #for extracting zipfile

import mysql.connector
from mysql.connector import errorcode


#file imports
import app_constants as AppConstants

connection = mysql.connector.connect(**AppConstants.db_config)
connection.reset_session(user_variables = None, session_variables = None)

def get_timeframe():
	if len(sys.argv) != 2:
		print("Usage: python3 chatbot_database.py 2017-11")
		sys.exit(1)
	else:
		timeframe = sys.argv[1]
	return timeframe

timeframe = get_timeframe()
sql_transaction = 0

# connection = sqlite3.connect('{}.db'.format(AppConstants.database))
c = connection.cursor(buffered=True)

table_name = "parent_reply"

def create_table():
	try:
		c.execute("CREATE TABLE IF NOT EXISTS {} (parent_id VARCHAR(200) PRIMARY KEY, comment_id VARCHAR(200) UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT, timeframe TEXT)".format(table_name))
	except mysql.connector.Error as err:
		if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
			print("WARN: Table already exists")
		else:
			print(err.msg)

def get_file(path):
	if os.path.exists(path):
		return path
	else:
		url = "http://files.pushshift.io/reddit/comments/RC_{}.bz2".format(timeframe)
		download_file_path = download_file(url, path)
		extract_file(download_file_path)
		return path

def extract_file(path):
	with open(path.split(".")[0], 'wb') as new_file, open(path, 'rb') as file:
		decompressor = bz2.BZ2Decompressor()
		for data in iter(lambda : file.read(100 * 1024), b''):
			new_file.write(decompressor.decompress(data))

def download_file(url, path):
	data_path = create_path(path)
	file_name = url.split('/')[-1]
	response = urllib.request.urlopen(url)
	download_file_path = os.path.join(data_path, file_name)
	if os.path.exists(download_file_path):
		return download_file_path
	f = open(os.path.join(data_path, file_name), 'wb')
	file_size = int(response.getheader("Content-Length"))
	print("Downloading: {} Bytes: {}".format(file_name, file_size))

	file_size_dl = 0
	block_sz = 10000
	while True:
		file_buffer = response.read(block_sz)
		if not file_buffer:
			break
		file_size_dl += len(file_buffer)
		f.write(file_buffer)
		status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
		status = status + chr(8)*(len(status)+1)
		print(status),
	f.close()
	return download_file_path

def create_path(path):
	data_folder, year, file_name = path.split("/")
	new_path = os.path.join(data_folder)
	if not os.path.exists(new_path):
		os.makedirs(new_path)
	new_path = os.path.join(new_path, year)
	if not os.path.exists(new_path):
		os.makedirs(new_path)
	return new_path

def format_data(data):
	data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
	return data

def find_parent(pid):
	try:
		query = "SELECT comment FROM {} WHERE comment_id = '{}' LIMIT 1".format(table_name, pid)
		c.execute(query)
		result = c.fetchone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
		raise(e)
		return False

def find_existing_score(pid):
	try:
		query = "SELECT score FROM {} WHERE parent_id = '{}' LIMIT 1".format(table_name, pid)
		c.execute(query)
		result = c.fetchone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
		raise(e)
		return False

def acceptable(data):
	if len(data.split(' ')) > 50 or len(data) < 1:
		return False
	elif len(data) > 1000:
		return False
	elif data == '[deleted]' or data == '[removed]':
		return False
	else:
		return True

def transaction_bldr(sql):
	global sql_transaction
	sql_transaction += 1
	if sql_transaction > 10:
		connection.commit()
		sql_transaction = 0

def sql_insert_replace_comment(comment_id, parent_id, parent_data, comment, subreddit, time, score):
	try:
		query = ("""UPDATE parent_reply SET parent_id = %s, comment_id = %s, parent = %s, comment = %s, subreddit = %s, unix = %s, score = %s WHERE parent_id = %s""")
		params = (parent_id, comment_id, parent_data, comment, subreddit, int(time), score, parent_id)
		c.execute(query, params)	
	except mysql.connector.errors.DatabaseError as err:
		print('replace_comment', str(err)) #for invalid characters
	except Exception as e:
		print('replace_comment', str(e))
		raise(e)

def sql_insert_has_parent(comment_id, parent_id, parent_data, comment, subreddit, time, score, timeframe):
	try:
		query = ("""INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score, timeframe) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")
		params = (parent_id, comment_id, parent_data, comment, subreddit, int(time), score, timeframe)
		c.execute(query, params)
	except mysql.connector.errors.DatabaseError as err:
		print('insert_comment_has_parent', str(err)) #for invalid characters
	except Exception as e:
		print('insert_comment_has_parent', str(e))
		raise(e)

def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, time, score, timeframe):
	try:
		query = ("""INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score, timeframe) VALUES (%s, %s, %s, %s, %s, %s, %s)""")
		params = (parent_id, comment_id, comment, subreddit, int(time), score, timeframe)
		c.execute(query, params)
	except mysql.connector.errors.DatabaseError as err:
		print('insert_comment_no_parent', str(err)) #for invalid characters
	except Exception as e:
		print('insert_comment_no_parent', str(e))
		raise(e)

def format_score(score):
	if score:
		return score
	else:
		return 0

if __name__ == '__main__':
	create_table()
	row_counter = 0
	piared_rows = 0
	file = get_file('data/{}/RC_{}'.format(timeframe.split("-")[0], timeframe))

	with open(file, buffering=1000)  as f:
		for row in f:
			row_counter += 1
			row = json.loads(row)
			parent_id = row['parent_id'].split("_")[1]
			body = format_data(row['body'])
			created_utc = row['created_utc']
			score = format_score(row['score'])
			subreddit = row['subreddit']
			comment_id = row['id']
			timeframe = timeframe
			parent_data = find_parent(parent_id)

			if score >= 2:
				if acceptable(body):
					existing_comment_score = find_existing_score(parent_id)
					if existing_comment_score:
						if score > existing_comment_score:
							sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
					else:
						if parent_data:
							sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score, timeframe)
							piared_rows += 1
						else:
							sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score, timeframe)

			if row_counter % 1000 == 0:
				print("Total rows read: {}, Paired Rows: {}, Time: {}".format(row_counter, piared_rows, str(datetime.now())))
				connection.commit()
	connection.commit()
	c.close()
	connection.close()