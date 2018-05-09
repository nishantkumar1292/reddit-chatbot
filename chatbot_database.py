import sqlite3
import json
from datetime import datetime
import os
import urllib.request #for downloading file
import bz2 #for extracting zipfile

database = 'reddit_comments'
timeframe = '2017-11'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(database))
c = connection.cursor()

year, month = timeframe.split("-")
table_name = "parent_reply_{}_{}".format(year, month)

def create_table():
	c.execute("CREATE TABLE IF NOT EXISTS {} (parent_id TEXT PRIMARY_KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)".format(table_name))

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
		result = c.fecthone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
		return False

def find_existing_score(pid):
	try:
		query = "SELECT score FROM {} WHERE parent_id = '{}' LIMIT 1".format(table_name, pid)
		c.execute(query)
		result = c.fecthone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
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
	sql_transaction.append(sql)
	if len(sql_transaction) > 1000:
		c.execute('BEGIN TRANSACTION')
		for s in sql_transaction:
			try:
				c.execute(s)
			except Exception as e:
				print("insert_in_db", str(e))
		connection.commit()
		sql_transaction = []

def sql_insert_replace_comment(comment_id, parent_id, parent_data, comment, subreddit, time, score):
	try:
		query = """UPDATE {} SET parent_id = '{}', comment_id = '{}', parent = '{}', comment = '{}', subreddit = '{}', unix = {}, score = {} WHERE parent_id = '{}';""".format(table_name, parent_id, comment_id, parent_data, comment, subreddit, int(time), score, parent_id)
		transaction_bldr(query)
	except Exception as e:
		print('replace_comment', str(e))

def sql_insert_has_parent(comment_id, parent_id, parent_data, comment, subreddit, time, score):
	try:
		query = """INSERT INTO {} (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ('{}', '{}', '{}', '{}', '{}', {}, {});""".format(table_name, parent_id, comment_id, parent_data, comment, subreddit, int(time), score, parent_id)
		transaction_bldr(query)
	except Exception as e:
		print('insert_comment_has_parent', str(e))

def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, time, score):
	try:
		query = """INSERT INTO {} (parent_id, comment_id, comment, subreddit, unix, score) VALUES ('{}', '{}', '{}', '{}', '{}', {}, {});""".format(table_name, parent_id, comment_id, comment, subreddit, int(time), score, parent_id)
		transaction_bldr(query)
	except Exception as e:
		print('insert_comment_no_parent', str(e))

if __name__ == '__main__':
	create_table()
	row_counter = 0
	piared_rows = 0
	file = get_file('data/{}/RC_{}'.format(timeframe.split("-")[0], timeframe))

	with open(file, buffering=1000)  as f:
		for row in f:
			row_counter += 1
			row = json.loads(row)
			parent_id = row['parent_id']
			body = format_data(row['body'])
			created_utc = row['created_utc']
			score = row['score']
			subreddit = row['subreddit']
			comment_id = row['name']
			parent_data = find_parent(parent_id)

			if score >= 2:
				if acceptable(body):
					existing_comment_score = find_existing_score(parent_id)
					if existing_comment_score:
						if score > existing_comment_score:
							sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
					else:
						if parent_data:
							sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
							piared_rows += 1
						else:
							sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

			if row_counter % 1000 == 0:
				print("Total rows read: {}, Paired Rows: {}, Time: {}".format(row_counter, piared_rows, str(datetime.now())))