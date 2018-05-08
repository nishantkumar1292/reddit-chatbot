import sqlite3
import json
from datetime import datetime
import os
import urllib.request #for downloading file
import bz2 #for extracting zipfile

database = 'reddit_comments'
timeframe = '2005-12'
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
		query = "SELECT comment FROM {} WHERE parent_id = '{}'".format(pid)
		c.execute(query)
		result = c.fecthone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
		return False

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
			parent_data = find_parent(parent_id)

			if score >= 2:
				existing_comment_score = find_existing_score(parent_id)