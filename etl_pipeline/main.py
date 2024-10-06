from google,oauth2 import service_account #google cloud library
import requests #for consuming data from API endpoints using URLs
import pandas as pd #for data processing

#Function to read data from /posts API endpoint
def read_posts(url):
	"""Extract data from /posts endpoint."""
	print("Extracting posts data...")
	post_res = requests.get(url)
	return pd.DataFrame(post_res.json())

#Function to read data from /users API endpoint
def extract_endpoint_users(url):
	"""Extract data from /users endpoint."""
	print("Extracting users data...")
	user_res = requests.get(url)
	user_df = pd.DataFrame(user_res.json())
	final_user_df['userID']=users_df['id
	for i in range(len(users_df['company'])):
		for j in users_df['company'][i]:
			final_user_df[j]=users_df['company'][i][j]
	return final_user_df

#Function to integrate user data with their posts informations.
def process(post_df, users_df):
	"""Transform data: enrich posts with user information and add status field."""
	print("Transforming data...")
	post_df['status']= 'lengthy' if len(post['body']) > 150 else 'concise'
	final_df = pd.merge(post_df, users_df, on='userID', how='inner')
	return final_df

#Function to store processed data into big query table.
def load_data(processed_info,key_path):
	"""Load data into Google bigquery."""
	credentials = service_account.Credentials.from_service_account_file(key_path)
	#Writing pandas dataframe to bigquery table in google cloud.
	processed_info.to_gbq(destination_table='project_id.dataset.user_posts_info',credentials=credentials, if_exists='replace')

if __name__ == "__main__":
	print("ETL Job Started")	
	key_path = '/path-to-service_account-key-json-file/filename.json'
	# Extract phase
	posts = read_posts('https://jsonplaceholder.typicode.com/posts') #Reading post data from posts API endpoint
	users = extract_endpoint_users('https://jsonplaceholder.typicode.com/users') #Reading users data from user API endpoint
	# Transform phase
	posts_transformed = process(posts, users)
	# Load phase
	load_data(posts_transformed, key_path)
	print("ETL Job Finished")
