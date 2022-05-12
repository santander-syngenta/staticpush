import threading
import DATABASES as D
from sqlalchemy import create_engine, text
import pandas as pd
import requests
import json


def __main__():
	execute('USNB0H1232021')

def execute(trial_id):
	###Queries the datalake for BITS & UAV data
	obj = Stone(trial_id)
	obj.merge_data()
	
	##Pulls out the longitude and latitude of the trial from the BITS data
	df = obj.dataframes['final']
	longitude = df['longitude'][df['longitude'].notnull()].mode()[0]
	latitude = df['latitude'][df['latitude'].notnull()].mode()[0]

	###Queries the CEHub API for environmental data
	loc = Location(longitude, latitude)
	environmental_df = loc.cehub_query()

	###Prints the data to a file in the Output folder
	path = 'C:\\Users\\s1030345\\CODING\\OUTPUTS\\' + df['trial_id'].mode()[0] + '.xlsx'
	writer = pd.ExcelWriter(path, engine = 'xlsxwriter')
	df.to_excel(writer, sheet_name = 'BITS & UAV Data')
	environmental_df.to_excel(writer, sheet_name = 'Environmental Data')
	writer.save()
	writer.close()


class Stone(object):
	"""
	A classed used for querying the DataLake to retrieve UAV & trial data. 

	Attributes
	----------
	trial_id : str
		Trial-ID reported in BITS and must be present in BioAnalytics.
	engine : object
		Engine used for connecting to the Data Lake with SQLAlchemy
	dataframes : dict
		Dictionary of dataframes for accessing during merging
    """
	def __init__(self, trial_id):
		"""
		Parameters
		----------
		trial_id : str
			Trial-ID reported in BITS and must be present in BioAnalytics. 
		"""
		self._trial_id = trial_id
		self._engine = self.data_lake_connect()
		self._dataframes = {}
	
	@property
	def trial_id(self):
		return self._trial_id

	@property
	def engine(self):
		return self._engine

	@property
	def dataframes(self):
		return self._dataframes

	def merge_data(self):
		df0 = self.dataframes[0]
		df1 = self.dataframes[1]
		df2 = self.dataframes[2]
		df3 = self.dataframes[3]

		#First level merging trial level data
		first =  pd.merge(df0, df1, on=['trial'])

		#Second level merging treatment level data
		df2.columns = df2.columns.str.replace('treatment_no', 'trt_num')
		df2.columns = df2.columns.str.replace('trial_id', 'trial')
		second = pd.merge(first, df2, on=['trial', 'trt_num'])
		second.columns = second.columns.str.replace('plot_number', 'plot_id')


		#Third level merging assessment level data
		second['plot_id'] = second['plot_id'].map(int)
		df3['plot_id'] = df3['plot_id'].map(int)
		third = pd.merge(second, df3, on=['trial','plot_id'])

		output = third 
		#Fills NA values
		for column in df0.columns:
			if not output[column].isnull().all():
				output[column].fillna(output[column][output[column].notnull()].mode()[0], inplace=True)

		##Cleans output data
		output['trial_year'] = output['trial_year'].map(int)
		output.drop_duplicates(inplace=True)
		output = output.sort_values(by=['assmt_date', 'assmt_type', 'plot_id'])
		output.replace(to_replace = -99998, value = '.', inplace = True)
		self.dataframes['final'] = output
		return output
	
	def execute_queries(self):
		queries = ['./FILES/zero.sql','./FILES/one.sql', './FILES/two.sql',  './FILES/union.sql']
		threads = []; x = 0
		for query in queries:
			sql = open(query, 'r').read().replace('USNB0H1232021', self.trial_id)
			threads.append(threading.Thread(target = self.data_lake_execute, kwargs = {'SQL':sql,'query_name':x}))
			x += 1
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()

	def data_lake_execute(self, SQL, query_name):
		if type(SQL) == str:
			SQL = text(SQL)
		df = pd.read_sql(SQL, self.engine)
		self.dataframes[query_name] = df

	def data_lake_connect(self):
		engine = create_engine('postgresql://s1030345:s1030345PASS81997!@deawirbitt001.clwtglrkcnfi.eu-central-1.redshift.amazonaws.com:5439/mio')
		return engine


class Location(object):
	"""Location object for retrieving environmental data based on decimal degree coordinates."""
	def __init__(self, lower_left_longitude, lower_left_latitude):
		self._ll_longitude = lower_left_longitude
		self._ll_latitude = lower_left_latitude


	@property
	def ll_latitude(self):
		return self._ll_latitude
		
	@property
	def ll_longitude(self):
		return self._ll_longitude
	

	def cehub_query(self):
		lat = self.ll_latitude
		lon = self.ll_longitude
		post_data = json.loads(open('./FILES/post.json', 'r').read())
		url = 'http://my.meteoblue.com/dataset/query?apikey=syn23weriori0wh'
		response = requests.post(url, json=post_data)
		data = response.json()
		output = []
		for x in data:
			aggregation = x['codes'][0]['aggregation']
			unit = x['codes'][0]['unit']
			variable = x['codes'][0]['variable'] + '(' + x['domain'] + ')'
			level = x['codes'][0]['level']
			row = [variable, level, unit, aggregation] + x['codes'][0]['dataPerTimeInterval'][0]['data'][0]
			output.append(row)
		cols = ['Variable (dataset)','Level','Units','Aggregation'] + data[0]['timeIntervals'][0]
		df = pd.DataFrame(output, columns = cols)
		return df


	def cehub_data_recommendation(self):
		access_token = 'apiKey=syn23weriori0wh'
		API_url = 'https://cehubservices.syngenta-ais.com/api/Recommendation/GetRecommendations'
		parameters = {
			'longitude': self.ll_longitude,
			'latitude': self.ll_latitude
		}
		response = requests.get(API_url + '?' + access_token, params = parameters)
		return response