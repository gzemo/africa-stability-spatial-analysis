import os
import io
import csv
import string
import zipfile
import requests
import numpy as np
import pandas as pd
import geopandas as gpd


class DayEstimator():

	def __init__(self, date:str, cameos:list,
		filepath_final_df:str,
		filepath_geometries:str,
		filepath_colnames:str):

		assert isinstance(date, str) and len(date)==8, "Not a valid input date!"

		#country_codes = pd.read_csv(filepath_country_codes, sep="\t")
		#country_list = country_codes[country_codes.Alpha3_code.notnull()].Alpha3_code.tolist()
		#self.country_list = country_list

		self.date = date
		self.cameos = cameos
		self.record_list = []

		self.filename = filepath_final_df
		self.geometries =  gpd.read_file(filepath_geometries)

		self.header = []
		with open(filepath_colnames, "r") as f:
			self.header = [line.strip() for line in f.readlines()]
		#self.header.extend(["geometry", "CountryISO", "CountryName"])


	def _retrieve_daily_records(self):
		"""
		Perform the call and store all daily updates
		"""
		prefix = "http://data.gdeltproject.org/gdeltv2"
		self.record_list = [ f"{prefix}/{self.date}{'0'+str(hour) if len(str(hour))==1 else str(hour)}{timestamp}00.export.CSV.zip"\
							for hour in range(0,23+1)\
							for timestamp in ["00", "15", "30", "45"] ]


	def _download_process_single(self, single_record_url, clean_after_computation=True):
		"""
		Single timestamp (15min records) download, extraction, filtering and processing
		to have the current 15min graph 
		Args:
			single_record_url: (str) url for that given endpoint
			clean_after_computation: (bool) whether to delete the 15min timestamp csv after
				computations
		Return the downloaded dataframe
		"""
		single_record_url = single_record_url.strip()
		tmp = single_record_url.split("/")[-1].lower().split(".")
		date, spec = tmp[0], tmp[1]

		print(f"  Downloading: {date[0:4]}/{date[4:6]}/{date[6:8]} - {date[8:10]}:{date[10:12]} {spec} {single_record_url}", end="  ")
		
		# performing request and check integrity
		r = requests.get(single_record_url)
		if not r.ok:
			print(f"*** Warning: no valid response gathered! date: {date} ; spec: {spec}")
			return pd.DataFrame()

		# zip extraction 
		z = zipfile.ZipFile(io.BytesIO(r.content))
		z.extractall(f"./rawdata/")
		
		# address the empty file condition
		try:
			current_df = pd.read_csv(f"./rawdata/{date}.{spec}.CSV", sep="\t", header=None)
		except Exception:
			print(f"*** Warning: something wrong in the process of reading the current file! date: {date} ; spec: {spec}")
			return 

		# remove file
		if clean_after_computation:
			os.system(f"rm ./rawdata/{date}.{spec}.CSV")

		current_df.drop(current_df.columns[51:59], axis=1, inplace=True)
		current_df = current_df.set_axis(self.header, axis=1)
		return current_df


	def _filter_latlon(self, current_df):
		"""
		Filter the current 15 min updates by retaining only those events
		belonging to the geometry of interest

		Return the filtered GeoDataFrame
		"""
		if current_df.shape[0] == 0:
			return pd.DataFrame()


		geom_tosave = []
		# Check if some character is hindring the conversion to float
		for i,line in current_df.iterrows():
			#for cname in ["Actor1Geo_Lat", "Actor1Geo_Long",
			#				"Actor2Geo_Lat", "Actor2Geo_Long"]:

				# # check for punctuation in string format
				# if type(line) == str:
				# 	punct = set(line.cname).intersection(
				# 		set(string.ascii_letters)\
				# 		.union(set(string.punctuation)-set([".","-"])))

				# 	if len(punct) > 0:

				# 		print("\n*** ",punct)

				# 		tmp, toremove = list(line.cname), list(punct)
				# 		for item in (toremove):
				# 			tmp.remove(item) 
				# 		line.cname = float("".join(tmp))

				# 		print("after correction:", line.cname)
					
			# check for actor position to consider
			location = [line.Actor1Geo_Lat, line.Actor1Geo_Long, line.Actor2Geo_Lat, line.Actor2Geo_Long]

			if all(pd.isna(location)):
				geom_tosave.append(pd.DataFrame({"lat":np.nan, "lon":np.nan}, index = [i]))
			
			elif all(pd.isna(location) == [True, True, False, False]) and \
				(type(line.Actor2Geo_Lat)!=str or type(line.Actor2Geo_Long)!=str):
				geom_tosave.append(pd.DataFrame({"lat":line.Actor2Geo_Lat, "lon":line.Actor2Geo_Long}, index = [i]))

			elif all(pd.isna(location) == [False, False, True, True,]) and \
				(type(line.Actor1Geo_Lat)!=str or type(line.Actor1Geo_Long)!=str):
				geom_tosave.append(pd.DataFrame({"lat":line.Actor1Geo_Lat, "lon":line.Actor1Geo_Long}, index = [i]))
			
			else:
				geom_tosave.append(pd.DataFrame({"lat":np.nan, "lon":np.nan}, index = [i]))

			# if pd.isna(line.Actor1Geo_Lat) or pd.isna(line.Actor1Geo_Long) or \
			# 	type(line.Actor1Geo_Lat)==str or type(line.Actor1Geo_Long)==str or \
			# 	geom_tosave.append(pd.DataFrame({"lat":line.Actor2Geo_Lat, "lon":line.Actor2Geo_Long}, index = [i]))
			
			# elif pd.isna(line.Actor2Geo_Lat) or pd.isna(line.Actor2Geo_Long) or \
			# 	type(line.Actor2Geo_Lat)==str or type(line.Actor2Geo_Long)==str :
			# 	geom_tosave.append(pd.DataFrame({"lat":line.Actor1Geo_Lat, "lon":line.Actor1Geo_Long}, index = [i]))
			# else:
			# 	geom_tosave.append(pd.DataFrame({"lat":np.nan, "lon":np.nan}, index = [i]))

		tosave = pd.concat(geom_tosave)

		gdf = gpd.GeoDataFrame(current_df,
				geometry=gpd.points_from_xy(
					tosave.lon,
					tosave.lat),
					#current_df[current_df.columns[49]],
					#current_df[current_df.columns[48]]),
				crs="EPSG:4326")

		filtered = gpd.sjoin(gdf, self.geometries, predicate='within')
		filtered.drop(filtered.columns[[-6,-5,-2,-1]], axis=1, inplace=True)

		filtered.to_csv("./checkit.csv")

		return filtered

	
	def _filter_cameo(self, current_df):
		"""
		Filter according to some CAMEO 

		Return the filtered GeoDataFrame
		"""
		if current_df.shape[0] == 0:
			return pd.DataFrame()

		try:			
			#current_df = current_df.astype({"EventBaseCode":int})
			filtered = current_df[current_df.EventBaseCode>=140] # Hardcoded
			return filtered

		except Exception:
			print("No cameo event found!")
			return pd.DataFrame()


	def _update_file(self, row):
		"""
		Deprecated: if you need to save in a massive csv
		"""
		if not os.path.exists(self.filename):
			with open(self.filename, "w", newline="") as csvfile:
				writer = csv.writer(csvfile, delimiter=",")
				for line in [self.header, row]:
					writer.writerow(line)
		else:
			with open(self.filename, "a", newline="") as csvfile:
				writer = csv.writer(csvfile, delimiter=",")
				writer.writerow(row) 


	def process_day(self):
		"""
		Single day matrix estimation by additive process
		"""
		daily_files = []
		self._retrieve_daily_records()
		for _, record in enumerate(self.record_list):
			
			print()
			try:
				current_df = self._download_process_single(record)
				current_df = self._filter_latlon(current_df)
				#current_df = self._filter_cameo(current_df)

				if current_df.shape[0] == 0:
					continue

			except Exception:
				continue

			daily_files.append(current_df)

			#for _, line in current_df.iterrows():
			#	self._update_file(line.tolist())

		tosave = pd.concat(daily_files)

		try:
			tosave.to_parquet(f"./records/{self.date}_records.parquet")
		except Exception:
			tosave.to_csv(f"./records/{self.date}_records.csv")

		print(f"Completed!", end="\n")

"""
de = DayEstimator(
	date = "20200101",
	cameos = [],
	filepath_final_df = "./records.csv", 
	filepath_geometries = "./Africa_Boundaries-shp/Africa_Boundaries.dbf",
	filepath_colnames = "./colnames.txt"
	)

de.process_day()
"""