#--------------------------------------------------------------------------------------------------------------
# General utilities function to perform dataset extraction considering BATCH_SIZE records to retain in memory #
#--------------------------------------------------------------------------------------------------------------

import os
import time
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq


DATAPATH = "./records"
GEOMETRIES = "Africa_Boundaries-shp/Africa_Boundaries.dbf"
RECORD_FILE = "./data/records_2020_2023.parquet"
BATCH_SIZE = 4*10**5


def howmanybatches(record_file,btcsize):
	"""
	Return the amount of batches given the current record_file and BATCH_SIZE
	"""
	parquet_file = pq.ParquetFile(record_file)
	c = 0
	for batch_partition in parquet_file.iter_batches(batch_size = btcsize):
		c+=1
	return c


def load_store_records(datapath):
	"""
	Load and save into a single parque the complete list of daily records
	"""
	df = []
	t0 = time.time()
	for record in os.listdir(datapath):
		try:
			df.append(pd.read_parquet(f"{datapath}/{record}"))
		except:
			df.append(pd.read_csv(f"{datapath}/{record}"))
	df = pd.concat(df)
	t1 = time.time()
	df.sort_values(by="DATEADDED", inplace=True)
	df.Actor1Geo_ADM2Code = df.Actor1Geo_ADM2Code.astype(float)
	df.Actor2Geo_ADM2Code = df.Actor2Geo_ADM2Code.astype(float)
	print("Saving into file:")
	df.to_parquet("./data/records_2020_2023.parquet")
	print("Elapsed: ", round(t1-t0, 3), "sec")


def load_geometries(geom_filename):
	"""
	Return the geometries GeoDataFrame
	"""
	return gpd.read_file(geom_filename)


def preprocess_batch(batch):
	"""
	Helper function to adapt the current batch in order to be filtered.
	"""
	try:
		batch.drop(["Day","MonthYear","Year", "FractionDate", "Unnamed: 0"], axis=1, inplace=True)
		batch["Year"]  = batch.DATEADDED.apply(lambda x: str(x)[0:4])
		batch["Month"] = batch.DATEADDED.apply(lambda x: str(x)[4:6])
		batch["Day"]   = batch.DATEADDED.apply(lambda x: str(x)[6:8])
	except:
		pass


def preprocess_batch_geometry(batch):
	"""
	Description
	"""
	t0 = time.time()
	print("Batch geometry preprocessing...")
	geom_tosave = []

	batch.drop("geometry", axis=1, inplace=True) if "geometry" in batch.columns else None

	# Check if some character is hindring the conversion to float
	for i,line in batch.iterrows():
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

	tosave = pd.concat(geom_tosave)

	t1 = time.time()
	print("Elapsed: ", round(t1-t0, 3), "sec")
	return gpd.GeoDataFrame(batch,
			geometry=gpd.points_from_xy(
				tosave.lon,
				tosave.lat),
			crs="EPSG:4326")


def filter_by_year(record_file, year):
	"""
	Return the whole parque filtered by the given year
	"""
	t0 = time.time()
	print("Filtering by year:", year)
	tosave = []
	parquet_file = pq.ParquetFile(record_file)
	for batch_partition in parquet_file.iter_batches(batch_size = BATCH_SIZE):
		batch = batch_partition.to_pandas()
		preprocess_batch(batch)
		filtered = batch[batch.Year==year]
		if filtered.shape[0] > 0:
			tosave.append(filtered)

	t1 = time.time()
	print("Elapsed (sec):", round(t1-t0, 3))
	print("Now saving in './data'")
	tosave_df = pd.concat(tosave)
	tosave_df.to_parquet(f"./data/timeseries_year{year}.parquet")	


def filter_by_EOI(record_file, event):
	"""
	Extract from the whole time span record file all entries that match a given
	EventBaseCode.
	Return DataFrame with geometry.
	"""
	t0 = time.time()
	print("Filtering by cameo:", event)
	tosave = []
	parquet_file = pq.ParquetFile(record_file)
	for batch_partition in parquet_file.iter_batches(batch_size = BATCH_SIZE):
		batch = batch_partition.to_pandas()
		preprocess_batch(batch)
		filtered = batch[batch.EventBaseCode==event]
		if filtered.shape[0] > 0:
			tosave.append(filtered)
	t1 = time.time()
	print("Elapsed (sec):", round(t1-t0, 3))
	return pd.concat(tosave) if tosave != [] else None


def filter_by_country(record_file, country_name):
	"""
	Extract from the whole time span record file all entries belonging to a given
	country.
	Return DataFrame with geometry.
	"""
	t0 = time.time()
	print("Filtering by country:", country_name)
	tosave = []
	parquet_file = pq.ParquetFile(record_file)
	for batch_partition in parquet_file.iter_batches(batch_size = BATCH_SIZE):
		batch = batch_partition.to_pandas()
		preprocess_batch(batch)
		filtered = batch[batch.NAME_0==country_name]
		if filtered.shape[0] > 0:
			tosave.append(filtered)
	t1 = time.time()
	print("Elapsed (sec):", round(t1-t0, 3))
	return pd.concat(tosave) if tosave != [] else None


def extracting_timeseries(record_file, country_names, cameos):
	"""
	(Optimised version)
	Exploratory analysis: extract and process information related to the number
	of events recorded in the whole time span according to a set of cameo EventRootCode
	for all countries under examination. 
	Save and return the timeseries DataFrame
	"""
	assert len(cameos) == 1, "You can choose at most one CAMEO Root Code."

	t0 = time.time()
	print("Performing timeseries filtering:")

	# list all days in the intervall
	dayslist = [
		f"{item.year}\
		{str(item.month) if len(str(item.month))==2 else '0'+str(item.month)}\
		{str(item.day)   if len(str(item.day))==2 else '0'+str(item.day)}".replace("\t","")
		for item in pd.date_range(pd.Timestamp('2020-01-01'), pd.Timestamp('2023-12-31'))]

	tosave = []
	for country in country_names:
		batch = filter_by_country(record_file, country)
		batch = batch[batch.EventRootCode.isin(cameos)]

		tmp = pd.DataFrame({"Year":batch.Year,
			"Month":batch.Month, 
			"Day":batch.Day, 
			"Date":batch.Year+batch.Month+batch.Day,
			"EventRootCode":batch.EventRootCode})

		gb = tmp.groupby(["Year", "Month", "Day", "Date"]).\
			EventRootCode.count().reset_index()

		current_dates = gb.Date.unique()

		date_series = [gb[gb.Date==date].EventRootCode.values[0]\
			if date in current_dates else 0 for date in dayslist]

		tosave.append(pd.DataFrame({
			"NAME_0":country,
			"Date":dayslist,
			"count":date_series
			}))

	t1 = time.time()
	print("Elapsed (sec):", round(t1-t0, 3))
	print("Now saving in './data'")
	tosave_df = pd.concat(tosave)
	tosave_df.to_parquet(f"./data/timeseries_cameo{cameos[0]}.parquet")
	return tosave_df


def get_neighbours(geom_filename):
	"""
	Return a list of Neighbouring countries for each state in the shapefile
	"""
	geometries = load_geometries(geom_filename)
	#geometries["neigh"] = None  
	toreturn = dict()
	for i,country in geometries.iterrows():   
		neighbours = geometries[~geometries.geometry.disjoint(country.geometry)].ISO.tolist()
		neighbours = [name for name in neighbours if country.ISO != name]
		toreturn[country.ISO] = neighbours
	return toreturn



def extract_relationships(record_file, geom_filename):

	t0 = time.time()
	print("Performing Neighbouring information extraction:")
	tosave = dict()
	geometries = load_geometries(geom_filename)
	neighbours = get_neighbours(geom_filename)

	# initialize the structure
	for country in neighbours:
		tosave[country] = []

	for year in ["2023"]:

		if not os.path.exists(f"./data/timeseries_year{year}.parquet"):
			filter_by_year(record_file, year)
		parquet_file = pq.ParquetFile(f"./data/timeseries_year{year}.parquet")

		for batch_partition in parquet_file.iter_batches(batch_size = BATCH_SIZE):
			batch = batch_partition.to_pandas()
			preprocess_batch(batch)
			batch = preprocess_batch_geometry(batch)

			for country in neighbours:

				if neighbours[country]==[]:
					continue 
				filt_geom = geometries[geometries.ISO.isin(neighbours[country])]
				filtered = gpd.sjoin(batch, filt_geom, predicate='within')
				if filtered.shape[0] > 0:
					print(f"{country}: found {filtered.shape[0]} entries in {year}.")

					tmp = pd.DataFrame({
						"ISO":[country for i in range(filtered.AvgTone.shape[0])],
						"Actor1CountryCode":filtered.Actor1CountryCode,
						"Actor2CountryCode":filtered.Actor2CountryCode,
						"AvgTone":filtered.AvgTone,
						"GoldsteinScale":filtered.GoldsteinScale,
						"Year":filtered.Year,
						"Month":filtered.Month,
						"Day":filtered.Day})

					tosave[country].append(tmp)

	for country in tosave.keys():
		if tosave[country] != []:
			print("Now saving: ", country)

			# saving the filtered dataset
			tmp = pd.concat(tosave[country])

			# saving the filtered dataset by performing group by operators
			current = tmp.groupby(["Year","Month", "Day"])[["AvgTone","GoldsteinScale"]].mean().reset_index()
			current["ISO"] = country
			current.to_parquet(f"./data/neighbours_2023_{country}_avg.parquet")

	return tosave


def extract_relationships_foreach_neighbours(record_file, geom_filename, year):
	"""
	Desctiption
	"""
	t0 = time.time()
	print("Performing Neighbouring information extraction:")
	tosave,tostore = dict(),[]
	geometries = load_geometries(geom_filename)
	neighbours = get_neighbours(geom_filename)

	# initialize the structure
	for country in neighbours:
		tosave[country] = []

	if not os.path.exists(f"./data/timeseries_year{year}.parquet"):
		filter_by_year(record_file, year)
	parquet_file = pq.ParquetFile(f"./data/timeseries_year{year}.parquet")
	
	for batch_partition in parquet_file.iter_batches(batch_size = BATCH_SIZE):
		batch = batch_partition.to_pandas()
		preprocess_batch(batch)
		batch = preprocess_batch_geometry(batch)

		for country in neighbours:

			if neighbours[country]==[]:
				continue 

			filt_geom = geometries[geometries.ISO.isin(neighbours[country])]
			filtered = gpd.sjoin(batch, filt_geom, predicate='within')
			if filtered.shape[0] > 0:
				print(f"{country}: found {filtered.shape[0]} entries in {year}.")
				tmp = pd.DataFrame({
					"ISO":[country for i in range(filtered.AvgTone.shape[0])],
					"nISO": filtered.ISO_left,
					"Actor1CountryCode":filtered.Actor1CountryCode,
					"Actor2CountryCode":filtered.Actor2CountryCode,
					"AvgTone":filtered.AvgTone,
					"GoldsteinScale":filtered.GoldsteinScale,
					"Year":filtered.Year,
					"Month":filtered.Month,
					"Day":filtered.Day})

				tosave[country].append(tmp)

	for country in tosave.keys():
		if tosave[country] != []:
			print("Now saving: ", country)

			# saving the filtered dataset
			tmp = pd.concat(tosave[country])
			current = tmp.groupby(["Year","nISO"])[["AvgTone","GoldsteinScale"]].mean().reset_index()
			current["ISO"] = country
			tostore.append(current)
		
	tostore_df = pd.concat(tostore)
	tostore_df.to_parquet(f"./data/neighbours_laginfo_{year}.parquet")



if __name__ == "__main__":

	#df = load_store_records(DATAPATH)
	#df.to_parquet("./records_2020_2023.parquet")
	#geometries = load_geometries(GEOMETRIES)
	#batch = singlebatch(RECORD_FILE)
	#c1 = filter_by_EOI(RECORD_FILE, 203)
	#c2 = filter_by_EOI(RECORD_FILE, 204)

	import os
	import time
	import pandas as pd
	import geopandas as gpd
	import matplotlib.pyplot as plt

	from analysis import *

	DATAPATH    = "./records"
	GEOM_FILE   = "./Africa_Boundaries-shp/"
	GEOMETRIES  = "./Africa_Boundaries-shp/Africa_Boundaries.dbf"
	RECORD_FILE = "./data/records_2020_2023.parquet"
	BATCH_SIZE  = 4*10**5

	#extract_relationships_foreach_neighbours(RECORD_FILE,GEOM_FILE, "2020")
	#extract_relationships_foreach_neighbours(RECORD_FILE,GEOM_FILE, "2021")
	#extract_relationships_foreach_neighbours(RECORD_FILE,GEOM_FILE, "2022")
	#extract_relationships_foreach_neighbours(RECORD_FILE,GEOM_FILE, "2023")
	#extract_relationships(RECORD_FILE,GEOM_FILE)

