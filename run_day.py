import os
import io
import sys
import zipfile
import requests
import numpy as np
import pandas as pd
from builder import DayEstimator

def run_single_day(date):
	de = DayEstimator(
		date = date,
		cameos = [],
		filepath_final_df = "./records.csv", 
		filepath_geometries = "./Africa_Boundaries-shp/Africa_Boundaries.dbf",
		filepath_colnames = "./colnames.txt"
		)	
	de.process_day()

if __name__ == "__main__":
	print(sys.argv)
	if not len(sys.argv)==2:
		print("Example usage:\npython process_month.py \"20201207\"")
		raise Exception
	date = str(sys.argv[1])
	run_single_day(date)