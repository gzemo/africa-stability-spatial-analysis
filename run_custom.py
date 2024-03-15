import os
import io
import sys
import zipfile
import requests
import numpy as np
import pandas as pd
from builder import DayEstimator

def run_single_month(date):

	for day in range(1, 32):
		tmp = f"{date}{'0'+str(day) if len(str(day))==1 else str(day)}"
		print("***", tmp)
		de = DayEstimator(
			date = tmp,
			cameos = [],
			filepath_final_df = "./records.csv", 
			filepath_geometries = "./Africa_Boundaries-shp/Africa_Boundaries.dbf",
			filepath_colnames = "./colnames.txt"
			)	
		de.process_day()

if __name__ == "__main__":
	print(sys.argv)
	if not len(sys.argv)==2:
		print("Example usage:\npython run_custom.py \"202012\"")
		raise Exception
	date = str(sys.argv[1])
	run_single_month(date)