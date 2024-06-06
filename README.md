# Africa Stability Spatial Analysis

## Abstract
The present analysis tested a spatially oriented hypothesis over data collected in a period of 3 years within a subset of countries belonging to the African continent. Spatial autoregressive model assessment had been computed with the purpose of evaluating whether significant and persistent relations between event predictors and countries' stability score can be found throughout the period under examination.

**Keywords:** Africa, Stability Index, Spatial Durbin Model, GDELT project.

---

## Reports

**Report** available at: [report](https://github.com/gzemo/africa-stability-spatial-analysis/blob/main/report.pdf)

**Spatial analysis notebook** available at: [spatial_analysis](https://github.com/gzemo/africa-stability-spatial-analysis/blob/main/spatial_test.pdf)

---

## Data extraction and processing

1. Raw GDELT daily update extraction and processing in order to retain only relevant records by filtering according to each coordinate of interest and years (2020-2022)
2. CAMEO eventCode filtering by retaining only those events belonging to a subset of root eventCodes.
3. Final dataset preparation: merging Stability indexes and predictors (see `analysis.ipynb`).

Data available at:
* `./data/timeseries_cameoXX.parquet`: full set of filtered records according to a given CAMEO root eventCode.
* `./data/event_predictors.csv`: final predictor dataset merging counts of all events happened according to time and eventCode.
* `./data/stability_indexes.csv`: stability index of each country according to years.


# Credits and Licences:
* **ArcGIS**: unlicensed
* *Event Data* (modified) **GDELT**:  (https://www.gdeltproject.org/)
* *Countries stability index* **World Bank Open Data**: CC-BY (https://www.worldbank.org/en/home)
