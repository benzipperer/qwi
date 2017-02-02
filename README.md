# Processing QWI data
Scripts to process QWI data from the [Census](https://lehd.ces.census.gov/data/#qwi).

## Contents
[qwi_convert.R](qwi_convert.R) - R script to download and convert raw .csv files into Stata .dta format. Requires [allstates.csv](allstates.csv).

[allstates.csv](allstates.csv) - list of all state abbreviations potentially available (not all are available with every release)