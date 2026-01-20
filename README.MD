
Summary:
For this exercise I created an ETL (etl.py) that creates an output file (output.parquet) that can be queried by a python script (query.py) in order to answer some of the questions posed in the prompt (criteria #7). Below I explain my script, the acceptance criteria, and what could be improved upon.

The Script:
My python script (etl.py) loads each year’s wind, solar, and generator excel files into dataframes using pandas load_excel. Each tab of each file is loaded into its own dataframe, then ‘operable_status’, ‘type’ (solar, wind, etc) and ‘year’ columns are created from the tab and file names. Lastly I combine all into a single data frame (no need for partitioning), compress it, and output it.

The Query:
I created a second python script containing some SQL scripts that attempt to answer the questions posed in the seventh acceptance criteria. Running it in terminal should output some relevant stats.

The Acceptance Criteria:
1. I use each year’s solar, wind, and generator files, loading each tab separately and then combining into one dataframe
2. I addressed all formatting errors when debugging, including filling nulls and white spaces
3. My model is unified in that the data fits in one table, and it allows for time series since I included a year column
4. I did not institute any strict quality checks, however I did run into quality issues when compressing to parquet. This likely caught and helped me fix some data type errors
5. It is SQL-queryable if one wants summary stats
6. Running this script multiple times will always replace the old output file with a new one since the output file name is always the same. There will be no duplicate files, and the original files are untouched, so subsequent loads and runs should be identical to the first
7. SQL queries answering most of the prompts are inside the query.py script

Final Thoughts
If given more time I would work on the acceptance criteria I could not solve and I would also get clarification on what 'region' and 'battery' capacity are
