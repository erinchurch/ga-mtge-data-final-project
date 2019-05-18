GA 2019 Python Final Project
==============================================

FNMA Loan Performance Data Analysis, from consuming raw files to generating pre-specified reports.

The Workflow & Supporting Information
-----------

This processes include:

* README.md - this file
* consolidate_files.py - given directories of raw files, this will consolidate and normalize data format.
* merge_data.py - given the performance and acqusition data sets, join and write to a separate file for additional processing.
* report_tagging.py - given the merged data set, add pre-specified reporting tags, or ad hoc tags to facilitate downstream report creation.
* report_groupby.py - given the consolidated data set and report tags, generated grouped or summarized dataframes to build reports from, write them to individual files.
* report_creation.py - given the summarized data frames, generate charts and tables that can be written directly to pdf or other image format.

Note on Data Sourcing
------------------

The data used to develop this program is available from the Fannie Mae website.  
At the time of writing the data was available free of charge but required the creation of an account.

Data is not a part of the code respository. 
One must download the source data files and make them available to the code base you wish to run. 

