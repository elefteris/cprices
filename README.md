# The core pipeline

* receives the staged data prepared by the validation_staged pipeline
* can handle multiple:
    - items
    - retailers
    - months
* takes the data through the following stages:
    - preprocessing
    - classification
    - averaging
    - outlier detection
    - imputation
    - index calculations (on retailer level)
    - aggregation (indices on item level)
* can run multiple configuration scenarios. This is possible with the use of a number of different config files
* stores data intermediate and final outputs in hdfs to feed into the analysis pipeline

# Use of config files

The config folder has:
* a generic config file with:
    - the start date of the time series
    - the columns needed for the index
    - the items chosen to run
    - the scenarios chosen to run
* one or several scenario config files. Scenario 1 should be the baseline scenario.
The user can copy and paste the baseline scenario and make the necessary changes
to provide a different combination of config parameters regarding the stages of the core pipeline
