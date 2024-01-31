#create_dataframe
#read_file
#hdfs_read_csv
#move_log_txt_cdsw_to_csv_hdfs
#find
#melt_df
#get_hdfs_files

# python
import logging
import os
import pandas as pd
import subprocess

# pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def create_dataframe(
    segmented_lines,
    names,
    converters=None,
    debug=False
):
    """
    Creates a pandas dataframe from the data in the segmented_lines parameter.

    If the converter parameter isn’t set to default (i.e. type none), then
    either a cast to a type is applied or a function is applied to the
    relevant column instead.

    Used by the hdfs_read_csv module.

    Parameters
    ----------
    segmented_lines : any Python iterable data structure of iterable data structures (dict, list, array)
    names : a list of col names
    converters : a dataframe (or array) of Types (or functions)
    debug : a Boolean flag to set verbose output describing the frame being created

    Returns
    -------
    df : Pandas dataframe
    """
    if debug:
        print("lines")
        print(segmented_lines)
        print("names")
        print(names)

    if len(segmented_lines) > 0:
        if len(segmented_lines[0]) != len(names):
            segmented_lines = []

    segmented_lines = [s for s in segmented_lines if s != ['']]

    df = pd.DataFrame(segmented_lines, columns=names)

    if not converters is None:
        for colname in converters:
            if colname in df.columns:
                converter = converters[colname]

                if type(converter) == type:
                    df[colname] = df[colname].astype(converter)
                else:
                    df[colname] = df[colname].apply(converter)
    return df


def read_file(path):
    """
    Creates a Pandas dataframe.
    Reads in the list of files pointed to by the ‘path’ parameter and
    calls create_dataframe(), splitting up each line of the read file on
    comma separators.

    Parameters
    ----------
    path : a HDFS path
    converters : a dataframe (or array) of Types (or functions)
    debug : a Boolean flag to set verbose output describing the frame being created

    Returns
    -------
    create_dataframe : a Pandas dataframe

    Author
    ------
    ?

    Notes
    -----
    Pipelines used in:

    * core_dev
    * dashboard
    """
    cat = subprocess.Popen(["hadoop","fs","-cat", path], stdout=subprocess.PIPE)

    lines = []

    for line in cat.stdout:
        if len(line) > 0:
            lines.append(line.decode("utf-8").replace('\n','').replace('\r',''))

    return lines


def hdfs_read_csv(path, converters=None, debug=False):
    """
    Creates a Pandas dataframe.  Reads in the list of files pointed to by
    the ‘path’ parameter and calls create_dataframe(), splitting up each
    line of the read file on comma separators.

    Parameters
    ----------
    path : a HDFS path
    converters : a dataframe (or array) of Types (or functions)
    debug : a Boolean flag to set verbose output describing the frame being created

    Returns
    -------
    pandas dataframe
    """
    lines = read_file(path)
    if len(lines) == 0:
        print('No data')
        return None

    segmented_lines = []
    header = lines[0].split(',')
    for line in lines[1:]:
        segmented_line = line.split(',')

        segmented_lines.append(segmented_line)
    # print('loaded {0} segmented lines'.format(len(segmented_lines)))
    return create_dataframe(segmented_lines, header, converters, debug)


def move_log_txt_cdsw_to_csv_hdfs(infile, outfile):
    """
    Reads and outputs the files present in the specified HDFS file path
    and cleans them up by decoding the strings and removing new lines (\n)
    and carriage return (\r) characters.

    Used by the hdfs_read_csv module.


    Parameters
    ----------
    infile : string
        path to file in CDSW to be moved to HDFS

    outfile : string
        filepath to write in HDFS

    Notes
    -----
    Should be replaced with hadoop command line function
    hadoop fs -copyFromLocal <from_path> <to_path>

    Pipelines used in:
    All pipelines that export txt log files to CDSW plus the diagnostics
    reports from validation.
    """
    if os.path.exists(infile):
        # read txt from CDSW into pandas dataframe
        f = open(infile, "r")
        df = [str(line) for line in f]
        f.close()
        df = pd.DataFrame(df, columns=['msg'])

        # turn to pyspark dataframe
        schema = StructType([StructField('msg', StringType(), False)])
        df = spark.createDataFrame(df, schema)

        # write log as csv in HDFS
        (
            df
            .coalesce(1)
            .write
            .save(outfile, header=True, format='csv', mode='overwrite')
        )
        os.remove(infile)
    else:
        print(f"{infile} doesn't exist.")


def find(key, dictionary):

    """
    Returns all values in a dictionary (with nested dictionaries) that have
    to the user-specified key.

    Parameters
    ----------
    key : string
        The key to look for inside the dictionary.

    dictionary : python dictionary
        The dictionary to look for the user-specified key.

    Returns
    -------
    Python generator object - cane be turned into a list.
    """
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in find(key, d):
                    yield result

def melt_df(df, id_vars, value_vars, var_name, value_name):
    """
    Converts spark dataframe from wide to long format.
    Parameters
    ----------
    df : Spark dataframe
    id_vars :
    value_vars :
    var_name :
    value_name :

    Returns
    -------
    Spark dataframe

    Notes
    -----
    https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.melt.html
    https://www.mien.in/2018/03/25/reshaping-dataframe-using-pivot-and-melt-in-apache-spark-and-pandas/

    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(*(
        F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

    return _tmp.select(*cols)


def get_hdfs_files(
    directory,
    recursive=False,
    strings_to_search_for=[""],
    decoder="utf-8"
):
    """
    Use a terminal command in python to get equivalent of os.listdir() from HDFS.

    Parameters
    ----------
    directory : string
        the directory path in which to search

    recursive : bool
        flag for whether or not to do a recursive search

    strings_to_search_for : list of strings
        If strings_to_search_for is specified, it will return only files that contain
        all those strings (e.g. [".csv"]).

    decoder : string

    Returns
    -------
    matching_files : list
        list of the matching files
    """

    command = (
        f"hdfs dfs -ls -C -R {directory}"
        if recursive
        else f"hdfs dfs -ls -C {directory}"
    )

    p = subprocess.Popen(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT
                        )

    files = [path.decode(decoder).rstrip("\n") for path in p.stdout.readlines()]

    matching_files = [
        f for f in files if all([s.lower() in f.lower() for s in strings_to_search_for])
    ]

    return matching_files
