##################################################################################################################################################
##
##  Data Comparison Script for VRP Project
##  Author: Fred Ng
##  Date: 11/14/2017
##
##  Description: This script will compare the Student Stop dataset that was made
##  manually and the dataset that was created using a script. This will check 
##  each stop's attritubes against each other to see how accurate the datasets 
##  are. 
##
##  Version: 1.0
##
##################################################################################################################################################

# Importing modules

print "Importing modules..."

from pandas import DataFrame, read_csv

import sys
import pandas as pd
import numpy as np
import random
import datetime as dt
import time

print "Importing modules complete!"

# Importing csv into dataframes

print "Importing dataframes..."

df1 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\StudentD31092117.csv')
df2 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\StudentD31092117.csv')

print "Dataframe importing complete!"

# print list(df1)
# print list(df2)



# print df2[~df2.OSIS_ID.isin(df1.OSIS_ID.values)]

# print df1[~df1.OSIS_ID.isin(df2.OSIS_ID.values)]


































