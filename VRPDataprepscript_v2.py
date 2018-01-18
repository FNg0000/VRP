##################################################################################################################################################
##
##  Data Prep Script for VRP Project
##  Author: Fred Ng
##  Date: 11/14/2017
##
##  Description: This script will combine the GE and SE Student Report files
##  and use them as the base schema. Other datasets that will be used include
##  GIS Students, GIS Schools, GIS Stop Route, GIS Bus Garages, Travel Time,
##  Medical Alert Code, Ambulatory Code, New Vehicle Capacity, and ALT PM Stops. 
##
##  Version: 1.0
##
##################################################################################################################################################

# Importing modules

print "Importing modules..."

from pandas import DataFrame, read_csv

import matplotlib
import sys
import pandas as pd
import numpy as np
import random
import datetime as dt
import time



print "Importing modules complete!"

# Importing csv into dataframes

print "Importing dataframes..."

df1 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\StudentsReport_GE_09_21_17.csv')
df2 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\StudentsReport_SE_09_21_17.csv')
##df3 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\ALT_PM_BUS_STOPS.csv')
##df4 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\AMBULATORYCODE.csv')
##df5 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\GIS_SI_BUS_GARAGES.csv')
##df6 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\GIS_SI_SCHOOLS.csv')


##df9 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\MEDICALALERTCODE.csv')
##df10 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\TRAVELTIME.csv')
##df11 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\NEWVEHICLECAPACITY.csv')

print "Dataframe importing complete!"

# Concatenating the Student Report GE and SE together and dropping duplicate Student IDs by keeping the last duplicate

print "Now concatenating GE and SE into a single dataframe..."

dfconcat = [df1, df2]

Count_Row = df1.shape[0]
print "Number of student stops: ", Count_Row
Count_Row = df2.shape[0]
print "Number of student stops: ", Count_Row

dfresult1 = pd.concat(dfconcat, axis=0, join='outer', join_axes=None, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)

Count_Row = dfresult1.shape[0]
print "Number of student stops: ", Count_Row

print "Concatenate success!"

print "Now filtering out district 31 students only..."

dfresult2 = dfresult1.loc[dfresult1["School district"] == 31]

Count_Row = dfresult2.shape[0]
print "Number of student stops: ", Count_Row

print "Filter success!"

# Dropping columns from the dataframe

print "Now dropping columns and duplicate rows from the dataframe..."

dfresult3 = dfresult2.drop(dfresult2.columns[[1,2,3,6,7,8,27,28,35,37,71,74,77,80,81,82,83,88,89,90,91,92,93,94,95,96]], axis=1).drop_duplicates(subset=['OSIS ID'],keep='last').reset_index(drop=True)

Count_Row = dfresult3.shape[0]
print "Number of student stops: ", Count_Row

print "Drop success!"

# Rename columns to their correct column names

print "Renaming columns..."

dfresult4 = dfresult3.rename(columns={'AM Address':'Address', 'AM city':'City', 'AM zip code': 'Zip code', 
                                        'AM GE Stop location': 'AM Stop location', 'AM GE Stop time': 'AM_Stop_time', 
                                        'PM GE Stop location': 'PM Stop location', 'PM GE Stop time': 'PM Stop time' ,
                                        'ALT PM address': 'ALT PM location', 'AM Route#': 'AM Route num', 'AM Sequence#': 'AM Sequence num', 
                                        'PM Route#': 'PM Route num', 'PM Sequence#': 'PM Sequence num', 'After 4 Sequence#': 'After 4 Sequence num'})

print "Renaming success!"

# Replacing spaces with underscore in column headers

print "Replacing spaces with underscore in column headers..."

dfresult4.columns = pd.Series(dfresult4.columns).str.replace(' ','_')

print "Repacement success!"

# Import GIS Students into a dataframe. Then selecting only the relevant columns. Then renaming the columns.

print "Importing GIS Students into a dataframe. Selecting out Student ID, Xcoord, and Ycoord columns. Then renaming them..."

df3 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\GIS_SI_STUDENTS.csv')

df3_1 = df3[['Student_ID', 'XCoordinate', 'YCoordinate']]

df3_2 = df3_1.rename(columns={'XCoordinate': 'Address_XCOORD', 'YCoordinate': 'Address_YCOORD'})

print "Import done!"

# Joining the Student Stops dataframe with the GIS Student dataframe, based on OSIS ID and Student ID from their respective dataframes, for the Student's XY Coords.

print "Joining XY Coordinate columns from GIS Students to Student Stops dataframe"

dfresult5 = pd.merge(dfresult4, df3_2, how='outer', on=None,  left_on='OSIS_ID', right_on='Student_ID').drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

Count_Row = dfresult5.shape[0]
print "Number of student stops: ", Count_Row

print "Joining complete!"

# Importing the GIS Stop Route Data into a dataframe. Then selecting only the relevant columns. Then renaming the columns.

print "Importing GIS Students into a dataframe. Selecting out Student ID, Xcoord, and Ycoord columns. Then renaming them..."

df4 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\GIS_SI_STOPROUTEDATA.csv')

df4_1 = df4[['OPT_School', 'Stop_ID', 'Longitude', 'Latitude']]

df4_2 = df4_1.rename(columns={'Longitude': 'AM_Stop_XCOORD', 'Latitude': 'AM_Stop_YCOORD'})

df4_3 = df4_1.rename(columns={'Longitude': 'PM_Stop_XCOORD', 'Latitude': 'PM_Stop_YCOORD'})

# Joining the Student Stops dataframe with the GIS Stop Route dataframe, based on OPT Code and Stop ID from their respective dataframes, for the AM and PM stop XY Coords.

print "Joining AM AND PM stop XY Coordinate columns from GIS STOP ROUTE to Student Stops dataframe"

dfresult6 = pd.merge(dfresult5, df4_2, how='outer', on=None,  left_on=['AM_GE_Stop_number', 'School_code'], right_on=['Stop_ID','OPT_School']).drop_duplicates(subset=['OSIS_ID'],keep='last')

Count_Row = dfresult6.shape[0]

print "Number of student stops: ", Count_Row

dfresult7 = pd.merge(dfresult6, df4_3, how='outer', on=None,  left_on=['PM_GE_Stop_number', 'School_code'], right_on=['Stop_ID','OPT_School']).drop_duplicates(subset=['OSIS_ID'],keep='last')

Count_Row = dfresult7.shape[0]

print "Number of student stops: ", Count_Row

print "Joining complete!"

# Adding Stop location and XY Coordinates for SE Student's AM and PM Stops. 

print "Adding Stop location and XY Coordinates for SE Student's AM and PM Stops..."

dfresult7.AM_Stop_location = dfresult7.Address.where(dfresult7.Service_type == 'SE', dfresult7.AM_Stop_location)
dfresult7.AM_Stop_XCOORD = dfresult7.Address_XCOORD.where(dfresult7.Service_type == 'SE', dfresult7.AM_Stop_XCOORD)
dfresult7.AM_Stop_YCOORD = dfresult7.Address_YCOORD.where(dfresult7.Service_type == 'SE', dfresult7.AM_Stop_YCOORD)

dfresult7.PM_Stop_location = dfresult7.Address.where(dfresult7.Service_type == 'SE', dfresult7.PM_Stop_location)
dfresult7.PM_Stop_XCOORD = dfresult7.Address_XCOORD.where(dfresult7.Service_type == 'SE', dfresult7.PM_Stop_XCOORD)
dfresult7.PM_Stop_YCOORD = dfresult7.Address_YCOORD.where(dfresult7.Service_type == 'SE', dfresult7.PM_Stop_YCOORD)

print "Adding complete!"

# Adding columns for stop time and session time values and converting them into second.

print "Adding columns for stop time and session time values and converting them into seconds..."  

#for column in dfresult7:
#    if dfresult7.columns.str.contains('_Stop_time'):
#        loc = dfresult7.get_loc()
#        colname = dfresult7.columns[loc]
#        colname2 = colname[:-10]
#        print colname2
#        dfresult7['AM_Stop_time'] = pd.to_datetime(dfresult7['AM_Stop_time']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#        dfresult7['AM_Stop_time_seconds'] = dfresult7['AM_Stop_time'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#        break 

dfresult7['AM_Stop_time'] = dfresult7['AM_Stop_time'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['AM_Stop_time']),'AM_Stop_time'] = pd.to_datetime(dfresult7['AM_Stop_time']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7['AM_Stop_time_seconds'] = dfresult7.loc[pd.notnull(dfresult7['AM_Stop_time']),'AM_Stop_time'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60

# This truncates the row in the column for 5 characters from the right. This is done to remove the "AM" or "PM" from the row. 

dfresult7['PM_Stop_time'] = dfresult7['PM_Stop_time'].str[:5]

# This selects rows from the column that are not blank. Then it converts the time value into %H:%M if the row is not null. And then populates them with the 

#dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'] = pd.to_datetime(dfresult7['PM_Stop_time']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'] = dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['PM_Stop_time_seconds'] = dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#
#dfresult7['After_4_Stop_time'] = dfresult7['After_4_Stop_time'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'] = pd.to_datetime(dfresult7['After_4_Stop_time']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'] = dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['PM_Stop_time_seconds'] = dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#
#dfresult7['School_Session_Time_AM_Monday'] = dfresult7['School_Session_Time_AM__Monday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday']),'School_Session_Time_AM_Monday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Monday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7['School_Session_Time_AM_Monday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday']),'School_Session_Time_AM_Monday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Monday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday_seconds']),'School_Session_Time_AM_Monday_seconds'] - 1500
#dfresult7['Monday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday_seconds']),'School_Session_Time_AM_Monday_seconds'] - 600
#
#dfresult7['School_Session_Time_AM_Tuesday'] = dfresult7['School_Session_Time_AM__Tuesday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday']),'School_Session_Time_AM_Tuesday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Tuesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7['School_Session_Time_AM_Tuesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday']),'School_Session_Time_AM_Tuesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Tuesday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday_seconds']),'School_Session_Time_AM_Tuesday_seconds'] - 1500
#dfresult7['Tuesday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday_seconds']),'School_Session_Time_AM_Tuesday_seconds'] - 600
#
#dfresult7['School_Session_Time_AM_Wednesday'] = dfresult7['School_Session_Time_AM__Wednesday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday']),'School_Session_Time_AM_Wednesday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Wednesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7['School_Session_Time_AM_Wednesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday']),'School_Session_Time_AM_Wednesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Wednesday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday_seconds']),'School_Session_Time_AM_Wednesday_seconds'] - 1500
#dfresult7['Wednesday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday_seconds']),'School_Session_Time_AM_Wednesday_seconds'] - 600
#
#dfresult7['School_Session_Time_AM_Thursday'] = dfresult7['School_Session_Time_AM__Thursday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday']),'School_Session_Time_AM_Thursday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Thursday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7['School_Session_Time_AM_Thursday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday']),'School_Session_Time_AM_Thursday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Thursday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday_seconds']),'School_Session_Time_AM_Thursday_seconds'] - 1500
#dfresult7['Thursday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday_seconds']),'School_Session_Time_AM_Thursday_seconds'] - 600
#
#dfresult7['School_Session_Time_AM_Friday'] = dfresult7['School_Session_Time_AM__Friday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday']),'School_Session_Time_AM_Friday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Friday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7['School_Session_Time_AM_Friday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday']),'School_Session_Time_AM_Friday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Friday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday_seconds']),'School_Session_Time_AM_Friday_seconds'] - 1500
#dfresult7['Friday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday_seconds']),'School_Session_Time_AM_Friday_seconds'] - 600
#
#Count_Row = dfresult7.shape[0]
#
#print "Number of student stops: ", Count_Row
#
#dfresult7['School_Session_Time_PM_Monday'] = dfresult7['School_Session_Time_PM__Monday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Monday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_PM_Monday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Monday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday_seconds']),'School_Session_Time_PM_Monday_seconds'] + 1500
#
#dfresult7['School_Session_Time_PM_Tuesday'] = dfresult7['School_Session_Time_PM__Tuesday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Tuesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_PM_Tuesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Tuesday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday_seconds']),'School_Session_Time_PM_Tuesday_seconds'] + 1500
#
#dfresult7['School_Session_Time_PM_Wednesday'] = dfresult7['School_Session_Time_PM__Wednesday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Wednesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_PM_Wednesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Wednesday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday_seconds']),'School_Session_Time_PM_Wednesday_seconds'] + 1500
#
#dfresult7['School_Session_Time_PM_Thursday'] = dfresult7['School_Session_Time_PM__Thursday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Thursday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_PM_Thursday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Thursday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday_seconds']),'School_Session_Time_PM_Thursday_seconds'] + 1500
#
#dfresult7['School_Session_Time_PM_Friday'] = dfresult7['School_Session_Time_PM__Friday'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Friday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_PM_Friday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Friday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday_seconds']),'School_Session_Time_PM_Friday_seconds'] + 1500
#
#Count_Row = dfresult7.shape[0]
#
#print "Number of student stops: ", Count_Row

# This section is for School Session Time Exemption which will not be used for this project. Also needs heavy refinement to make work. 

#dfresult7['School_Session_Time_Exemption'] = dfresult7['School_Session_Time_Exemption'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'] = pd.to_datetime(dfresult7['School_Session_Time_Exemption']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_Exemption_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Exemption_Pickup_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption_seconds']),'School_Session_Time_Exemption_seconds'] + 1500

print "Adding columns success!"

# Adding a column for Age_group. Any student who was is older than 13 by September 21st would be placed into the "Older" category while 13 or younger would be placed in the "Younger" category.

print "Adding columns for Age_group and calculating age group students belong in..."

dfresult7['startdate'] = pd.to_datetime('9/21/2017', format = '%m/%d/%Y')
dfresult7['Date_of_birth'] = pd.to_datetime(dfresult7['Date_of_birth'])
dfresult7['Age'] = ((dfresult7['startdate'] - dfresult7['Date_of_birth']).dt.days / 365).apply(np.floor)
dfresult7.loc[dfresult7['Age'] > 13,'Age_group'] = 'Older'
dfresult7.loc[dfresult7['Age'] <= 13,'Age_group'] = 'Younger'

print "Adding column for Age Group success!"



# Reording the columns

##print "Reording the columns..."

##df = df[[col1, col2, col3, col5, col4]]

##print "Reording columns done!"


##print dfresult5

dfresult7.to_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\StudentD31092117.csv', sep=',')
#dfresult5.to_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\StudentD31092117_2.csv', sep=',')

print "done!"

print dfresult7.dtypes

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

print is_string_dtype(dfresult7['startdate'])
print is_numeric_dtype(dfresult7['startdate'])

print is_string_dtype(dfresult7['Date_of_birth'])
print is_numeric_dtype(dfresult7['Date_of_birth'])


