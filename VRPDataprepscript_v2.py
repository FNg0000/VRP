# -*- coding: utf-8 -*-
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

print "Dataframe importing complete!"

# Dropping all duplicate GE students. This creates an error later on and it is difficult to determine which is the correct entry for the student.

df1 = df1.drop_duplicates(subset=['OSIS ID'],keep=False).reset_index(drop=True)

print "Dropping duplicates complete!"

# Dropping all GE students who do not have a GE Stop assigned to them. They are not routed for the purpose of this project.

print "Dropping GE students who do not have a route..."

df1 = df1.dropna(subset=['AM GE Stop number']).reset_index(drop=True)

print "Dropping complete!"

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

dfresult2 = dfresult1.loc[dfresult1['School district'] == 31].reset_index(drop=True)

# Removing students that are not routed on any routes.

print "Now removing students who are not assigned to any routes..."

dfresult2 = dfresult2.dropna(subset=['AM Route#']).reset_index(drop=True)

print "Removal complete!"

Count_Row = dfresult2.shape[0]
print "Number of student stops: ", Count_Row

print "Filter success!"

# Extracting desired columns from the dataframe into a new dataframe

print "Now extracting the columns and duplicate rows from the dataframe..."

dfresult3 = dfresult2[['Service type' , 'OSIS ID' , 'Date of birth' , 'AM Address' , 
                        'AM city' , 'Home boro' , 'AM zip code' , 'Home district' , 
                        'AM GE Stop location' , 'AM GE Stop number' , 'AM GE Stop time' , 
                        'PM GE Stop location' , 'PM GE Stop number' , 'PM GE Stop time' , 
                        'ALT PM address' , 'ALT PM days' , 'After 4 Stop location' , 'After 4 Stop number' , 
                        'After 4 Stop time' , 'School code' , 'School name' , 'School address' , 'School city' , 
                        'School state' , 'School zip code' , 'School boro' , 'School district' , 'School Affiliation type' , 
                        'School Session Time AM  Monday' , 'School Session Time AM  Tuesday' , 'School Session Time AM  Wednesday' , 
                        'School Session Time AM  Thursday' , 'School Session Time AM  Friday' , 'School Session Time PM  Monday' , 
                        'School Session Time PM  Tuesday' , 'School Session Time PM  Wednesday' , 'School Session Time PM  Thursday' , 
                        'School Session Time PM  Friday' , 'School Session Time Exemption' , 'AM Route#' , 'AM Sequence#' , 
                        'AM bus type' , 'AM Vendor Id' , 'AM Vendor name' , 'PM Route#' , 'PM Sequence#' , 'PM bus type' , 
                        'PM Vendor Id' , 'PM Vendor name' , 'ALT PM route' , 'ALT PM sequence' , 'ALT PM bus type' , 
                        'ALT PM Vendor Id' , 'ALT PM Vendor name' , 'After 4 route' , 'After 4 Sequence#' , 'After 4 bus type' , 
                        'After 4 Vendor Id' , 'After 4 Vendor name' , 'AM Garage name' , 'AM Garage address' , 'PM Garage name' , 
                        'PM Garage address' , 'ALT PM Garage name' , 'ALT PM AM Garage address' , 'After 4 Garage name' , 
                        'After 4 Garage address' , 'Medical Alert code' , 'Description of Medical Alert code' , 'Ambulatory code' , 
                        'Description of Ambulatory code'
                        ]].drop_duplicates(subset=['OSIS ID'],keep='last').reset_index(drop=True)


#dfresult3 = dfresult2.drop(dfresult2.columns[[1,2,3,6,7,8,27,28,35,37,71,74,77,80,81,82,83,88,89,90,91,92,93,94,95,96]], axis=1).drop_duplicates(subset=['OSIS ID'],keep='last').reset_index(drop=True)

Count_Row = dfresult3.shape[0]
print "Number of student stops: ", Count_Row

print "Extraction success!"

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

# Joining the Student Stops dataframe with the GIS Student dataframe, based on OSIS ID and Student ID from their respective dataframes, for the Student's XY Coords.By doing an outer merge, it keeps all the rows on the left dataframe even if there are no values from the right data frame.


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

# This selects rows from the column that are not blank. Then it converts the time value into %H:%M if the row is not null. And then replaces the original value in the dataframe. The output is a string.

dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'] = pd.to_datetime(dfresult7['PM_Stop_time']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')

# This line does something similar and has three parts. The first partis to convert PM Stop Time into 24 hour time. 
# This is done by to extracting out the hour value of the string by spliting the string by ':' and then using apply and lambda. Since it's pm stop time, 12 added to it. This is converted back into a string value. 
# The second part is to concatanate the back in the ':'
# The third part is the same as the first part except no additional value is added to the minute field. 
# This string is then supplanted back into the column where there are actually values.

dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'] = dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'].str.split(':').apply(lambda x: int(x[1])).astype(str)

# This next part selects out rows where PM Stop time is not null and converts it into seconds. It does so by by multiplying the hour value by 60 to get minutes, adding it to minutes, and then multiplying the value by 60 to get seconds.

dfresult7['PM_Stop_time_seconds'] = dfresult7.loc[pd.notnull(dfresult7['PM_Stop_time']),'PM_Stop_time'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60

dfresult7['After_4_Stop_time'] = dfresult7['After_4_Stop_time'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'] = pd.to_datetime(dfresult7['After_4_Stop_time']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'] = dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'].str.split(':').apply(lambda x: int(x[1])).astype(str)
dfresult7['After_4_Stop_time_seconds'] = dfresult7.loc[pd.notnull(dfresult7['After_4_Stop_time']),'After_4_Stop_time'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60

dfresult7['School_Session_Time_AM_Monday'] = dfresult7['School_Session_Time_AM__Monday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday']),'School_Session_Time_AM_Monday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Monday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7['School_Session_Time_AM_Monday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday']),'School_Session_Time_AM_Monday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Monday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday_seconds']),'School_Session_Time_AM_Monday_seconds'] - 1500
dfresult7['Monday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Monday_seconds']),'School_Session_Time_AM_Monday_seconds'] - 300

dfresult7['School_Session_Time_AM_Tuesday'] = dfresult7['School_Session_Time_AM__Tuesday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday']),'School_Session_Time_AM_Tuesday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Tuesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7['School_Session_Time_AM_Tuesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday']),'School_Session_Time_AM_Tuesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Tuesday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday_seconds']),'School_Session_Time_AM_Tuesday_seconds'] - 1500
dfresult7['Tuesday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Tuesday_seconds']),'School_Session_Time_AM_Tuesday_seconds'] - 300

dfresult7['School_Session_Time_AM_Wednesday'] = dfresult7['School_Session_Time_AM__Wednesday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday']),'School_Session_Time_AM_Wednesday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Wednesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7['School_Session_Time_AM_Wednesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday']),'School_Session_Time_AM_Wednesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Wednesday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday_seconds']),'School_Session_Time_AM_Wednesday_seconds'] - 1500
dfresult7['Wednesday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Wednesday_seconds']),'School_Session_Time_AM_Wednesday_seconds'] - 300

dfresult7['School_Session_Time_AM_Thursday'] = dfresult7['School_Session_Time_AM__Thursday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday']),'School_Session_Time_AM_Thursday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Thursday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7['School_Session_Time_AM_Thursday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday']),'School_Session_Time_AM_Thursday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Thursday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday_seconds']),'School_Session_Time_AM_Thursday_seconds'] - 1500
dfresult7['Thursday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Thursday_seconds']),'School_Session_Time_AM_Thursday_seconds'] - 300

dfresult7['School_Session_Time_AM_Friday'] = dfresult7['School_Session_Time_AM__Friday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday']),'School_Session_Time_AM_Friday'] = pd.to_datetime(dfresult7['School_Session_Time_AM_Friday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7['School_Session_Time_AM_Friday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday']),'School_Session_Time_AM_Friday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Friday_AM_dropoff_time_window_start']  = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday_seconds']),'School_Session_Time_AM_Friday_seconds'] - 1500
dfresult7['Friday_AM_dropoff_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_AM_Friday_seconds']),'School_Session_Time_AM_Friday_seconds'] - 300

Count_Row = dfresult7.shape[0]

print "Number of student stops: ", Count_Row

dfresult7['School_Session_Time_PM_Monday'] = dfresult7['School_Session_Time_PM__Monday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Monday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
dfresult7['School_Session_Time_PM_Monday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday']),'School_Session_Time_PM_Monday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Monday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Monday_seconds']),'School_Session_Time_PM_Monday_seconds'] + 1800

dfresult7['School_Session_Time_PM_Tuesday'] = dfresult7['School_Session_Time_PM__Tuesday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Tuesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
dfresult7['School_Session_Time_PM_Tuesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday']),'School_Session_Time_PM_Tuesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Tuesday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Tuesday_seconds']),'School_Session_Time_PM_Tuesday_seconds'] + 1800

dfresult7['School_Session_Time_PM_Wednesday'] = dfresult7['School_Session_Time_PM__Wednesday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Wednesday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
dfresult7['School_Session_Time_PM_Wednesday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday']),'School_Session_Time_PM_Wednesday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Wednesday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Wednesday_seconds']),'School_Session_Time_PM_Wednesday_seconds'] + 1800

dfresult7['School_Session_Time_PM_Thursday'] = dfresult7['School_Session_Time_PM__Thursday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Thursday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
dfresult7['School_Session_Time_PM_Thursday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday']),'School_Session_Time_PM_Thursday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Thursday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Thursday_seconds']),'School_Session_Time_PM_Thursday_seconds'] + 1800

dfresult7['School_Session_Time_PM_Friday'] = dfresult7['School_Session_Time_PM__Friday'].str[:5]
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'] = pd.to_datetime(dfresult7['School_Session_Time_PM_Friday']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'].str.split(':').apply(lambda x: int(x[1])).astype(str)
dfresult7['School_Session_Time_PM_Friday_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday']),'School_Session_Time_PM_Friday'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
dfresult7['Friday_PM_dropoff_PM_Pickup_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_PM_Friday_seconds']),'School_Session_Time_PM_Friday_seconds'] + 1800

Count_Row = dfresult7.shape[0]

print "Number of student stops: ", Count_Row

# This section is for School Session Time Exemption which will not be used for this project. Also needs heavy refinement to make work. 

#dfresult7['School_Session_Time_Exemption'] = dfresult7['School_Session_Time_Exemption'].str[:5]
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'] = pd.to_datetime(dfresult7['School_Session_Time_Exemption']).apply(lambda x: x.strftime('%H:%M')if not pd.isnull(x) else '0:00')
#dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'].str.split(':').apply(lambda x: int(x[0]) + 12).astype(str) + ":" + dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'].str.split(':').apply(lambda x: int(x[1])).astype(str)
#dfresult7['School_Session_Time_Exemption_seconds'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption']),'School_Session_Time_Exemption'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) * 60
#dfresult7['Exemption_Pickup_time_window_end'] = dfresult7.loc[pd.notnull(dfresult7['School_Session_Time_Exemption_seconds']),'School_Session_Time_Exemption_seconds'] + 1500

print "Adding columns success!"

# Adding a column for Age_group. Any student who was is older than 13 by September 21st would be placed into the "Older" category while 13 or younger would be placed in the "Younger" category.

print "Adding columns for Age_group and calculating age group students belong in..."

dfresult7['startdate'] = pd.to_datetime("9/21/2017", format = '%m/%d/%Y')
dfresult7['Date_of_birth'] = pd.to_datetime(dfresult7['Date_of_birth'])
dfresult7['Age'] = ((dfresult7['startdate'] - dfresult7['Date_of_birth']).dt.days / 365).apply(np.floor)
dfresult7.loc[dfresult7['Age'] > 13,'Age_group'] = "Older"
dfresult7.loc[dfresult7['Age'] <= 13,'Age_group'] = "Younger"

print "Adding column for Age Group success!"

# Importing the GIS Schools Data into a dataframe. Then selecting only the relevant columns. Then renaming the columns.

print "Importing GIS Schools into a dataframe. Selecting out OPT_Code, nxcoord, and nycoord columns. Then renaming them..."

df5 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\GIS_SI_SCHOOLS.csv')

df5_1 = df5[['OPT_Code', 'nxcoord', 'nycoord']]

df5_2 = df5_1.rename(columns={'nxcoord': 'School_XCOORD', 'nycoord': 'School_YCOORD'})

print "Importing complete!"

# Joining the Student Stops dataframe with the GIS Schools dataframe, based on OPT Code afrom their respective dataframes.

print "Joining School XY Coordinate columns from GIS Schoools to Student Stops dataframe"

dfresult8 = pd.merge(dfresult7, df5_2, how='outer', on=None,  left_on=['School_code'], right_on=['OPT_Code']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

print "Join complete!"

Count_Row = dfresult8.shape[0]

print "Number of student stops: ", Count_Row

# This part will separate the GE and SE Standard Bus (SB) types into SBN (normal) and SBH (handicapped)respectively for the AM_bus_type and PM_bus_type fields.

print "Updating SB types in the AM and PM bus type fields..."

dfresult8.loc[(dfresult8['Service_type'] == 'GE') & (dfresult8['AM_bus_type'] == 'SB'), 'AM_bus_type'] = "SBN"
dfresult8.loc[(dfresult8['Service_type'] == 'SE') & (dfresult8['AM_bus_type'] == 'SB'), 'AM_bus_type'] = "SBH"
dfresult8.loc[(dfresult8['Service_type'] == 'GE') & (dfresult8['PM_bus_type'] == 'SB'), 'PM_bus_type'] = "SBN"
dfresult8.loc[(dfresult8['Service_type'] == 'SE') & (dfresult8['PM_bus_type'] == 'SB'), 'PM_bus_type'] = "SBN"

print "Updating complete!"

# Importing the New Vehicle Capacity table into a dataframe to be used in creating the vehicle information that service each stop.

print "Importing New Vehicle Capacity into a dataframe and formating column headers..."

df6 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\NEWVEHICLECAPACITY.csv')

df6_1 = df6[['Vehicle type new', 'Vehicle Type Designation', 'Maximum Seated Capacity', 'Maximum Wheelchair Capacity', 'Ambulance Capacity', 'Attendant', 'Lift']]

df6_2 = df6_1.rename(columns={'Vehicle type new' : 'AM bus type new' , 'Vehicle Type Designation' : 'AM Vehicle Type Designation' , 'Maximum Seated Capacity' : 'AM Seated Passenger Capacity' , 'Maximum Wheelchair Capacity' : 'AM Wheelchair Capacity',
                                'Ambulance Capacity' : 'AM Ambulance Capacity', 'Attendant' : 'AM Attendant', 'Lift' : 'AM Lift Capable'})

df6_2.columns = pd.Series(df6_2.columns).str.replace(' ','_')

df6_3 = df6_1.rename(columns={'Vehicle type new' : 'PM bus type new' , 'Vehicle Type Designation' : 'PM Vehicle Type Designation' , 'Maximum Seated Capacity' : 'PM Seated Passenger Capacity' , 'Maximum Wheelchair Capacity' : 'PM Wheelchair Capacity',
                                'Ambulance Capacity' : 'PM Ambulance Capacity', 'Attendant' : 'PM Attendant', 'Lift' : 'PM Lift Capable'})

df6_3.columns = pd.Series(df6_3.columns).str.replace(' ','_')

df6_4 = df6_1.rename(columns={'Vehicle type new' : 'ALT PM bus type new' , 'Vehicle Type Designation' : 'ALT PM Vehicle Type Designation' , 'Maximum Seated Capacity' : 'ALT PM Seated Passenger Capacity' , 'Maximum Wheelchair Capacity' : 'ALT PM Wheelchair Capacity',
                                'Ambulance Capacity' : 'ALT PM Ambulance Capacity', 'Attendant' : 'ALT PM Attendant', 'Lift' : 'ALT PM Lift Capable'})

df6_4.columns = pd.Series(df6_4.columns).str.replace(' ','_')

df6_5 = df6_1.rename(columns={'Vehicle type new' : 'After 4 bus type new' , 'Vehicle Type Designation' : 'After 4 Vehicle Type Designation' , 'Maximum Seated Capacity' : 'After 4 Seated Passenger Capacity' , 'Maximum Wheelchair Capacity' : 'After 4 Wheelchair Capacity',
                                'Ambulance Capacity' : 'After 4 Ambulance Capacity', 'Attendant' : 'After 4 Attendant', 'Lift' : 'After 4 Lift Capable'})

df6_5.columns = pd.Series(df6_5.columns).str.replace(' ','_')

print "Import and processing complete!"

# This next part joins the following columns from the NEWVEHICLECAPACITY dataframe using the updated AM/PM bus type fields : "AM_Seated_Passenger_Capacity", "AM_Wheelchair_Capacity", "AM_Ambulance_Capacity", "AM_Attendant", "AM_Lift_Capable"

print "Joining the new columns from New Vehicle Capacity to Student Stops dataframe"

dfresult9 = pd.merge(dfresult8, df6_2, how='outer', on=None,  left_on=['AM_bus_type'], right_on=['AM_Vehicle_Type_Designation']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

dfresult10 = pd.merge(dfresult9, df6_3, how='outer', on=None,  left_on=['PM_bus_type'], right_on=['PM_Vehicle_Type_Designation']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

dfresult11 = pd.merge(dfresult10, df6_4, how='outer', on=None,  left_on=['ALT_PM_bus_type'], right_on=['ALT_PM_Vehicle_Type_Designation']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

dfresult12 = pd.merge(dfresult11, df6_5, how='outer', on=None,  left_on=['After_4_bus_type'], right_on=['After_4_Vehicle_Type_Designation']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

print "Join complete!"

Count_Row = dfresult12.shape[0]

print "Number of student stops: ", Count_Row

# This splits the AM_Garage/PM_Garage/ALT_PM_AM_Garage/After_4 address into four columns since the entire address is concatenated with ":" into one column. The columns will be used to merge with GIS Garages later to bring in the XY coords of the garages.

print "Now splitting garage addresses up..."

dfresult12['AM_Garage_city'] = dfresult12.loc[pd.notnull(dfresult12['AM_Garage_address']),'AM_Garage_address'].str.split(':').apply(lambda x: x[1]).astype(str)

dfresult12['AM_Garage_state'] = dfresult12.loc[pd.notnull(dfresult12['AM_Garage_address']),'AM_Garage_address'].str.split(':').apply(lambda x: x[2]).astype(str)

dfresult12['AM_Garage_zip'] = dfresult12.loc[pd.notnull(dfresult12['AM_Garage_address']),'AM_Garage_address'].str.split(':').apply(lambda x: x[3]).astype(str)

dfresult12['AM_Garage_address'] = dfresult12.loc[pd.notnull(dfresult12['AM_Garage_address']),'AM_Garage_address'].str.split(':').apply(lambda x: x[0]).astype(str)

dfresult12['PM_Garage_city'] = dfresult12.loc[pd.notnull(dfresult12['PM_Garage_address']),'PM_Garage_address'].str.split(':').apply(lambda x: x[1]).astype(str)

dfresult12['PM_Garage_state'] = dfresult12.loc[pd.notnull(dfresult12['PM_Garage_address']),'PM_Garage_address'].str.split(':').apply(lambda x: x[2]).astype(str)

dfresult12['PM_Garage_zip'] = dfresult12.loc[pd.notnull(dfresult12['PM_Garage_address']),'PM_Garage_address'].str.split(':').apply(lambda x: x[3]).astype(str)

dfresult12['PM_Garage_address'] = dfresult12.loc[pd.notnull(dfresult12['PM_Garage_address']),'PM_Garage_address'].str.split(':').apply(lambda x: x[0]).astype(str)

dfresult12['ALT_PM_Garage_city'] = dfresult12.loc[pd.notnull(dfresult12['ALT_PM_AM_Garage_address']),'ALT_PM_AM_Garage_address'].str.split(':').apply(lambda x: x[1]).astype(str)

dfresult12['ALT_PM_Garage_state'] = dfresult12.loc[pd.notnull(dfresult12['ALT_PM_AM_Garage_address']),'ALT_PM_AM_Garage_address'].str.split(':').apply(lambda x: x[2]).astype(str)

dfresult12['ALT_PM_Garage_zip'] = dfresult12.loc[pd.notnull(dfresult12['ALT_PM_AM_Garage_address']),'ALT_PM_AM_Garage_address'].str.split(':').apply(lambda x: x[3]).astype(str)

dfresult12['ALT_PM_Garage_address'] = dfresult12.loc[pd.notnull(dfresult12['ALT_PM_AM_Garage_address']),'ALT_PM_AM_Garage_address'].str.split(':').apply(lambda x: x[0]).astype(str)

dfresult12['After_4_Garage_city'] = dfresult12.loc[pd.notnull(dfresult12['After_4_Garage_address']),'After_4_Garage_address'].str.split(':').apply(lambda x: x[1]).astype(str)

dfresult12['After_4_Garage_state'] = dfresult12.loc[pd.notnull(dfresult12['After_4_Garage_address']),'After_4_Garage_address'].str.split(':').apply(lambda x: x[2]).astype(str)

dfresult12['After_4_Garage_zip'] = dfresult12.loc[pd.notnull(dfresult12['After_4_Garage_address']),'After_4_Garage_address'].str.split(':').apply(lambda x: x[3]).astype(str)

dfresult12['After_4_Garage_address'] = dfresult12.loc[pd.notnull(dfresult12['After_4_Garage_address']),'After_4_Garage_address'].str.split(':').apply(lambda x: x[0]).astype(str)

print "Splitting complete!"

# Importing the GIS BUS GARAGES into a dataframe. Then selecting only the relevant columns. Then renaming the columns.

print "Importing GIS Schools into a dataframe. Selecting out OPT_Code, nxcoord, and nycoord columns. Then renaming them..."

df7 = pd.read_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\Data\GIS_SI_BUS_GARAGES.csv')

df7_1 = df7[['GarageAddress', 'POINT_X', 'POINT_Y']]

df7_2 = df7_1.rename(columns={'POINT_X': 'Garage_XCOORD', 'POINT_Y': 'Garage_YCOORD'})

print "Importing complete!"

# This splits the GarageAddress into two columns since the entire address is one string separated by "," and a space in one column. The columns will be used to merge with GIS Garages later to bring in the XY coords of the garages.

print "Now splitting GarageAddress in the GIS Garage dataframe..."

df7_2['Garage_city'] = df7_2.loc[pd.notnull(df7_2['GarageAddress']),'GarageAddress'].str.split(',').apply(lambda x: x[1]).astype(str)

# The output of the following line will be NY and then a zip code. Will need an additional line to separate out NY and the zip code.

df7_2['Garage_state'] = df7_2.loc[pd.notnull(df7_2['GarageAddress']),'GarageAddress'].str.split(',').apply(lambda x: x[2]).str.strip().astype(str)

df7_2['Garage_zip'] = df7_2.loc[pd.notnull(df7_2['Garage_state']),'Garage_state'].str.split(' ').apply(lambda x: x[1]).astype(str)

df7_2['Garage_state'] = df7_2.loc[pd.notnull(df7_2['Garage_state']),'Garage_state'].str.split(' ').apply(lambda x: x[0]).astype(str)

df7_2['Garage_address'] = df7_2.loc[pd.notnull(df7_2['GarageAddress']),'GarageAddress'].str.split(',').apply(lambda x: x[0]).astype(str)

df7_2 = df7_2.drop(df7_2.columns[[0,3,4]], axis=1).reset_index(drop=True)

print "Splitting complete!"

# This will further split Garage addresses into separate dataframes for AM/PM/ALT AM/After 4.

print "Creating new dataframes to house the garage address data for different busing times..."

df7_3 = df7_2.rename(columns={'Garage_address' : 'AM_Garage_address' , 'Garage_zip' : 'AM_Garage_zip' , 'Garage_XCOORD' : 'AM_Garage_XCOORD' , 'Garage_YCOORD' : 'AM_Garage_YCOORD'})

df7_4 = df7_2.rename(columns={'Garage_address' : 'PM_Garage_address' , 'Garage_zip' : 'PM_Garage_zip' , 'Garage_XCOORD' : 'PM_Garage_XCOORD' , 'Garage_YCOORD' : 'PM_Garage_YCOORD'})

df7_5 = df7_2.rename(columns={'Garage_address' : 'ALT_PM_Garage_address' , 'Garage_zip' : 'ALT_PM_Garage_zip' , 'Garage_XCOORD' : 'ALT_PM_Garage_XCOORD' , 'Garage_YCOORD' : 'ALT_PM_Garage_YCOORD'})

df7_6 = df7_2.rename(columns={'Garage_address' : 'After_4_Garage_address' , 'Garage_zip' : 'After_4_Garage_zip' , 'Garage_XCOORD' : 'After_4_Garage_XCOORD' , 'Garage_YCOORD' : 'After_4_Garage_YCOORD'})


print "Process complete!"

# This will merge the GIS Garages dataframe into the student stops dataframe. The two columns used in this merge will be garage address and the zip code.

print "Joining the 4 GIS Bus Garage data to the student stops dataframe..."

dfresult13 = pd.merge(dfresult12, df7_3, how='outer', on=None,  left_on=['AM_Garage_address' , 'AM_Garage_zip'], right_on=['AM_Garage_address' , 'AM_Garage_zip']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

dfresult14 = pd.merge(dfresult13, df7_4, how='outer', on=None,  left_on=['PM_Garage_address' , 'PM_Garage_zip'], right_on=['PM_Garage_address' , 'PM_Garage_zip']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

dfresult15 = pd.merge(dfresult14, df7_5, how='outer', on=None,  left_on=['ALT_PM_Garage_address', 'ALT_PM_Garage_zip'], right_on=['ALT_PM_Garage_address', 'ALT_PM_Garage_zip']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

dfresult16 = pd.merge(dfresult15, df7_6, how='outer', on=None,  left_on=['After_4_Garage_address' , 'After_4_Garage_zip'], right_on=['After_4_Garage_address' , 'After_4_Garage_zip']).drop_duplicates(subset=['OSIS_ID'],keep='last').reset_index(drop=True)

print "Join complete!"

# The next part will create new columns, with bolean TRUE and FALSE values to separate out the special needs for medical alerts and ambulatory codes. The first part calculates wheelchair demand. 

print "Creating a new column for MAC wheelchair and populating values..."

# This part will create a new column that will populate "TRUE" if the MAC is equal to "R" OR if AC is equal to W. Otherwise it will be false. This is done using numpy select with conditions and choices. 

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Medical_Alert_code'] == 'R'),
    (dfresult16['Ambulatory_code'] == 'W')]
choices = ['TRUE', 'TRUE']
dfresult16['Wheelchair'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

# Same thing in the previous part but more complex.

print "Creating a new column for wheelchair with a para and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Medical_Alert_code'] == 'F') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'C') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'G') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'H') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'K') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'L') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'N') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'P') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'Q') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'Z') & (dfresult16['Ambulatory_code'] == 'W')]
choices = ['TRUE', 'TRUE','TRUE','TRUE','TRUE','TRUE','TRUE','TRUE','TRUE','TRUE']
dfresult16['Wheelchair_wPara'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for Limited Travel Time and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Description_of_Medical_Alert_code'].str.contains('Limited Travel Time', na=False))]
choices = ['TRUE']
dfresult16['Limited_travel_time'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for A/C_demand and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Description_of_Medical_Alert_code'].str.contains('A/C', na=False))]
choices = ['TRUE']
dfresult16['A/C_demand'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for mini-wagon and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Description_of_Medical_Alert_code'].str.contains('Mini-wagon', na=False))]
choices = ['TRUE']
dfresult16['Mini_wagon'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for 3_seats and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Description_of_Medical_Alert_code'].str.contains('3 seats', na=False))]
choices = ['TRUE']
dfresult16['3_seats'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for 2_seats and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Description_of_Medical_Alert_code'].str.contains('2 seats', na=False))]
choices = ['TRUE']
dfresult16['2_seats'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for para and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Medical_Alert_code'] == 'C'),
    (dfresult16['Medical_Alert_code'] == '3'),
    (dfresult16['Medical_Alert_code'] == 'F') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'C') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'G') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'H') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'K') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'L') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'N') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'P') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'Q') & (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'Z') & (dfresult16['Ambulatory_code'] == 'W')]
choices = ['TRUE','TRUE','TRUE', 'TRUE','TRUE','TRUE','TRUE','TRUE','TRUE','TRUE','TRUE','TRUE']
dfresult16['Para'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

print "Creating a new column for Lift and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Description_of_Ambulatory_code'].str.contains('Lift', na=False))]
choices = ['TRUE']
dfresult16['Lift'] = np.select(conditions, choices, default='FALSE')

print "Complete!"

# This part of the script will now create columns for seated demands. 1 represents 1 student requires a seated demand of that type. 0 means the student does not require a seated demand of that type.

print "Creating a new column for Ambulatory seated demand and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Ambulatory_code'] == 'A'),
    (dfresult16['Ambulatory_code'] == 'B'),
    (dfresult16['Medical_Alert_code'] == 'C') & (dfresult16['Ambulatory_code'] == 'A'),
    (dfresult16['Medical_Alert_code'] == 'C') & (dfresult16['Ambulatory_code'] == 'B')]
choices = ['1','1','1', '1']
dfresult16['Ambulatory_demand'] = np.select(conditions, choices, default='0')

print "Complete!"

print "Creating a new column for Ambulatory seated demand and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Medical_Alert_code'] == 'R'),
    (dfresult16['Ambulatory_code'] == 'W')]
choices = ['1','1']
dfresult16['Wheelchair_demand'] = np.select(conditions, choices, default='0')

print "Complete!"

print "Creating a new column for seated demand and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Ambulatory_demand'] == '1'),
    (dfresult16['Wheelchair_wPara'] == 'TRUE'),
    (dfresult16['Wheelchair_demand'] == '1' ) & (dfresult16['Wheelchair_wPara'] == 'FALSE'),
    (dfresult16['Para'] == 'TRUE') & (dfresult16['Wheelchair_wPara'] == 'FALSE'),
    (dfresult16['3_seats'] == 'TRUE'),
    (dfresult16['2_seats'] == 'TRUE')]
choices = ['0','1','0','2','3','2']
dfresult16['Seated_Demand'] = np.select(conditions, choices, default='1')

print "Complete!"

print "Creating a new column for Outside Borough Travel and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Home_boro'] == dfresult16['School_boro'])]
choices = ['FALSE']
dfresult16['Outside_Borough_Travel'] = np.select(conditions, choices, default='TRUE')

print "Complete!"

# This part will create columns for total load/unload times for each student. 

print "Creating a new column for total load/unload times  and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Service_type'] == 'SE') & (dfresult16['Ambulatory_code'] != 'L') & (dfresult16['Ambulatory_code'] != 'W') & (dfresult16['Medical_Alert_code'] != 'R') & (dfresult16['Medical_Alert_code'] != 'V'),
    (dfresult16['Ambulatory_code'] == 'L'),
    (dfresult16['Ambulatory_code'] == 'W'),
    (dfresult16['Medical_Alert_code'] == 'R'),
    (dfresult16['Medical_Alert_code'] == 'V')]
choices = ['60','300','600','600','120']
dfresult16['Total_load_unload_time'] = np.select(conditions, choices, default='10')

print "Complete!"

# This part will create columns for maximum ride times for each student. 

print "Creating a new column for total load/unload times  and populating values..."

dfresult16 = pd.DataFrame(dfresult16)
conditions = [
    (dfresult16['Service_type'] == 'GE') & (dfresult16['Outside_Borough_Travel'] == 'FALSE') & (dfresult16['Limited_travel_time'] == 'FALSE'),
    (dfresult16['Service_type'] == 'GE') & (dfresult16['Outside_Borough_Travel'] == 'TRUE') & (dfresult16['Limited_travel_time'] == 'FALSE'),
    (dfresult16['Service_type'] == 'SE') & (dfresult16['Outside_Borough_Travel'] == 'FALSE') & (dfresult16['Limited_travel_time'] == 'FALSE'),
    (dfresult16['Service_type'] == 'SE') & (dfresult16['Outside_Borough_Travel'] == 'TRUE') & (dfresult16['Limited_travel_time'] == 'FALSE'),
    (dfresult16['Service_type'] == 'SE') & (dfresult16['Outside_Borough_Travel'] == 'FALSE') & (dfresult16['Limited_travel_time'] == 'TRUE'),
    (dfresult16['Service_type'] == 'SE') & (dfresult16['Outside_Borough_Travel'] == 'TRUE') & (dfresult16['Limited_travel_time'] == 'TRUE')]
choices = ['5400','10800','5400','6300','3600','4500']
dfresult16['Maximum_Ride_Time'] = np.select(conditions, choices, default='0')

print "Complete!"

Count_Row = dfresult16.shape[0]

print "Number of student stops: ", Count_Row

# Reording the columns

print "Reording the columns..."

dfresult17 = dfresult16[['Service_type' , 'OSIS_ID' , 'Age_group' , 'City' , 'Home_boro' , 'Zip_code' , 'Home_district' , 'Address_XCOORD' , 'Address_YCOORD' ,
                                'AM_Stop_location' , 'AM_GE_Stop_number' , 'AM_Stop_XCOORD' , 'AM_Stop_YCOORD' , 'AM_Stop_time' , 'AM_Stop_time_seconds' ,
                                'PM_Stop_location' , 'PM_GE_Stop_number' , 'PM_Stop_XCOORD' , 'PM_Stop_YCOORD' , 'PM_Stop_time' , 'PM_Stop_time_seconds' ,
                                'ALT_PM_location' , 'ALT_PM_days' , 
                                'After_4_Stop_location' , 'After_4_Stop_number' , 'After_4_Stop_time' , 'After_4_Stop_time_seconds' ,
                                'School_code' , 'School_name' , 'School_address' , 'School_city' , 'School_state' , 'School_zip_code' , 'School_boro' , 'School_district' , 'School_XCOORD' , 'School_YCOORD' , 'School_Affiliation_type' , 
                                'School_Session_Time_AM_Monday' , 'School_Session_Time_AM_Monday_seconds' , 'Monday_AM_dropoff_time_window_start' , 'Monday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Tuesday' , 'School_Session_Time_AM_Tuesday_seconds' , 'Tuesday_AM_dropoff_time_window_start' , 'Tuesday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Wednesday' , 'School_Session_Time_AM_Wednesday_seconds' , 'Wednesday_AM_dropoff_time_window_start' , 'Wednesday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Thursday' , 'School_Session_Time_AM_Thursday_seconds' , 'Thursday_AM_dropoff_time_window_start' , 'Thursday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Friday' , 'School_Session_Time_AM_Friday_seconds' , 'Friday_AM_dropoff_time_window_start' , 'Friday_AM_dropoff_time_window_end' ,
                                'School_Session_Time_PM_Monday' , 'School_Session_Time_PM_Monday_seconds' , 'Monday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Tuesday' , 'School_Session_Time_PM_Tuesday_seconds' , 'Tuesday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Wednesday' , 'School_Session_Time_PM_Wednesday_seconds' , 'Wednesday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Thursday' , 'School_Session_Time_PM_Thursday_seconds' , 'Thursday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Friday' , 'School_Session_Time_PM_Friday_seconds' , 'Friday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_Exemption',
                                'AM_Route_num' , 'AM_Sequence_num' , 'AM_bus_type' , 'AM_bus_type_new' , 'AM_Vehicle_Type_Designation' , 'AM_Seated_Passenger_Capacity' , 'AM_Wheelchair_Capacity' , 'AM_Ambulance_Capacity' , 'AM_Attendant' , 'AM_Lift_Capable' , 'AM_Vendor_Id' , 'AM_Vendor_name' , 
                                'PM_Route_num' , 'PM_Sequence_num' , 'PM_bus_type' , 'PM_bus_type_new' , 'PM_Vehicle_Type_Designation' , 'PM_Seated_Passenger_Capacity' , 'PM_Wheelchair_Capacity' , 'PM_Ambulance_Capacity' , 'PM_Attendant' , 'PM_Lift_Capable' , 'PM_Vendor_Id' , 'PM_Vendor_name' , 
                                'ALT_PM_route' , 'ALT_PM_sequence' , 'ALT_PM_bus_type' , 'ALT_PM_bus_type_new' , 'ALT_PM_Vehicle_Type_Designation' , 'ALT_PM_Seated_Passenger_Capacity' , 'ALT_PM_Wheelchair_Capacity' , 'ALT_PM_Ambulance_Capacity' , 'ALT_PM_Attendant' , 'ALT_PM_Lift_Capable' ,'ALT_PM_Vendor_Id' , 'ALT_PM_Vendor_name' , 
                                'After_4_route' , 'After_4_Sequence_num' , 'After_4_bus_type' , 'After_4_bus_type_new' , 'After_4_Vehicle_Type_Designation' , 'After_4_Seated_Passenger_Capacity' , 'After_4_Wheelchair_Capacity' , 'After_4_Ambulance_Capacity' , 'After_4_Attendant' , 'After_4_Lift_Capable' , 'After_4_Vendor_Id' , 'After_4_Vendor_name' , 
                                'AM_Garage_name' , 'AM_Garage_address' , 'AM_Garage_city' , 'AM_Garage_state' , 'AM_Garage_zip' , 'AM_Garage_XCOORD' , 'AM_Garage_YCOORD' ,
                                'PM_Garage_name' , 'PM_Garage_address' , 'PM_Garage_city' , 'PM_Garage_state' , 'PM_Garage_zip' , 'PM_Garage_XCOORD' , 'PM_Garage_YCOORD' ,
                                'ALT_PM_Garage_name' , 'ALT_PM_AM_Garage_address' , 'ALT_PM_Garage_city' , 'ALT_PM_Garage_state' , 'ALT_PM_Garage_zip' , 'ALT_PM_Garage_XCOORD' , 'ALT_PM_Garage_YCOORD' ,
                                'After_4_Garage_name' , 'After_4_Garage_address' , 'After_4_Garage_city' , 'After_4_Garage_state' , 'After_4_Garage_zip' , 'After_4_Garage_XCOORD' , 'After_4_Garage_YCOORD' ,
                                'Medical_Alert_code' , 'Description_of_Medical_Alert_code' , 'Ambulatory_code' , 'Description_of_Ambulatory_code' , 
                                'Wheelchair' , 'Wheelchair_wPara' , 'Limited_travel_time' , 'A/C_demand' , 'Mini_wagon' , '3_seats' , '2_seats' , 'Para' , 'Lift' , 
                                'Ambulatory_demand' , 'Wheelchair_demand' , 'Seated_Demand' , 'Outside_Borough_Travel' , 
                                'Total_load_unload_time' , 'Maximum_Ride_Time']]

print "Reording columns done!"

# This till save the final dataframe into a csv for the master dataset for student stops and drop the last row which is an empty row.

print "Saving... Do not turn off the power..."

#dfresult17.drop(dfresult17.tail(1).index,inplace=True)
dfresult17.to_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\StudentD31092117.csv', sep=',')

print "Save complete!"

# This will create a new dataframe to store the AM Stops only.

print "Separating the AM Stops  only into their own dataframe..."

dfresultam = dfresult17[['Service_type' , 'OSIS_ID' , 'Age_group' , 'City' , 'Home_boro' , 'Zip_code' , 'Home_district' , 'Address_XCOORD' , 'Address_YCOORD' ,
                                'AM_Stop_location' , 'AM_GE_Stop_number' , 'AM_Stop_XCOORD' , 'AM_Stop_YCOORD' , 'AM_Stop_time' , 'AM_Stop_time_seconds' ,
                                'School_code' , 'School_name' , 'School_address' , 'School_city' , 'School_state' , 'School_zip_code' , 'School_boro' , 'School_district' , 'School_XCOORD' , 'School_YCOORD' , 'School_Affiliation_type' , 
                                'School_Session_Time_AM_Monday' , 'School_Session_Time_AM_Monday_seconds' , 'Monday_AM_dropoff_time_window_start' , 'Monday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Tuesday' , 'School_Session_Time_AM_Tuesday_seconds' , 'Tuesday_AM_dropoff_time_window_start' , 'Tuesday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Wednesday' , 'School_Session_Time_AM_Wednesday_seconds' , 'Wednesday_AM_dropoff_time_window_start' , 'Wednesday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Thursday' , 'School_Session_Time_AM_Thursday_seconds' , 'Thursday_AM_dropoff_time_window_start' , 'Thursday_AM_dropoff_time_window_end' , 'School_Session_Time_AM_Friday' , 'School_Session_Time_AM_Friday_seconds' , 'Friday_AM_dropoff_time_window_start' , 'Friday_AM_dropoff_time_window_end' ,
                                'AM_Route_num' , 'AM_Sequence_num' , 'AM_bus_type' , 'AM_bus_type_new' , 'AM_Vehicle_Type_Designation' , 'AM_Seated_Passenger_Capacity' , 'AM_Wheelchair_Capacity' , 'AM_Ambulance_Capacity' , 'AM_Attendant' , 'AM_Lift_Capable' , 'AM_Vendor_Id' , 'AM_Vendor_name' , 
                                'AM_Garage_name' , 'AM_Garage_address' , 'AM_Garage_city' , 'AM_Garage_state' , 'AM_Garage_zip' , 'AM_Garage_XCOORD' , 'AM_Garage_YCOORD' ,
                                'Medical_Alert_code' , 'Description_of_Medical_Alert_code' , 'Ambulatory_code' , 'Description_of_Ambulatory_code' , 
                                'Wheelchair' , 'Wheelchair_wPara' , 'Limited_travel_time' , 'A/C_demand' , 'Mini_wagon' , '3_seats' , '2_seats' , 'Para' , 'Lift' , 
                                'Ambulatory_demand' , 'Wheelchair_demand' , 'Seated_Demand' , 'Outside_Borough_Travel' , 
                                'Total_load_unload_time' , 'Maximum_Ride_Time']]

print "Separation complete!"

# This till save the AM Stops only from final dataframe into a csv for the master dataset for student stops.

dfresultam.to_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\StudentD31092117am.csv', sep=',')

# This will create a new dataframe to store the PM Stops only.

print "Separating the AM Stops  only into their own dataframe..."

dfresultpm = dfresult17[['Service_type' , 'OSIS_ID' , 'Age_group' , 'City' , 'Home_boro' , 'Zip_code' , 'Home_district' , 'Address_XCOORD' , 'Address_YCOORD' ,
                                'PM_Stop_location' , 'PM_GE_Stop_number' , 'PM_Stop_XCOORD' , 'PM_Stop_YCOORD' , 'PM_Stop_time' , 'PM_Stop_time_seconds' , 
                                'School_code' , 'School_name' , 'School_address' , 'School_city' , 'School_state' , 'School_zip_code' , 'School_boro' , 'School_district' , 'School_XCOORD' , 'School_YCOORD' , 'School_Affiliation_type' , 
                                'School_Session_Time_PM_Monday' , 'School_Session_Time_PM_Monday_seconds' , 'Monday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Tuesday' , 'School_Session_Time_PM_Tuesday_seconds' , 'Tuesday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Wednesday' , 'School_Session_Time_PM_Wednesday_seconds' , 'Wednesday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Thursday' , 'School_Session_Time_PM_Thursday_seconds' , 'Thursday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_PM_Friday' , 'School_Session_Time_PM_Friday_seconds' , 'Friday_PM_dropoff_PM_Pickup_window_end' , 'School_Session_Time_Exemption',
                                'PM_Route_num' , 'PM_Sequence_num' , 'PM_bus_type' , 'PM_bus_type_new' , 'PM_Vehicle_Type_Designation' , 'PM_Seated_Passenger_Capacity' , 'PM_Wheelchair_Capacity' , 'PM_Ambulance_Capacity' , 'PM_Attendant' , 'PM_Lift_Capable' , 'PM_Vendor_Id' , 'PM_Vendor_name' , 
                                'PM_Garage_name' , 'PM_Garage_address' , 'PM_Garage_city' , 'PM_Garage_state' , 'PM_Garage_zip' , 'PM_Garage_XCOORD' , 'PM_Garage_YCOORD' ,
                                'Medical_Alert_code' , 'Description_of_Medical_Alert_code' , 'Ambulatory_code' , 'Description_of_Ambulatory_code' , 
                                'Wheelchair' , 'Wheelchair_wPara' , 'Limited_travel_time' , 'A/C_demand' , 'Mini_wagon' , '3_seats' , '2_seats' , 'Para' , 'Lift' , 
                                'Ambulatory_demand' , 'Wheelchair_demand' , 'Seated_Demand' , 'Outside_Borough_Travel' , 
                                'Total_load_unload_time' , 'Maximum_Ride_Time']]

print "Separation complete!"

print "Now removing students who are not assigned to any routes..."

dfresultpm = dfresultpm.dropna(subset=['PM_Route_num']).reset_index(drop=True)

print "Removal complete!"

# This till save the AM Stops only from final dataframe into a csv for the master dataset for student stops.

dfresultpm.to_csv(r'Z:\GIS\Development\VRP\Data\DataPrep\test1\StudentD31092117pm.csv', sep=',')

print "done!"

# print dfresult17.dtypes



