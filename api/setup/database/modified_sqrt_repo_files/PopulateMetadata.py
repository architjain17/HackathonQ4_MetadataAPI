#!/mnt/env/sqrt_python27/bin/python
import os
import sys
import csv
import json
import collections
import datetime
import time
import argparse
import re


# Add Square-Root modules library path
sys.path.append(os.path.normpath(os.environ['RIGHTIMPORT']+'/sqrt_lib/'))
from sq_file import ImportFile
from sq_sql import DBClient

parser = argparse.ArgumentParser(description='Generate Metadata for different destinations, NNA, NCI, MAPR (in NNA), VPIPE (in NNA)')
parser.add_argument('-o',  metavar='Metadata for point data refresh (MAPR, VPIPE, CAT, MI50, PLSS, SGP)', required=False, help='use MAPR or VPIPE or CAT or MI50 or PLSS or SGP, if the metadata is for MAPR or VPIPE or CAT or MI50 or PLSS or SGP point data refresh')
parser.add_argument('-d', metavar='Database', required=False, default='Util', help='Database name representing customer\'s AutoUpdate job. Eg: CBAAutoUpdate')
args = parser.parse_args()

print "args.o="
print args.o
database = args.d

# Constants
metadata_files_base_path = os.environ['RIGHTIMPORT'] + '/ETL Documentation and Scheduling/Metadata/'
start_time = time.time()


def read_csv_into_list_of_ordered_dicts(csv_file):

    data_list = []

    with open(csv_file, 'rb') as f:
        r = csv.reader(f)

        headers = None
        for row in r:
            if not headers:
                headers = row
            else:
                d = collections.OrderedDict()
                for header, value in zip(headers, row):
                    if header.lower() in ['id', 'precision', 'shift', 'active', 'dwdestinationid', 'dwdatasourcefileid_primary', 'datapointid', 'metric1', 'metric2', 'goal_datapointid', 'dwdatasourceid', 'updatefrequencywarndays', 'etl-dev_only', 'intra-oceanus only', 'hasdatapointmetadata', 'datapointgoalid', 'refonlydatapointid', 'refonlyisnsightenabled', 'refonly_fileidprimary', 'dwdatadestinationfileid', 'datapointcategoryid', 'refonly_relative_rank_desc', 'refonly_relation_count', 'refonly_duplicateexists', 'refonly_inverseexists', 'refonly_occurrence'] and value not in ('NA'):
                        d[header] = int(value)
                    elif header.lower() in ['spearman_coeff', 'fair_threshold_percentage'] and value:
                        d[header] = float(value)
                    else:
                        d[header] = value

                data_list.append(d)

    return data_list


print "\n===== Starting PopulateMetadata.py at " + str(datetime.datetime.now())

# acquire advisory lock to serialize metadata activity
acquire_advisory_lock = """
select pg_advisory_lock(convert_lock_handle('{0}_metadata'));
""".format(database)

# set up schemata
schema_setup_sql = """

CREATE SCHEMA IF NOT EXISTS Util_tmp;

CREATE SCHEMA IF NOT EXISTS Util_raw;

CREATE SCHEMA IF NOT EXISTS Util_clean;

CREATE SCHEMA IF NOT EXISTS Util_deliver;
"""

drop_contraints_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointRelatedMetrics;

DROP TABLE IF EXISTS Util_raw.DWDataPointCategoryMember;

DROP TABLE IF EXISTS Util_raw.DWDataPointCategory;

DROP TABLE IF EXISTS Util_raw.DWDataPointDestination;

DROP TABLE IF EXISTS Util_raw.DWDataPointComputePeriod;

DROP TABLE IF EXISTS Util_raw.DWDataPointGoals;

DROP TABLE IF EXISTS Util_raw.DWDataPointGoalNames;

DROP TABLE IF EXISTS Util_raw.DWDataPoint;

DROP TABLE IF EXISTS Util_raw.DWDataDestinationFile;

DROP TABLE IF EXISTS Util_raw.DWDataDestination;

DROP TABLE IF EXISTS Util_raw.DWDataSourceFile;

DROP TABLE IF EXISTS Util_raw.DWDataSource;

"""


create_dwdatasource_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataSource;

CREATE TABLE Util_raw.DWDataSource(
    ID varchar(4000) NOT NULL PRIMARY KEY,
    Name varchar(4000) NOT NULL,
    Description varchar(4000) NOT NULL,
    Type varchar(4000) NOT NULL,
    Source varchar(4000) NOT NULL,
    Point_of_Contact varchar(4000) NOT NULL,
    Oceanus_Contact varchar(4000) NOT NULL,
    Theme varchar(4000) NOT NULL,
    Active varchar(4000) NOT NULL,
    Status_Notes varchar(4000) NOT NULL,
    SourceUpdateFreq varchar(4000) NOT NULL,
    Data_Transfer_Method varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    Modified_Date varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL
);

"""

create_dwdatasourcefile_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataSourceFile;

CREATE TABLE Util_raw.DWDataSourceFile(
    DWDataSourceID varchar(4000) NOT NULL,
    ID varchar(4000) NOT NULL PRIMARY KEY,
    FileNameSearchString varchar(4000) NOT NULL,
    Description varchar(4000) NOT NULL,
    Active varchar(4000) NOT NULL,
    etl_dev_only varchar(4000) NOT NULL,
    SourceUpdateFreq varchar(4000) NOT NULL,
    UpdateFrequencyWarnDays varchar(4000) NOT NULL,
    TargetDB varchar(4000) NOT NULL,
    Raw_Table_Name varchar(4000) NULL,
    Delimiter   varchar(4000) NULL,
    Data_Sniffing_Enabled varchar(1) NULL, 
    Comments varchar(4000) NULL,
    Modified_Date varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL,
    FOREIGN KEY (DWDataSourceID) REFERENCES Util_raw.DWDataSource(ID)
);

"""

create_dwdatadestination_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataDestination;

CREATE TABLE Util_raw.DWDataDestination(
    ID varchar(4000) NOT NULL PRIMARY KEY,
    Name varchar(4000) NOT NULL,
    Active varchar(4000) NOT NULL,
    Intra_Oceanus_Only varchar(4000) NOT NULL,
    Comment varchar(4000) NULL,
    Modified_Date varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL
);
"""

# hasDataPointMetadata column in Util_raw.DWDataDestinationFile specifies for a report whether it is supported by metadata (as well as metadata cross checks) and which are not
# Only a handful of the files in our output streams (dealer_data_points_wRollups, for example) have data descriptions and related metrics in Metadata.
# The rest, such as repair order transactions for other teams (Maritz) are simple flat file dumps with no metadata

create_dwdatadestinationfile_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataDestinationFile;

CREATE TABLE Util_raw.DWDataDestinationFile(
    DWDestinationID varchar(4000) NOT NULL,
    ID varchar(4000) NOT NULL PRIMARY KEY,
    Name varchar(4000) NOT NULL,
    Active varchar(4000) NOT NULL,
    hasDataPointMetadata varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    Modified_Date varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL,
    FOREIGN KEY (DWDestinationID) REFERENCES Util_raw.DWDataDestination(ID)
);

"""

create_dwdatapoint_sql = """

DROP TABLE IF EXISTS Util_raw.DWDataPoint;

CREATE TABLE Util_raw.DWDataPoint(
    DWDataSourceFileID_Primary varchar(4000) NOT NULL,
    RefOnly_DWDataSourceFile varchar(4000) NOT NULL,
    ID varchar(4000) NOT NULL PRIMARY KEY,
    data_point_code varchar(4000) NOT NULL,
    data_type varchar(4000) NOT NULL,
    unit varchar(4000) NOT NULL,
    human_name varchar(4000) NOT NULL,
    precision varchar(4000) NOT NULL,
    shift varchar(4000) NOT NULL,
    Description varchar(4000) NOT NULL,
    Good_High_Low_NoColor varchar(4000) NOT NULL,
    fair_threshold_percentage varchar(4000),
    RollupCalculationType varchar(4000) NOT NULL,
    Active varchar(4000) NOT NULL,
    view_by_default varchar(4000) NULL,
    Comments varchar(4000) NULL,
    Modified_By varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL,
    RefOnlyDataPointID varchar(4000) NOT NULL,
    RefOnlyIsnSightEnabled varchar(4000) NOT NULL,
    RefOnly_FileIDPrimary varchar(4000) NOT NULL,
    owner varchar(4000) NOT NULL,
    FOREIGN KEY (DWDataSourceFileID_Primary) REFERENCES Util_raw.DWDataSourceFile(ID)
);

"""

create_dwdatapointgoalnames_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointGoalNames;

CREATE TABLE Util_raw.DWDataPointGoalNames(
    ID varchar(4000) NOT NULL PRIMARY KEY,
    Name varchar(4000) NOT NULL,
    Description varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    Modified_By varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL
);
"""

create_dwdatapointgoals_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointGoals;

CREATE TABLE Util_raw.DWDataPointGoals(
    DataPointID varchar(4000) NOT NULL,
    data_point_code varchar(4000) NOT NULL,
    DataPointGoalID varchar(4000) NOT NULL,
    DataPointGoalName varchar(4000) NOT NULL,
    goal_DataPointID varchar(4000) NOT NULL,
    goal_data_point_code varchar(4000) NOT NULL,
    Comments varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL,
    PRIMARY KEY (DataPointID, DataPointGoalID, goal_DataPointID),
    FOREIGN KEY (DataPointID) REFERENCES Util_raw.DWDataPoint(ID),
    FOREIGN KEY (DataPointGoalID) REFERENCES Util_raw.DWDataPointGoalNames(ID),
    FOREIGN KEY (goal_DataPointID) REFERENCES Util_raw.DWDataPoint(ID)
);

"""

create_dwdatapointcomputeperiod_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointComputePeriod;

CREATE TABLE Util_raw.DWDataPointComputePeriod(
    DataPointID varchar(4000) NOT NULL,
    data_point_code varchar(4000) NOT NULL,
    Computeperiod varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    Modified_By varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL,
    PRIMARY KEY (DataPointID, Computeperiod),
    FOREIGN KEY (DataPointID) REFERENCES Util_raw.DWDataPoint(ID)
);

"""

create_dwdatapointdestination_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointDestination;

CREATE TABLE Util_raw.DWDataPointDestination(
    DataPointID varchar(4000) NOT NULL,
    data_point_code varchar(4000) NOT NULL,
    DWDataDestinationFileID varchar(4000) NOT NULL,
    DWDataDestinationFileName varchar(4000) NOT NULL,
    DWDestinationID varchar(4000) NOT NULL,
    DWDestination varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    Modified_By varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL,
    PRIMARY KEY (DataPointID, DWDataDestinationFileID),
    FOREIGN KEY (DWDataDestinationFileID) REFERENCES Util_raw.DWDataDestinationFile(ID),
    FOREIGN KEY (DWDestinationID) REFERENCES Util_raw.DWDataDestination(ID)
);

"""

create_dwdatapointcategory_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointCategory;

CREATE TABLE Util_raw.DWDataPointCategory(
    ID varchar(4000) NOT NULL PRIMARY KEY,
    Name varchar(4000) NOT NULL,
    Description varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    Modified_Date varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL
);

"""

create_dwdatapointcategorymember_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointCategoryMember;

CREATE TABLE Util_raw.DWDataPointCategoryMember(
    DataPointID varchar(4000) NOT NULL,
    RefOnly_DataPointCode varchar(4000) NOT NULL,
    DataPointCategoryID varchar(4000) NOT NULL,
    RefOnly_DataPointCategoryName varchar(4000) NOT NULL,
    Comments varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL,
    PRIMARY KEY (DataPointID, DataPointCategoryID),
    FOREIGN KEY (DataPointID) REFERENCES Util_raw.DWDataPoint(ID),
    FOREIGN KEY (DataPointCategoryID) REFERENCES Util_raw.DWDataPointCategory(ID)
);

"""

create_dwdatapointrelatedmetrics_sql = """
DROP TABLE IF EXISTS Util_raw.DWDataPointRelatedMetrics;

CREATE TABLE Util_raw.DWDataPointRelatedMetrics(
    Metric1 varchar(4000) NOT NULL,
    RefOnly_DataPointID1 varchar(4000) NOT NULL,
    Metric2 varchar(4000) NOT NULL,
    RefOnly_DataPointID2 varchar(4000) NOT NULL,
    Spearman_Coeff varchar(4000) NOT NULL,
    Modified_By varchar(4000) NOT NULL,
    Modified_Date varchar(4000) NOT NULL,
    Comments varchar(4000) NULL,
    RefOnly_Relative_Rank_Desc varchar(4000) NOT NULL,
    RefOnly_Relation_Count varchar(4000) NOT NULL,
    RefOnly_Keys varchar(4000) NOT NULL,
    RefOnly_DuplicateExists varchar(4000) NOT NULL,
    RefOnly_InverseKeys varchar(4000) NOT NULL,
    RefOnly_InverseExists varchar(4000) NOT NULL,
    RefOnly_Occurrence varchar(4000) NOT NULL,
    PRIMARY KEY (Metric1, Metric2),
    FOREIGN KEY (Metric1) REFERENCES Util_raw.DWDataPoint(ID),
    FOREIGN KEY (Metric2) REFERENCES Util_raw.DWDataPoint(ID)
);
"""
# release advisory lock used to serialize metadata activity
release_advisory_lock = """
select pg_advisory_unlock(convert_lock_handle('{0}_metadata'));
""".format(database)

########## get SourceUpdateDate from Util_dbo.TableETLLog ##########
getSourceUpdateDate_sql = """
 with l as (select l.FileOrTableName_From,
                    l.ObjectTimeStamp_From - cast(now() as date) days_since_extract,
                    l.ObjectTimeStamp_From,
                    l.LoadDate,
                    l.PathOrSchema_From,
                    row_number() over (partition by l.FileOrTableName_From, l.LoadDate order by l.ObjectTimeStamp_From desc) as is_most_recent_objecttimestamp,
                    row_number() over (partition by l.FileOrTableName_From order by l.LoadDate desc) as is_most_recent_loaddate
               from {0}_dbo.TableETLLog l),
       l2 as (select *
                from l
               where l.is_most_recent_objecttimestamp = 1
                 and l.is_most_recent_loaddate = 1)
 select s.ID, s.FileNameSearchString, replace(to_char(f.ObjectTimeStamp_From, 'YYYYMMDD HH24:MI:SS'), '-', '') SourceUpdateDate
      from Util_raw.DWDataSourceFile s
 left join (select * from
    (
       select
              a.FileNameSearchString
            , l.FileOrTableName_From
            , a.UpdateFrequencyWarnDays
            , l.ObjectTimeStamp_From - cast(now() as date) days_since_extract
            , l.ObjectTimeStamp_From
            , l.LoadDate
            , case when cast(l.LoadDate as date) = MAX(cast(l.LoadDate as date)) over (partition by a.FileNameSearchString) then 1 else null end is_most_recent_loaddate
            --, case when l.ObjectTimeStamp_From = MAX(l.ObjectTimeStamp_From) over (partition by a.FileNameSearchString, cast(l.LoadDate as date)) then 1 else null end is_most_recent_objecttimestamp
            , ROW_NUMBER() OVER (PARTITION BY a.FileNameSearchString, cast(l.LoadDate as date) ORDER BY l.ObjectTimeStamp_From desc) is_most_recent_objecttimestamp
            , a.Description
            , l.PathOrSchema_From
         from Util_raw.DWDataSourceFile a
    left join l2 as l
           on l.FileOrTableName_From ~ a.FileNameSearchString
    ) f0
                    where f0.is_most_recent_objecttimestamp = 1
                      and f0.is_most_recent_loaddate = 1) f
      on s.FileNameSearchString = f.FileNameSearchString;
""".format(database)

########## load data into Util database ##########
print "\nLoading data into Util database ..."

with DBClient(dbName='postgres', dbSchema='postgres', dbServer='localhost',
                 dbUser='arjain', dbPwd='password', dbFlavor='postgres') as client:
    print "\nAcquiring advisory lock to serialize metadata activity (lock handle = {0}_metadata) ...".format(database)
    print "\nAdvisory lock acquired ..."
    client.RunQuery(schema_setup_sql)
    client.RunQuery(drop_contraints_sql)

    client.RunQuery(create_dwdatasource_sql)
    client.RunQuery(create_dwdatasourcefile_sql)
    client.RunQuery(create_dwdatadestination_sql)
    client.RunQuery(create_dwdatadestinationfile_sql)
    client.RunQuery(create_dwdatapoint_sql)
    client.RunQuery(create_dwdatapointgoalnames_sql)
    client.RunQuery(create_dwdatapointgoals_sql)
    client.RunQuery(create_dwdatapointcomputeperiod_sql)
    client.RunQuery(create_dwdatapointdestination_sql)
    client.RunQuery(create_dwdatapointcategory_sql)
    client.RunQuery(create_dwdatapointcategorymember_sql)
    client.RunQuery(create_dwdatapointrelatedmetrics_sql)

    print 'Import DWDataSource.csv to Util_raw.DWDataSource'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataSource.csv'], db_schema='Util_raw', db_table='DWDataSource', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataSourceFile.csv to Util_raw.DWDataSourceFile'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataSourceFile.csv'], db_schema='Util_raw', db_table='DWDataSourceFile', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataDestination.csv to Util_raw.DWDataDestination'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataDestination.csv'], db_schema='Util_raw', db_table='DWDataDestination', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataDestinationFile.csv to Util_raw.DWDataDestinationFile'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataDestinationFile.csv'], db_schema='Util_raw', db_table='DWDataDestinationFile', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPoint.csv to Util_raw.DWDataPoint'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPoint.csv'], db_schema='Util_raw', db_table='DWDataPoint', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointGoalNames.csv to Util_raw.DWDataPointGoalNames'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointGoalNames.csv'], db_schema='Util_raw', db_table='DWDataPointGoalNames', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointGoals.csv to Util_raw.DWDataPointGoals'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointGoals.csv'], db_schema='Util_raw', db_table='DWDataPointGoals', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointComputePeriod.csv to Util_raw.DWDataPointComputePeriod'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointComputePeriod.csv'], db_schema='Util_raw', db_table='DWDataPointComputePeriod', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointDestination.csv to Util_raw.DWDataPointDestination'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointDestination.csv'], db_schema='Util_raw', db_table='DWDataPointDestination', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointCategory.csv to Util_raw.DWDataPointCategory'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointCategory.csv'], db_schema='Util_raw', db_table='DWDataPointCategory', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointCategoryMember.csv to Util_raw.DWDataPointCategoryMember'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointCategoryMember.csv'], db_schema='Util_raw', db_table='DWDataPointCategoryMember', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print 'Import DWDataPointRelatedMetrics.csv to Util_raw.DWDataPointRelatedMetrics'
    import_file_object = ImportFile([metadata_files_base_path + 'DWDataPointRelatedMetrics.csv'], db_schema='Util_raw', db_table='DWDataPointRelatedMetrics', db_insert_mode='append', db_name='postgres', db_server='localhost', db_user='arjain', db_pwd='password', db_flavor='postgres')
    import_file_object.start_import()

    print "Done loading data into Util database.\n"

    print "Getting SourceUpdateDate from {0}_dbo.TableETLLog ...".format(database)
    print "Done getting SourceUpdateDate.\n"

    print "\nAdvisory lock released ..."
exit(0)
########## reading data from CSV into List of Ordered Dicts ##########

DataSource_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataSource.csv')
DataSourceFile_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataSourceFile.csv')
DataDestination_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataDestination.csv')
DataDestinationFile_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataDestinationFile.csv')
DataPoint_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPoint.csv')
DWDataPointGoalNames_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointGoalNames.csv')
DWDataPointGoals_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointGoals.csv')
DataPointComputePeriod_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointComputePeriod.csv')
DataPointDestination_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointDestination.csv')
DataPointCategory_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointCategory.csv')
DataPointCategoryMember_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointCategoryMember.csv')
DataPointRelatedMetrics_list = read_csv_into_list_of_ordered_dicts(metadata_files_base_path + 'DWDataPointRelatedMetrics.csv')


########## populate JSON feed for the app ##########

print "Populating JSON feed ..."

SPAAutoUpdate_DataPoint_list = [] # Destination: 2, Nissan D360 - US
NCIAutoUpdate_DataPoint_list = [] # Destination: 3, Nissan D360 - Canada
NMACAutoUpdate_DataPoint_list = [] # Destination: 8, Nissan D360 - NMAC
VWAutoUpdate_DataPoint_list = [] # Destination: 21, Nissan D360 - VW
CBAAutoUpdate_DataPoint_list = [] # Destination: 22, Nissan D360 - CBA
Dogfood_DataPoint_list = [] # Dest 9: Dogfood

MAPRCategoryMember_list = []
CATCategoryMember_list = []
VPIPECategoryMember_list = []
MI50CategoryMember_list = []
PLSSCategoryMember_list = []
SGPCategoryMember_list = []

# populate 'DWDataPoint' with 'DWDataPointComputePeriod', 'DWDataPointCategoryMember', 'DWDataPointGoals', 'DWDataSource', 'DWDataSourceFile', 'DWDataPointRelatedMetrics', 'DWDataDestinationFile'
for d in range(0, len(DataPoint_list)):
    if DataPoint_list[d]['Active'] == 0:
        continue

    # Add DWDataPointComputePeriod
    p = []
    if DataPoint_list[d]['data_type'] != 'date' and DataPoint_list[d]['data_type'] != 'varchar':
        has_computeperiod = False
        for i in range(0, len(DataPointComputePeriod_list)):
            if DataPointComputePeriod_list[i]['DataPointID'] == DataPoint_list[d]['id']:
                has_computeperiod = True
                p.append(DataPointComputePeriod_list[i]['Computeperiod'])
        if has_computeperiod == False:
            print "\nAssertion 002 -- Data point with ID " + str(DataPoint_list[d]['id']) + " doesn't have a Computeperiod in DWDataPointComputePeriod!!!"
            end_time = time.time()
            print "\nPopulateMetadata.py terminated at " + str(datetime.datetime.now())
            print "\nThe run-time is: " + str(end_time - start_time) + " seconds"
            raise Exception

    # Add DWDataPointCategoryMember
    m = []
    for i in range(0, len(DataPointCategoryMember_list)):
        if DataPointCategoryMember_list[i]['DataPointID'] == DataPoint_list[d]['id']:
            for j in range(0, len(DataPointCategory_list)):
                if DataPointCategory_list[j]['Id'] == DataPointCategoryMember_list[i]['DataPointCategoryID']:
                    m.append(DataPointCategory_list[j]['Name'])
                    break

    # Add DWDataPointGoals
    g = {}
    for i in range(0, len(DWDataPointGoals_list)):
        if DWDataPointGoals_list[i]['DataPointID'] == DataPoint_list[d]['id']:
            goalName = DWDataPointGoals_list[i]['DataPointGoalName']
            g[goalName] = DWDataPointGoals_list[i]['goal_DataPointID']
            #print g
    gg = []
    gg.append(g)

    # Add Source and SourceUpdateFreq from DWDataSourceFile
    # s = []
    for i in range(0, len(DataSourceFile_list)):
        if DataSourceFile_list[i]['ID'] == DataPoint_list[d]['DWDataSourceFileID_Primary']:
            for j in range(0, len(DataSource_list)):
                if DataSource_list[j]['ID'] == DataSourceFile_list[i]['DWDataSourceID']:
                    #s.append(DWDataSource_list[j]['Source'])
                    DataPoint_list[d]['Source'] = DataSource_list[j]['Source']
                    DataPoint_list[d]['SourceUpdateFreq'] = DataSource_list[j]['SourceUpdateFreq']
                    break

    # Add SourceUpdateDate from SourceUpdateDate_list
    for i in range(0, len(SourceUpdateDate_list)):
        if int(SourceUpdateDate_list[i][0]) == DataPoint_list[d]['DWDataSourceFileID_Primary']:
            DataPoint_list[d]['SourceUpdateDate'] = SourceUpdateDate_list[i][2]
            break

    # Add DWDataPointRelatedMetrics
    r = []
    for i in range(0, len(DataPointRelatedMetrics_list)):
        if DataPointRelatedMetrics_list[i]['Metric1'] == DataPoint_list[d]['id']:
            r.append(DataPointRelatedMetrics_list[i]['Metric2'])

    DataPoint_list[d]['ComputePeriods'] = p
    DataPoint_list[d]['CategoryNames'] = m
    DataPoint_list[d]['Goals'] = gg
    DataPoint_list[d]['RelatedMetrics'] = r
    #DataPoint_list[d]['DataFileName'] = f # no need to a list; only one value is used

    # delete mappings the app won't use
    del DataPoint_list[d]['DWDataSourceFileID_Primary'] # fk
    del DataPoint_list[d]['RefOnly_DWDataSourceFile']
    del DataPoint_list[d]['RefOnlyDataPointID']
    del DataPoint_list[d]['RefOnlyIsnSightEnabled']
    del DataPoint_list[d]['RefOnly_FileIDPrimary']


    # Add DataFileName and Destination from DWDataPointDestination
    has_destination = False
    for i in range(0, len(DataPointDestination_list)):
        if DataPointDestination_list[i]['DataPointID'] == DataPoint_list[d]['id']:
            has_destination = True
            DataPoint_list[d]['DataFileName'] = DataPointDestination_list[i]['DWDataDestinationFileName']
            if int(DataPointDestination_list[i]['DWDestinationID']) == 2:
                SPAAutoUpdate_DataPoint_list.append(DataPoint_list[d])
            if int(DataPointDestination_list[i]['DWDestinationID']) == 3:
                NCIAutoUpdate_DataPoint_list.append(DataPoint_list[d])
            if int(DataPointDestination_list[i]['DWDestinationID']) == 8:
                NMACAutoUpdate_DataPoint_list.append(DataPoint_list[d])
            if int(DataPointDestination_list[i]['DWDestinationID']) == 9:
                Dogfood_DataPoint_list.append(DataPoint_list[d])
            if int(DataPointDestination_list[i]['DWDestinationID']) == 21:
                VWAutoUpdate_DataPoint_list.append(DataPoint_list[d])
            if int(DataPointDestination_list[i]['DWDestinationID']) == 22:
                CBAAutoUpdate_DataPoint_list.append(DataPoint_list[d])

    if has_destination == False:
        print "\nAssertion 003 -- Data point with ID " + str(DataPoint_list[d]['id']) + " doesn't have a destination in DWDataPointDestination!!!"
        end_time = time.time()
        print "\nPopulateMetadata.py terminated at " + str(datetime.datetime.now())
        print "\nThe run-time is: " + str(end_time - start_time) + " seconds"
        raise Exception


if args.o == 'MAPR':
    MAPR_DataPoint_list = [] # Destination: 2, Nissan D360 - US
    for d in range(0, len(SPAAutoUpdate_DataPoint_list)):
        for ma in range(0, len(MAPRCategoryMember_list)):
            if MAPRCategoryMember_list[ma]['DataPointID'] == SPAAutoUpdate_DataPoint_list[d]['id']:
                MAPR_DataPoint_list.append(SPAAutoUpdate_DataPoint_list[d])
                break

    j = json.dumps(MAPR_DataPoint_list, indent=4)
    f = open('Metadata_MAPR.json','w+')
    print >> f, j
    f.close()

if args.o == 'CAT':
    CAT_DataPoint_list = [] # Destination: 2, Nissan D360 - US
    for d in range(0, len(SPAAutoUpdate_DataPoint_list)):
        for ma in range(0, len(CATCategoryMember_list)):
            if CATCategoryMember_list[ma]['DataPointID'] == SPAAutoUpdate_DataPoint_list[d]['id']:
                CAT_DataPoint_list.append(SPAAutoUpdate_DataPoint_list[d])
                break

    j = json.dumps(CAT_DataPoint_list, indent=4)
    f = open('Metadata_CAT.json','w+')
    print >> f, j
    f.close()

if args.o == 'VPIPE':
    VPIPE_DataPoint_list = [] # Destination: 2, Nissan D360 - US
    for d in range(0, len(SPAAutoUpdate_DataPoint_list)):
        for ma in range(0, len(VPIPECategoryMember_list)):
            if VPIPECategoryMember_list[ma]['DataPointID'] == SPAAutoUpdate_DataPoint_list[d]['id']:
                VPIPE_DataPoint_list.append(SPAAutoUpdate_DataPoint_list[d])
                break

    j = json.dumps(VPIPE_DataPoint_list, indent=4)
    f = open('Metadata_VPIPE.json','w+')
    print >> f, j
    f.close()

if args.o == 'MI50':
    MI50_DataPoint_list = [] # Destination: 2, Nissan D360 - US
    for d in range(0, len(SPAAutoUpdate_DataPoint_list)):
        for ma in range(0, len(MI50CategoryMember_list)):
            if MI50CategoryMember_list[ma]['DataPointID'] == SPAAutoUpdate_DataPoint_list[d]['id']:
                MI50_DataPoint_list.append(SPAAutoUpdate_DataPoint_list[d])
                break

    j = json.dumps(MI50_DataPoint_list, indent=4)
    f = open('Metadata_MI50.json','w+')
    print >> f, j
    f.close()

if args.o == 'PLSS':
    PLSS_DataPoint_list = [] # Destination: 2, Nissan D360 - US
    for d in range(0, len(SPAAutoUpdate_DataPoint_list)):
        for ma in range(0, len(PLSSCategoryMember_list)):
            if PLSSCategoryMember_list[ma]['DataPointID'] == SPAAutoUpdate_DataPoint_list[d]['id']:
                PLSS_DataPoint_list.append(SPAAutoUpdate_DataPoint_list[d])
                break

    j = json.dumps(PLSS_DataPoint_list, indent=4)
    f = open('Metadata_PLSS.json','w+')
    print >> f, j
    f.close()

if args.o == 'SGP':
    SGP_DataPoint_list = [] # Destination: 2, Nissan D360 - US
    for d in range(0, len(SPAAutoUpdate_DataPoint_list)):
        for ma in range(0, len(SGPCategoryMember_list)):
            if SGPCategoryMember_list[ma]['DataPointID'] == SPAAutoUpdate_DataPoint_list[d]['id']:
                SGP_DataPoint_list.append(SPAAutoUpdate_DataPoint_list[d])
                break

    j = json.dumps(SGP_DataPoint_list, indent=4)
    f = open('Metadata_SGP.json','w+')
    print >> f, j
    f.close()  

j = json.dumps(SPAAutoUpdate_DataPoint_list, indent=4)
f = open('Metadata_SPAAutoUpdate.json','w+')
print >> f, j
f.close()

j = json.dumps(NCIAutoUpdate_DataPoint_list, indent=4)
f = open('Metadata_NCIAutoUpdate.json','w+')
print >> f, j
f.close()

j = json.dumps(NMACAutoUpdate_DataPoint_list, indent=4)
f = open('Metadata_NMACAutoUpdate.json','w+')
print >> f, j
f.close()

j = json.dumps(VWAutoUpdate_DataPoint_list, indent=4)
f = open('Metadata_VWAutoUpdate.json','w+')
print >> f, j
f.close()

j = json.dumps(CBAAutoUpdate_DataPoint_list, indent=4)
f = open('Metadata_CBAAutoUpdate.json','w+')
print >> f, j
f.close()

## Note to everyone: This is the ONLY way this should be done.
with open('Metadata_Dogfood.json', 'w+') as f:
    json.dump(Dogfood_DataPoint_list, f, indent=4)

print "Done populating JSON feed."

end_time = time.time()

print "\n===== PopulateMetadata.py completed at " + str(datetime.datetime.now())

print "The run-time is: " + str(end_time - start_time) + " seconds\n"
