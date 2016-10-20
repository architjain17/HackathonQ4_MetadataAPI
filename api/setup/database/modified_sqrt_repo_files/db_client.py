#!/mnt/env/sqrt_python27/bin/python
# -*- coding: utf-8 -*-
"""
Database interface class intended to be used along with
Pandas dataframe in order to perform database operation.

@author: NPatelThinkT431s
"""
import os
import sys
import numpy
# import pyodbc # jun commented this out to work around pyodbc when importing DBClient from sq_sql, don't git add this!
import pandas
import datetime
import json
import psycopg2
import string
import re
import numpy
from sq_psql import make_psql_compatible_column, double_quote_name

import psycopg2.extensions as ext
ext.register_type(psycopg2.extensions.UNICODE)
ext.register_type(psycopg2.extensions.UNICODEARRAY)
ext.register_type(ext.new_type((705,), "UNKNOWN", ext.UNICODE))
# https://github.com/psycopg/psycopg2/issues/282 <-- 'unknown' type codes will not be cast to unicode by default
# http://initd.org/psycopg/docs/usage.html#unicode-handling -- Note that this is only required in Python 2.7


try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

# # Support pandas v0.11-current
try:
    from pandas.util.py3compat import lzip  # pandas 0.11
except ImportError:
    from pandas.compat import lzip  # pandas 0.12 and 0.13

## 10-16-2014: (BD) Works in current Py 2.6 on etl-dev
from pandas import ExcelWriter

from sq_log import get_logger, get_bulleted_list
from sq_util import SquareRootException, is_file, random_string, move_file, delete_file, split_dataframe


def main():
    """ Example to use dbClient in your script """
    # Create new db instance
    dwClient = DBClient('TempNiral', 'dbo', '192.168.110.220,1441', 'npatel', '', 'SQL Server')

    # Run specific query and return dataframe with results
    df = dwClient.RunQuery("SELECT * FROM TempNiral..MovieTbl")

    # Performs above function but instead of query just passes table name
    #df = dwClient.Export('NNA_RO')

    # Imports dataframe into specified table with either replace/append/fail
    dwClient.Import('NNA_RO_new', df, 'append')

    dwClient.Close()

    ## Example of using with 'with':
    with DBClient(dbName='TempNiral') as client:
        df = client.RunQuery("SELECT * FROM TempNiral..MovieTbl")
        self.logger.info(df)


class DBClient(object):
    """ Square-Root DB client in order to work with DW """

    __SUPPORTED_OUTPUT_TYPES = [
        'data_frame'
        , 'list'
        , 'list_fetch_all'
        , 'list_fetch_one'
        , 'list_of_dicts'
        , 'list_of_ordereddicts'
        , 'rowcount'
    ]

    def __init__(self, dbName, dbSchema='public', dbServer=None,
                 dbUser=None, dbPwd=None, dbDriver=None, dbFlavor='postgres'):
        
        """ Setup database connection based on system environment variables"""
        self.logger = get_logger(__name__)
        self.dbFlavor = dbFlavor
        self._set_DBConfig(dbName, dbSchema, dbServer, dbUser, dbPwd, dbDriver)
        self.cnxn_string = ' '
        self.cnxn = None  ## BD 4/23/2014: Ensure cnxn is attribute of class

        try:
            if self.dbFlavor == 'mssql':
                self.cnxn_string = "DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;PORT=%s" % (self.dbDriver, self.dbServer, self.dbName, self.dbUser, self.dbPwd, self.dbPort)
                self.cnxn = pyodbc.connect(self.cnxn_string)

            elif self.dbFlavor == 'postgres':
                self.cnxn_string = "SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;PORT=%s" % (self.dbServer, self.dbName, self.dbUser, self.dbPwd, self.dbPort)
                self.cnxn = psycopg2.connect(database=self.dbName, user=self.dbUser, port=self.dbPort, host=self.dbServer, password=self.dbPwd)

        except BaseException as detail:
            self.logger.critical('DBError while connecting to database {0} with following error \n {1}'.format(self.dbServer,detail))
            self.logger.critical('The following connection string was used: {0}'.format(re.sub('PWD=.*;', 'PWD=<password-masked>', self.cnxn_string)))
            sys.exit(1)
        self.logger.debug('DB Connection {0} successful'.format(re.sub('PWD=.*;', 'PWD=<password-masked>', self.cnxn_string)))

    def Close(self):
        """ Close database client """
        if self.cnxn is not None:
            self.cnxn.close()

    def RunQuery(self, queryStr, output_type='data_frame', query_params=None, auto_commit=True):
        """ Run specific query and return results back in pandas dataframe

        output_type is an enumerable, and has the following accepted options/behaviors:

        option                  behavior
        ===============================================================================
        data_frame (default)    return a Pandas dataframe
        list                    return a python list
        list_fetch_all          return a python list having same output as of cursor.fetchall()
        list_fetch_one          return a python list having same output as of cursor.fetchone()
        list_of_dicts           return a list of Python dictionary objects
        list_of_ordereddicts    return a list of collections.OrderedDict objects (not Py 2.6 compatible)
        rowcount                return row count returned by running given query

        :param queryStr: SQL statement
        :type queryStr: basestring
        :param output_type: Format of output (enumerable, see doc)
        :type output_type: basestring
        :param query_params: data values to be passed with queryStr. Helps one to use parameter markers in queryStr
        :type query_params: list
        :param auto_commit: whether to commit the query to the DB
        :type auto_commit: boolean
        :rtype: object
        :returns: results of SQL query in requested format
        """
        # TODO: Check if queryStr is formatted properly and no sql injection #
        self.logger.debug(u'Running the following query against the DB\n\t{0}'.format(queryStr))
        try:
            cur = self.cnxn.cursor()
            if query_params:
                if type(query_params) is not list:
                    raise SquareRootException('Parameter query_params should be of list type')
                cur.execute(queryStr, query_params)
            else:
                cur.execute(queryStr)

            if output_type == 'rowcount':
                return cur.rowcount
            elif output_type == 'list_fetch_one':
                return cur.fetchone()
                
            # checking if query has returned rows or not. Queries which do not have select statement cause cur.description to be of None type
            if cur.description:
                try:
                    columns = [unicode(column[0], encoding='utf-8') for column in cur.description]
                    results = [OrderedDict(zip([unicode(column[0], encoding='utf-8') for column in cur.description], row))
                               for row in cur.fetchall()]
                except NameError:  # Python 3
                    columns = [column[0] for column in cur.description]
                    results = [OrderedDict(zip([column[0] for column in cur.description], row))
                               for row in cur.fetchall()]
            else:
                columns = []
                results = []
        except BaseException as detail:
            raise SquareRootException(
                'DBError while executing', queryStr, 'against following database', self.dbServer, 'with error \n',
                detail
            )
        finally:
            if cur:
                cur.close()
                if auto_commit:
                  self.cnxn.commit()

        return self._format_output(results, columns, output_type)

    def Export(self, tblname, output_type='data_frame'):
        """ Export specified table into dataframe (for now) from database (DW)

        This function effectively runs "SELECT * " against the desired table.

        output_type is an enumerable, and has the following accepted options/behaviors:

        option                  behavior
        ===============================================================================
        data_frame (default)    return a Pandas dataframe
        list                    return a python list
        list_fetch_all          return a python list having same output as of cursor.fetchall()
        list_of_dicts           return a list of Python dictionary objects
        list_of_ordereddicts    return a list of collections.OrderedDict objects (not Py 2.6 compatible)

        :param tblname: Name of table to export
        :type tblname: basestring
        :param output_type: Format of output (enumerable, see doc)
        :type output_type: basestring
        :rtype: object
        :returns: results of SQL query in requested format
        """
        #TODO: Add support for other datastructure such as .csv, .xls or other useful

        export_query = ("SELECT * FROM {0};").format(self._format_tablename(tblname))
        return self.RunQuery(export_query, output_type)

    def ExportToFile(self, queryStr, filename, overwrite_policy='delete'):
        """ Export the results of a query to an Excel or CSV file

        The file must end in "csv", "xls*" or ".json".

        This method may not be suitable for large data exports, as an internal Pandas DataFrame is created.

        overwrite_policy is an enumerable with the following options/behaviors:

        policy                      behavior
        ========================================================================
        delete                      Overwrite existing file
        move                        Rename the existing file (random file name, see log messages)
                                    If move failed, an exception is raised

        :param queryStr: Query to export
        :type queryStr: basestring
        :param filename: Fully specified location of file to create
        :type filename: basestring
        :param overwrite_policy: What to do if a file exists at the location specified (enum, see doc)
        :type overwrite_policy: basestring
        :raises: SquareRootException
        """

        ## Check if file already exsists
        if is_file(filename):
            if overwrite_policy == 'move':
                old_filename = '{0}.{1}'.format(filename, random_string())
                self.logger.warning('A file exists at {0}. It will be renamed to {1}.'.format(filename, old_filename))
                was_moved = move_file(filename, old_filename)
                if not was_moved:
                    raise SquareRootException('{0} could not be moved to {1}.'.format(filename, old_filename))
            elif overwrite_policy == 'delete':
                was_deleted = delete_file(filename)
                if not was_deleted:
                    raise SquareRootException('A file exists at {0} and could not be deleted.'.format(filename))

        ## get results
        results = self.RunQuery(queryStr, output_type='data_frame')

        ## get extension
        extension = filename.split('.')[-1]

        if extension == 'csv':
            results.to_csv(filename, sep=',', index=False)
        elif 'xls' in extension:
            writer = ExcelWriter(filename)
            results.to_excel(writer, 'Results', index=False)
            writer.close()
        elif extension == 'json':
            results
        else:
            raise SquareRootException('{0} has an unsupported export file type.'.format(filename))

    def Insert(self, tblname, columns_list=None, data_values=None):
        """Insert passed data_values into specified table by mapping data_values
        to columns specified in columns_list positionally

        Table should exists in DB else method will fail

        :param tblname: table into which data should be inserted
        :type tblname: string
        :param columns_list: list of column names to which data_values will be mapped
        :type columns_list: list
        :param data_values: data to be inserted into specified columns. Use None for inserting NULL value.
        :type data_values: list
        :raises: SquareRootException
        """

        exists = self._table_exists(tblname, self.cnxn)

        if exists:
            self.logger.debug('Inserting data into table {0}'.format(tblname))

            if len(columns_list) != len(data_values):
                raise SquareRootException('Number of columns specified in columns_list does not match with the data passed in data_values')

            if type(columns_list) is not list or type(data_values) is not list:
                raise SquareRootException('Parameters columns_list and data_values should be of list type')

            placeholder = ''
            for i in range(0, len(columns_list)):
                if self.dbFlavor == 'mssql':
                    placeholder += '?,'
                elif self.dbFlavor == 'postgres':
                    placeholder += '%s,'

            # removing last comma
            placeholder = placeholder[:-1]

            formatted_tblname = self._format_tablename(tblname)
            query_str = 'INSERT INTO {0} {1} VALUES ({2})'.format(formatted_tblname, str(columns_list).replace('[', '(').replace(']', ')').replace('\'',''), placeholder)

            self.RunQuery(queryStr=query_str, query_params=data_values)

        else:
            raise SquareRootException('Table {0} does not exists in Database'.format(tblname))


    def Import(self, tblname, data, if_exists='fail', format_file=None, keys=None, keep_case=False, is_transactional=False, log=False, df_chunk_size=None, use_null=False):
        """ Import specified dataframe into database (DW)

        Extending existing pandas sql api to add support for MSSQL and Postgres

        To create a table if it doesn't exist, set if_exists='append'.

        'data' must be either:
            * a pandas DataFrame
            * a list of dict objects
            * a list of OrderedDicts
        """
        if isinstance(data, list):
            if data:
                if not all(isinstance(data_point, dict) for data_point in data):
                    raise SquareRootException(
                        'Only pandas DataFrame and lists of dicts/OrderedDicts are acceptable input'
                    )
                data = pandas.DataFrame(data)
            else:
                self.logger.critical('Empty list passed to Import() of db_client.py. Cannot continue import. Exiting.')
                return

        # Replace empty strings with numpy.nan which will be imported in DB as <Null>
        if use_null:
            data.replace(to_replace='',value=numpy.nan,inplace=True)

        if self.dbFlavor == 'mssql' or self.dbFlavor == 'postgres':
            exists = self._table_exists(tblname, self.cnxn)

            if if_exists == 'fail' and exists:
                raise SquareRootException(
                    "Table '{s}' already exists. if_exists param needs a value - replace/append".format(s=tblname)
                )
            elif if_exists == 'fail' and not exists:
                raise SquareRootException(
                    "Table '{s}' doesn't exists. if_exists param needs a value - replace/append".format(s=tblname)
                )

            create = None
            drop = None

            if exists and if_exists == 'replace':
                drop = "DROP TABLE {s} CASCADE".format(s=self._format_tablename(tblname))
            if (not exists and if_exists == 'append') or if_exists == 'replace':
                if self.dbFlavor == 'mssql':
                    create = self._get_schema(data, self._format_tablename(tblname), format_file, keys)
                elif self.dbFlavor == 'postgres':
                    create, columns = self._get_schema(data, self._format_tablename(tblname), format_file, keys)
            # "Replace" and "Append" conditions

            if create is not None:
                if drop is not None:
                    self.RunQuery(drop, auto_commit=not is_transactional)
                self.RunQuery(create, auto_commit=not is_transactional)

            # if df_chunk_size is passed then split df and import
            if df_chunk_size:
                list_of_data = split_dataframe(data, df_chunk_size)
            else:
                list_of_data = [data]

            for index, data in enumerate(list_of_data):
                # deciding auto commit
                if is_transactional and index + 1 != len(list_of_data):
                    auto_commit = False
                else:
                    auto_commit = True

                # Replace spaces in DataFrame column names with _.
                if self.dbFlavor == 'mssql':

                    safe_names = [s.replace(' ', '_').replace('.', '_').replace('\n', '_').strip().rstrip('_')[:127] for s in data.dtypes.index]
                    self._write_data(data, self._format_tablename(tblname), safe_names)

                elif self.dbFlavor == 'postgres':
                    if if_exists == 'append':
                        columns = make_psql_compatible_column(data.dtypes.index)
                    self._write_data(data, self._format_tablename(tblname), columns, keep_case=keep_case, auto_commit=auto_commit, log=log)

        else:
            raise NotImplementedError('This module is not implemented')


    def GetColumnNames(self, tablename):
        """ Returns column names as list

        :param  tablename: tablename
        :type   tablename: str
        :returns: column names
        :rtype: list[str]
        """
        
        #schema_name = tablename.split('.')[0]
        
        #table_name = tablename.split('.')[1]

        tablename = self._format_tablename(tablename)

        cursor = self.cnxn.cursor()
        cursor.execute("Select * FROM {0} LIMIT 0".format(tablename))
        colnames = [desc[0] for desc in cursor.description]

        #cursor.execute("select column_name from information_schema.columns where table_schema = '{0}' and table_name='{1}'".format(schema_name, table_name))
        #column_names = [row[0] for row in cursor]

        self.cnxn.commit()        
        return colnames
        #return [r.column_name for r in cursor.columns(catalog=self.dbName, schema=self.dbSchema, table=tablename)]


    ### Utility methods ###


    def _set_DBConfig(self, dbName=None, dbSchema='public', dbServer=None,
                      dbUser=None, dbPwd=None, dbDriver= None, dbPort=None):
        """ DB config """
        self.dbServer = os.environ.get("DEFAULT_DB_INSTANCE_NAME") if dbServer is None else dbServer
        self.dbUser = os.environ.get("DB_USER") if dbUser is None else dbUser
        self.dbPwd = "" if dbPwd is None else dbPwd
        self.dbName = os.environ.get("DEFAULT_DB") if dbName is None and self.dbFlavor == 'postgres' else dbName
        self.dbDriver = self._get_DBDriver() if dbDriver is None else dbDriver
        self.dbSchema = 'dbo' if self.dbFlavor == 'mssql' and dbSchema is None else 'public' if self.dbFlavor == 'postgres' and dbSchema is None else dbSchema
        self.dbPort = '1433' if self.dbFlavor == 'mssql' and dbPort is None else '5433' if self.dbFlavor == 'postgres' and dbPort is None else dbPort

    def _get_DBDriver(self):
        """
        Get MSSQL driver name
        """
        if sys.platform.startswith("win"):
            return 'SQL Server'
        if self.dbFlavor == 'mssql':
            return 'ODBC Driver 11 for SQL Server'

    def _write_data(self, data, table, names, keep_case=False, auto_commit=True, log=False):

        """
        Create INSERT sql statement for MSSQL
        """
        
        if self.dbFlavor == 'mssql':
            bracketed_names = ['[' + column + ']' for column in names]
            col_names = ','.join(bracketed_names)
            wildcards = ','.join(['?'] * len(names))
            insert_query = 'INSERT INTO %s (%s) VALUES (%s)' % (table, col_names, wildcards)
        elif self.dbFlavor == 'postgres':
            ##Preparing column names for cursor.mogrify

            if keep_case:
                # check to see if any column names have uppercase characters.  If so, make them quoted identifiers
                quoted = ['"%s"' % n if n.lower() != n else n for n in names]
                col_names = ','.join(quoted)
                col_names = col_names.strip().replace(' ', '_').replace('.', '_').replace('-','_').replace('\n', '_')
            else:
                col_names = ','.join(names)

            cur_mog_second = ['%s' for name in names]
            b = tuple(cur_mog_second)
            d = str(b)
            e = d.replace("'","")

        # changing dtypes of data-frame columns to object type. As pyodbc doesn't support datetime64 data-type and NaT(Not a Time)
        data = data.astype(object)
        hasExtraCommaInTuple = False
        
        # Handle 'NaN' values inside sql server
        data = data.where((pandas.notnull(data)), None, inplace=False)
        
        # pandas types are badly handled if there is only 1 column ( Issue #3628 )
        if not len(data.columns) == 1:
            # In order to handle column size issue (if input data length is longer than column size)
            data = [tuple(x) for x in data.values]
        else:
            # In order to handle column size issue (if input data length is longer than column size)
            data = [tuple(x) for x in data.values.tolist()]
            # handle extra comma with each single element tuple in the list, only in case of single column feed
            hasExtraCommaInTuple = True
        try:
            cur = self.cnxn.cursor()
            if self.dbFlavor == 'mssql':
                if len(data):
                    cur.executemany(insert_query, data)
                self.cnxn.commit()

            elif self.dbFlavor == 'postgres':
                
                #Using cusor.mogrify method in order to build insert query
                try:
                    args_str = ','.join(cur.mogrify(e, unicode(x, encoding='utf-8')) for x in data)
                except (TypeError, NameError):  # Python 3
                    args_str = ','.join(cur.mogrify(e, x).decode('utf-8') for x in data)
                if hasExtraCommaInTuple is True:
                    args_str = args_str.replace(",)", ")")

                insert_query = 'INSERT INTO %s (%s) VALUES' % (table, col_names) + args_str
                if len(data):
                    if not log:
                        disable_log = "SET log_statement = 'none';SET log_min_duration_statement = -1;"
                        enable_log = "SET log_statement = 'all';SET log_min_duration_statement = 0;"

                        # We can not combine these 3 statements otherwise INSERT..INTO.. query will get logged
                        cur.execute(disable_log)
                        cur.execute(insert_query)
                        cur.execute(enable_log)
                    else:
                        cur.execute(insert_query)

                if auto_commit:
                    self.cnxn.commit()

        except BaseException as detail:
            self.cnxn.rollback()
            raise SquareRootException('Error occurred while executing insert query during import process of datFrame into database with message: {0}'.format(detail))

        finally:
            if cur:
                cur.close()
            
            # removing commit statement from here as a part of making Import method transaction
            # self.cnxn.commit()

    def parse_format_file(self, frame, formatFile):
        col_num_ff = 0
        column_types = []
        with open(formatFile) as ff:

            try:
                dic = json.load(ff)

            except Exception as detail:
                raise ValueError('*******Error in parsing format file*******\n {0}'.format(detail))

            finally:
                ff.close()

            col_num_ff = 0
            column_types = []
            col_list = frame.dtypes.index
            col_list_lower = list(frame.dtypes.index)
            col_list_lower = [x.lower() for x in col_list_lower]

            for key in dic:
                col_name = key
                if col_name.lower() in col_list_lower:
                    col_name = frame.dtypes.index[col_list_lower.index(col_name.lower())]
                    col_num_ff += 1
                    safe_col_name = col_name.replace(' ', '_').replace('.', '_').replace('\n', '_').strip().rstrip('_')[:127]
                    col_type = dic[key]
                    # temporary list to create append data in column_types that is a list of lists
                    col_name_type = (safe_col_name, col_type)
                    column_types.append(col_name_type)
                else:
                    raise ValueError('*******Error! Invalid column name specified in format file*******')

        if col_num_ff != len(frame.dtypes.index):
                raise ValueError('*******Error! Less column names specified in format file as compared to columns present in input file*******')

        # sort column_types according to order of columns in frame
        col_list_frame = list(frame.columns.values)
        column_types.sort(key=lambda x: col_list_frame.index(x[0]))

        return column_types

    def _get_schema(self, data, tblname, format_file=None, keys=None):
        """
        Return a CREATE TABLE statement to suit the contents of a DataFrame.
        Extending existing pandas sql api to add support for MSSQL
        """
        column_types = []
        if self.dbFlavor == 'mssql' or self.dbFlavor == 'postgres':

            if format_file != None:
                column_types = self.parse_format_file(data, format_file)

            else:
                lookup_type = lambda dtype: self._get_type(dtype.type)

                # Replace spaces in DataFrame column names with _.
                if self.dbFlavor == 'mssql':
                    safe_columns = [s.replace(' ', '_').replace('.', '_').replace('\n', '_').strip().rstrip('_')[:127] for s in
                            data.dtypes.index]
                elif self.dbFlavor == 'postgres':
                    safe_columns = make_psql_compatible_column(data.dtypes.index);
                column_types = lzip(safe_columns, map(lookup_type, data.dtypes))

            if self.dbFlavor == 'mssql':
                columns = ',\n  '.join('[%s] %s' % x for x in column_types)
            elif self.dbFlavor == 'postgres':
                column_list = []
                column_name_list = []
                #Checking column names for reserved words and non-ascii values
                for x in column_types:
                    col_name = x[0]
                    col_type = x[1]
                    column_list.append(col_name + ' ' + col_type)
                    column_name_list.append(col_name)

                columns = ',\n  '.join('%s' % x for x in column_list)

            keystr = ''
            if keys is not None:
                if isinstance(keys, pandas.compat.string_types):
                    keys = (keys,)
                keystr = ', PRIMARY KEY (%s)' % ','.join(keys)
            template = """CREATE TABLE %(name)s (
                    %(columns)s
                    %(keystr)s
                    );"""

            create_statement = template % {'name': tblname,
                                           'columns': columns,
                                           'keystr': keystr}
                
        else:
            raise NotImplementedError('This module is not implemented')

        if self.dbFlavor == 'mssql':
            return create_statement
        elif self.dbFlavor == 'postgres':
            return create_statement, column_name_list

    def _get_type(self, pytype):
        """
        Extending existing pandas sql api to add support for MSSQL and Pos
        """

        if self.dbFlavor == 'mssql':

            sqltype = {'mssql': 'VARCHAR (8000)'}

            if issubclass(pytype, numpy.floating):
                sqltype['mssql'] = 'VARCHAR (8000)'

            if issubclass(pytype, numpy.integer) or issubclass(pytype, numpy.int64):
                sqltype['mssql'] = 'VARCHAR (8000)'

            if issubclass(pytype, numpy.datetime64) or pytype is datetime:
                sqltype['mssql'] = 'VARCHAR (8000)'

            if pytype is datetime.date:
                sqltype['mssql'] = 'VARCHAR (8000)'

            return sqltype[self.dbFlavor]

        elif self.dbFlavor == 'postgres':

            sqltype = {'postgres': 'VARCHAR (8000)'}

            if issubclass(pytype, numpy.floating):
                sqltype['postgres'] = 'VARCHAR (8000)'

            if issubclass(pytype, numpy.integer) or issubclass(pytype, numpy.int64):
                sqltype['postgres'] = 'VARCHAR (8000)'

            if issubclass(pytype, numpy.datetime64) or pytype is datetime:
                sqltype['postgres'] = 'VARCHAR (8000)'

            if pytype is datetime.date:
                sqltype['postgres'] = 'VARCHAR (8000)'

            return sqltype[self.dbFlavor]

    def _format_tablename(self, tblname, no_quotes=False):
        """
        Format table string into database.schema.table pattern
        """
        if self.dbFlavor == 'postgres':
            tblname = tblname.lower()
        table_str = tblname.split(".")

        if no_quotes:
            # e.g. database.schema.table or database..table
            if len(table_str) == 3:
                if table_str[1] == "":
                    table_str[1] = self.dbSchema
                name = "{d}.{s}.{t}".format(d=table_str[0], s=table_str[1], t=table_str[2])
            # e.g. schema.table
            elif len(table_str) == 2:
                if table_str[0] == "":
                    table_str[0] = self.dbSchema
                name = "{d}.{s}.{t}".format(d=self.dbName.lower(), s=table_str[0], t=table_str[1])
            # e.g. table
            elif len(table_str) == 1:
                name = "{d}.{s}.{t}".format(d=self.dbName.lower(), s=self.dbSchema.lower(), t=table_str[0])
            # any other invalid case
            else:
                raise SquareRootException('Cannot format table name using the default / passed values for database, schema and table names. Trying to format table name \'{0}\''.format(tblname))
        else:
            # e.g. database.schema.table or database..table
            if len(table_str) == 3:
                if table_str[1] == "":
                    table_str[1] = self.dbSchema
                name = "{d}.{s}.{t}".format(d=double_quote_name(table_str[0]), s=double_quote_name(table_str[1]), t=double_quote_name(table_str[2]))
            # e.g. schema.table
            elif len(table_str) == 2:
                if table_str[0] == "":
                    table_str[0] = self.dbSchema
                name = "{d}.{s}.{t}".format(d=double_quote_name(self.dbName), s=double_quote_name(table_str[0]), t=double_quote_name(table_str[1]))
            # e.g. table
            elif len(table_str) == 1:
                name = "{d}.{s}.{t}".format(d=double_quote_name(self.dbName), s=double_quote_name(self.dbSchema), t=double_quote_name(table_str[0]))
            # any other invalid case
            else:
                raise SquareRootException('Cannot format table name using the default / passed values for database, schema and table names. Trying to format table name \'{0}\''.format(tblname))

        return name

    def _table_exists(self, tblname, con):
        """
        Extending existing pandas sql api to add support for MSSQL and Postgres
        """

        tblName = self._format_tablename(tblname, no_quotes=True)

        query = ("select * from INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = '{0}' "
                 "and TABLE_SCHEMA ='{1}'"
                 "and TABLE_NAME ='{2}';").format(tblName.split(".")[0],
                                                   tblName.split(".")[1], tblName.split(".")[2].strip())

        return len(self.RunQuery(query, 'list_of_dicts')) > 0

    def _format_output(self, results, columns, output_type):
        """ Output converter

        Format output given an output_type.
        """
        if output_type not in self.__SUPPORTED_OUTPUT_TYPES:
            self.logger.critical('{0} is not a supported output type. Please use one of:'
                                 '\n{1}'.format(output_type, get_bulleted_list(self.__SUPPORTED_OUTPUT_TYPES)))
            raise SquareRootException('Output format of requested type ({0}) is not supported.'.format(output_type))

        if output_type == 'list_of_ordereddicts':
            return results
        elif output_type == 'list_of_dicts':
            return [dict(x) for x in results]
        elif output_type == 'list':
            return [item for x in results for item in x.values()]
        elif output_type == 'list_fetch_all':
            return [x.values() for x in results]
        elif output_type == 'data_frame':
            if results:
                return pandas.DataFrame(results, columns=columns)
            else:
                return pandas.DataFrame(columns=columns)
        else:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.Close()

if __name__ == '__main__':
    main()
