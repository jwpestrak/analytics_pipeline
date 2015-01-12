#!/path/to/anaconda/bin/python

"""
################################################################################
The purpose of this script is to 
  1. Query transaction information, at a UPC level, of items sold 
     through the online shop.
  2. Organize the result set, by style group, into a time series of daily unit 
     sales. Specifically, two time series are of interest - the complete time 
     series and the 'first 91 days' time series. 
  3. Polynomials are fit - degrees 0 through 4 - to the 'first 91 days' time 
     series. The coefficients and sum of residuals squared are persisted as 
     values in a dict object (with the corresponding key being the style group
     identifier). Other data, such as count of zero-unit-sales days, are also
     persisted as values for each style group key in the dict object.
  4. These 'fit statistics' are INSERTed into a MySQL database table - 
     `fce`.`sg_polyfit` - for later retrieval and use by unsupervised
     learning algorithms.  
TO DO:
  - Normalize time series with respect to magnitude
  - 'Functionalize' creation of SQL INSERT statement.
  - JSON for database credentials
"""

#import seaborn
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import pandas as pd
import pyodbc
from numpy import polyfit
import mysql.connector
import imp

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    "A mysql.connector Converter that handles Numpy types."

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)

db_creds = imp.load_source('db_creds.py', '/path/to/db_creds.py')

pyodbc.pooling = False    

sql_file = open('/path/to/select_star_dev_table.sql', 'r')
#sql_file = open('/path/to/queries/poc_plot_generation.sql', 'r')
sql_statement = sql_file.read()
sql_file.close()

connection_string = 'DRIVER={Teradata};DBCNAME=TD1234ABC;PORT=1025;UID=wxyz;PWD=wxyz;'
conn = pyodbc.connect(connection_string, ANSI = True)
crsr = conn.cursor()

crsr.execute(sql_statement)

col_names = ['UPC_IDNT', 
             'STYLE_GROUP_IDNT', 
             'STYLE_GROUP_DESC',
             'COLR_IDNT', 
             'COLR_DESC', 
             'SIZE_1_IDNT',
             'SIZE_2_IDNT',
             'RINGING_DT',
#             'DAP',
             'DPCNT']

df = pd.DataFrame.from_records(crsr.fetchall(), columns = col_names)

conn.close()

df_dict = {sg: df[df.STYLE_GROUP_IDNT == sg].copy() for sg in df.STYLE_GROUP_IDNT.unique()}

fstm = '/path/to/plot_repository/'
fig = plt.figure()

conn = mysql.connector.Connect(**db_creds.login_info)
conn.set_converter_class(NumpyMySQLConverter)
crsr = conn.cursor()

for sg, df in df_dict.iteritems():
 
    print("Started", sg)
    
    if len(df.values) < 7:
        print("Skipped due to low observation count.")
        continue
    
    x = df.groupby(by = ['RINGING_DT'], sort = True)['DPCNT'].sum()

    x.index = pd.to_datetime(x.index, unit = 'D')
    x = x.resample('D').fillna(0)

    #fig = plt.figure()
    fig.clear()
    plt.plot(x)
    plt.xlabel("Purchase Date")
    plt.ylabel("Purchase Count")
    plt.title(sg + ": Daily Purchase Counts")
    fig.savefig(fstm + sg + '.svg')

    x_p90 = x.head(91)

    #fig = plt.figure()
    fig.clear()
    plt.plot(x_p90)
    plt.xlabel("Purchase Date")
    plt.ylabel("Purchase Count")
    plt.title(sg + ": Daily Purchase Counts - First 91 Days")
    fig.savefig(fstm + sg + '_p90.svg')

    x_p90 = pd.DataFrame(data = x_p90)
    x_p90['DayNum'] = x_p90.index.map(lambda x: x.toordinal())
    x_p90['DayNum'] = x_p90['DayNum'] - x_p90['DayNum'].min()

    # collect fit statistics

    try:
        zd_cnt = x_p90['DPCNT'].value_counts()[0]
    except KeyError:
        zd_cnt = 0
    
    ftst = {
        'sg': sg,
        'zd_cnt': zd_cnt,
        'dmax': int(x_p90['DPCNT'].max())
    }

    # Add order statistics?
    # Add adjusted R^2?

    for degree in xrange(5):
        fit = polyfit(x_p90['DayNum'],
                      x_p90['DPCNT'],
                      deg = degree,
                      full = True)
        ftst['p{0}_dv'.format(degree)] = fit[1][0]
        for power, coef in enumerate(reversed(fit[0])):
            ftst['p{0}_a{1}'.format(degree, power)] = coef

    # write fit statistics to database
    
    placeholders = ', '.join(['%s'] * len(ftst))
    columns = ', '.join(ftst.keys())
    sql = "INSERT INTO `fce`.`sg_polyfit` (%s) VALUES (%s)" % (columns, placeholders)
    crsr.execute(sql, ftst.values())     
 
    conn.commit()
    
    print("Completed", sg)

conn.close()

print("Script is done!")
