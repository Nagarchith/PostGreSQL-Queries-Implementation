#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import sys
import csv

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'Ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
RANGE_METADATA = 'RangePMetadata'
ROUNDROBIN_METADATA = 'RoundRobinPMetadata'
USER_ID_COLNAME = 'UserID'
MOVIE_ID_COLNAME = 'MovieID'
RATING_COLNAME = 'Rating'
INPUT_FILE_PATH = 'C:/Users/Nagarchith Balaji/Desktop/MS-Spring17/DDS/Assignment/A1/test_data.dat'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    with open(ratingsfilepath,'r') as file:
        data = file.read()


    data = data.replace('::', ':')


    with open(ratingsfilepath, 'w') as file:
        file.write(data)

    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
        cur.execute("CREATE  TABLE IF NOT EXISTS "+ ratingstablename+" (UserID INTEGER, MovieID INTEGER, Rating FLOAT,Time INTEGER)")

        out = open(ratingsfilepath,'r')
        cur.copy_from(out, ratingstablename, sep=':',columns=('UserID', 'MovieID', 'Rating','Time'))
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN Time")

        cur.close()
        openconnection.commit()
    except psycopg2.DatabaseError, e:

        print 'Error %s' % e

        sys.exit(1)

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    try:
        lab = RANGE_TABLE_PREFIX
        #print lab
        cur = openconnection.cursor()
        #cur.execute("SELECT * FROM "+ratingstablename)

        #print "Ratings loaded"
        cur.execute("DROP TABLE IF EXISTS "+RANGE_METADATA)
        cur.execute("CREATE TABLE IF NOT EXISTS "+ RANGE_METADATA+" (PartitionNum INT, MinRating REAL, MaxRating REAL, NumofTables INT)")
        Min = 0.0
        Max = 5.0
        increments = Max / (float)(numberofpartitions)
        i = 0
        while i < numberofpartitions:
            table = lab + `i`
            cur.execute("DROP TABLE IF EXISTS "+table)
            cur.execute("CREATE TABLE IF NOT EXISTS "+ table +" (UserID INTEGER, MovieID INTEGER, Rating FLOAT)")
            i += 1
        i = 0
        ul=0
        while Min < Max:
            ll = Min
            ul = Min + increments
            if ll < 0:
                ll= 0.0

            if ll == 0.0:
                #cur.execute("SELECT * FROM "+ratingstablename+" WHERE Rating >= '"+ ll+"' AND Rating <= '"+ ul +"' ")
                #cur.execute("SELECT * FROM %s WHERE Rating >= %f AND Rating <= %f" % (ratingstablename, ll, ul))
                #query = "SELECT * FROM " + ratingstablename + " WHERE Rating >= '"+ll+ "' AND Rating <= '"+ ul+ "' "
                #cur.execute("SELECT * FROM " + ratingstablename + " WHERE Rating >= '"+ll+ "' AND Rating <= '"+ ul+ "' ")
                #rows = cur.fetchall()
                table = lab + str(i)
                #for row in rows:
                    #cur.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (table, row[0], row[1], row[2]))
                cur.execute("INSERT INTO %s (SELECT * FROM %s WHERE Rating >= %f AND Rating <= %f)" % (table, ratingstablename, ll, ul))


            if ll != 0.0:
                #cur.execute("SELECT * FROM %s WHERE Rating > %f AND Rating <= %f" % (ratingstablename, ll, ul))
                #cur.execute("SELECT * FROM " + ratingstablename + " WHERE Rating >= '" + str(ll) + "' AND Rating <= '" + str(ul) + "' ")
                #rows = cur.fetchall()
                table = lab + str(i)
                #for row in rows:
                    #cur.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (table, row[0], row[1], row[2]))
                cur.execute("INSERT INTO %s (SELECT * FROM %s WHERE Rating > %f AND Rating <= %f)" % (table, ratingstablename, ll, ul))

            #cur.execute("INSERT INTO RangePMetadata (PartitionNum, MinRating, MaxRating)  VALUES(" + i + "," + ll + "," + ul + ")")
            cur.execute("INSERT INTO "+RANGE_METADATA+" (PartitionNum, MinRating, MaxRating,NumofTables) VALUES(%d, %f, %f,%d)" % (i, ll, ul,numberofpartitions))
            Min = ul
            i += 1;

        #openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cur:
            cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    try:
        lab = RROBIN_TABLE_PREFIX
        #print lab
        cur = openconnection.cursor()
        #cur.execute("SELECT * FROM " + ratingstablename)
        #if not bool(cur.rowcount):
        #   print "Ratings table not found"
        #  return
        cur.execute("DROP TABLE IF EXISTS "+ ROUNDROBIN_METADATA)
        cur.execute("CREATE TABLE IF NOT EXISTS "+ ROUNDROBIN_METADATA +" (PartitionNum INT, NextPartition INT)")
        i = 0
        #cur.execute("SELECT * FROM " + ratingstablename)
        #rows = cur.fetchall()
        lastInsertedTable = 0
        for row in range(0,numberofpartitions):
            #print row
            if i < numberofpartitions:
                table = lab + `i`
                cur.execute("DROP TABLE IF EXISTS " + table)
                cur.execute("CREATE TABLE IF NOT EXISTS "+table+" (UserID INT, MovieID INT, Rating FLOAT)")
                cur.execute("INSERT INTO %s (UserID,MovieID,rating)  (SELECT UserID,MovieID,Rating FROM (SELECT UserID,MovieID,rating,row_number() OVER () AS RM FROM %s) META WHERE mod(RM,%d) = %d)" % (table, ratingstablename, numberofpartitions, i))
                #cur.execute("INSERT INTO %s (SELECT * , row_num() OVER() AS rownum FROM %s WHERE mod(rownum,%d) = %d)"%(table,ratingstablename,numberofpartitions,i))
                i += 1
                lastInsertedTable = lastInsertedTable + 1
                y = (lastInsertedTable % numberofpartitions)
                #print y,lastInsertedTable
            else:
                table = lab + `y`
                #cur.execute("INSERT INTO " + table + " (UserID, MovieID, Rating) VALUES(%d,%d,%f)" % (row[0], row[1], row[2]))
                cur.execute("INSERT INTO %s (UserID,MovieID,rating)  (SELECT UserID,MovieID,Rating FROM (SELECT UserID,MovieID,Rating,row_number() OVER () AS RM FROM %s) META WHERE mod(RM,%d) = %d)" % (table, ratingstablename, numberofpartitions, i))
                lastInsertedTable = (lastInsertedTable+1) % numberofpartitions
                y = lastInsertedTable
                #print y,lastInsertedTable
        #cur.execute("INSERT INTO RoundRobinPMetadata (PartitionNum, NextPartition) VALUES("+numberofpartitions+","+ lastInsertedTable+")")
        cur.execute("INSERT INTO "+ROUNDROBIN_METADATA+" (PartitionNum, NextPartition) VALUES(%d,%d)"%(numberofpartitions ,y ))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cur:
            cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    #print "RoundRobin Insert"
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM "+ROUNDROBIN_METADATA)
    row = cur.fetchone()
    nexttable = row[1]
    numoftables= row[0]
    #print nexttable, numoftables
    if(numoftables>0):

        if(rating >=0 and rating <=5.0):
            table = RROBIN_TABLE_PREFIX + str(nexttable)
            cur.execute("INSERT INTO " + table + " (UserID, MovieID, Rating) VALUES(%d,%d,%f)" % (userid, itemid, rating))
            nexttable = (nexttable)% numoftables
            cur.execute("UPDATE "+ROUNDROBIN_METADATA+" SET PartitionNum = %d, NextPartition = %d"%(numoftables ,nexttable ))
        else:
            print "invalid rating"
    else:
        print "No partitions"
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    #print "Range Insert"
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM "+RANGE_METADATA+" WHERE MinRating < %f and MaxRating >= %f "%(rating,rating))
    row = cur.fetchone()
    numTab = row[0]
    #table = row[1]
    #print numTab
    tablename = RANGE_TABLE_PREFIX + str(numTab)
    cur.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (tablename, userid, itemid, rating))
    openconnection.commit()

def DeletePartitions(openconnection):
    cur= openconnection.cursor()
    cur.execute("SELECT NumofTables FROM "+RANGE_METADATA)
    num = cur.fetchone()
    j = num[0]
    for i in range(0,j):
        table = RANGE_TABLE_PREFIX+`i`
        cur.execute("DROP TABLE "+table)
    cur.execute("DROP TABLE " + RANGE_METADATA)
    cur.execute("SELECT PartitionNum FROM "+ROUNDROBIN_METADATA)
    num = cur.fetchone()
    j = num[0]
    for i in range(0, j):
        table = RROBIN_TABLE_PREFIX+`i`
        cur.execute("DROP TABLE "+table)
    cur.execute("DROP TABLE " + ROUNDROBIN_METADATA)




def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,

            loadratings('Ratings','F:/MS-Spring17/DDS/Assignment/A1/test_data.dat', con)
            #loadratings('Ratings', 'F:/MS-Spring17/DDS/Assignment/A1/ratings.dat', con)

            #rangepartition('Ratings', 3, con)
            roundrobinpartition('Ratings',4,con)
            roundrobininsert('Ratings',10,20,5,con)
            #roundrobininsert('Ratings', 11, 20, 5, con)
            #rangeinsert('Ratings', 10, 20, 5, con)


            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
