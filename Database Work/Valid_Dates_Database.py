import psycopg2;
import os;
from dotenv import load_dotenv;
import datetime

def get_valid_season_dates():
    """
    This will populate my valid_season_dates database
    with opening day and the last day of the world series for
    all seasons 1990-2024 inclusive.
    """
    valid_dates = {}
    valid_dates_1990 = (
        datetime.datetime(1990,4,9),
        datetime.datetime(1990,10,20)
    )
    valid_dates[1990]= valid_dates_1990
    valid_dates_1991 = (
        datetime.datetime(1991,4,8),
        datetime.datetime(1991,10,27)
    )
    valid_dates[1991] = valid_dates_1991
    valid_dates_1992 = (
        datetime.datetime(1992,4,6),
        datetime.datetime(1992,10,24)
    )
    valid_dates[1992] = valid_dates_1992
    valid_dates_1993 = (
        datetime.datetime(1993,4,5),
        datetime.datetime(1993,10,23)
    )
    valid_dates[1993] = valid_dates_1993
    valid_dates_1994 = (
        datetime.datetime(1994,4,3),
        datetime.datetime(1994,8,11)
    )
    valid_dates[1994] = valid_dates_1994
    valid_dates_1995 = (
        datetime.datetime(1995,4,25),
        datetime.datetime(1995,10,28)
    )
    valid_dates[1995] = valid_dates_1995
    valid_dates_1996 = (
        datetime.datetime(1996,3,31),
        datetime.datetime(1996,10,26)
    )
    valid_dates[1996] = valid_dates_1996
    valid_dates_1997 = (
        datetime.datetime(1997,4,1),
        datetime.datetime(1997,10,26)
    )
    valid_dates[1997] = valid_dates_1997
    valid_dates_1998 = (
        datetime.datetime(1998,3,31),
        datetime.datetime(1998,10,21)
    )
    valid_dates[1998] = valid_dates_1998
    valid_dates_1999 = (
        datetime.datetime(1999,4,4),
        datetime.datetime(1999,10,27)
    )
    valid_dates[1999] = valid_dates_1999
    valid_dates_2000 = (
        datetime.datetime(2000,3,29),
        datetime.datetime(2000,10,26)
    )
    valid_dates[2000] = valid_dates_2000
    valid_dates_2001 = (
        datetime.datetime(2001,4,1),
        datetime.datetime(2001,11,4)
    )
    valid_dates[2001] = valid_dates_2001
    valid_dates_2002 = (
        datetime.datetime(2002,3,31),
        datetime.datetime(2002,10,27)
    )
    valid_dates[2002] = valid_dates_2002
    valid_dates_2003 = (
        datetime.datetime(2003,3,30),
        datetime.datetime(2003,10,25)
    )
    valid_dates[2003] = valid_dates_2003
    valid_dates_2004 = (
        datetime.datetime(2004,3,30),
        datetime.datetime(2004,10,27)
    )
    valid_dates[2004] = valid_dates_2004
    valid_dates_2005 = (
        datetime.datetime(2005,4,3),
        datetime.datetime(2005,10,26)
    )
    valid_dates[2005] = valid_dates_2005
    valid_dates_2006 = (
        datetime.datetime(2006,4,2),
        datetime.datetime(2006,10,27)
    )
    valid_dates[2006] = valid_dates_2006
    valid_dates_2007 = (
        datetime.datetime(2007,4,1),
        datetime.datetime(2007,10,28)
    )
    valid_dates[2007] = valid_dates_2007
    valid_dates_2008 = (
        datetime.datetime(2008,3,25),
        datetime.datetime(2008,10,27)
    )
    valid_dates[2008] = valid_dates_2008
    valid_dates_2009 = (
        datetime.datetime(2009,4,5),
        datetime.datetime(2009,11,4)
    )
    valid_dates[2009] = valid_dates_2009
    valid_dates_2010 = (
        datetime.datetime(2010,4,4),
        datetime.datetime(2010,11,1)
    )
    valid_dates[2010] = valid_dates_2010
    valid_dates_2011 = (
        datetime.datetime(2011,3,31),
        datetime.datetime(2011,10,28)
    )
    valid_dates[2011] = valid_dates_2011
    valid_dates_2012 = (
        datetime.datetime(2012,3,28),
        datetime.datetime(2012,10,28)
    )
    valid_dates[2012] = valid_dates_2012
    valid_dates_2013 = (
        datetime.datetime(2013,3,31),
        datetime.datetime(2013,10,30)
    )
    valid_dates[2013] = valid_dates_2013
    valid_dates_2014 = (
        datetime.datetime(2014,3,22),
        datetime.datetime(2014,10,29)
    )
    valid_dates[2014] = valid_dates_2014
    valid_dates_2015 = (
        datetime.datetime(2015,4,5),
        datetime.datetime(2015,11,1)
    )
    valid_dates[2015] = valid_dates_2015
    valid_dates_2016 = (
        datetime.datetime(2016,4,3),
        datetime.datetime(2016,11,2)
    )
    valid_dates[2016] = valid_dates_2016
    valid_dates_2017 = (
        datetime.datetime(2017,4,2),
        datetime.datetime(2017,11,1)
    )
    valid_dates[2017] = valid_dates_2017
    valid_dates_2018 = (
        datetime.datetime(2018,3,29),
        datetime.datetime(2018,10,28)
    )
    valid_dates[2018] = valid_dates_2018
    valid_dates_2019 = (
        datetime.datetime(2019,3,20),
        datetime.datetime(2019,10,28)
    )
    valid_dates[2019] = valid_dates_2019
    valid_dates_2020 = (
        datetime.datetime(2020,7,23),
        datetime.datetime(2020,10,27)
    )
    valid_dates[2020] = valid_dates_2020
    valid_dates_2021 = (
        datetime.datetime(2021,4,1),
        datetime.datetime(2021,11,2)
    )
    valid_dates[2021] = valid_dates_2021
    valid_dates_2022 = (
        datetime.datetime(2022,4,7),
        datetime.datetime(2022,11,5)
    )
    valid_dates[2022] = valid_dates_2022
    valid_dates_2023 = (
        datetime.datetime(2023,3,30),
        datetime.datetime(2023,11,1)
    )
    valid_dates[2023] = valid_dates_2023
    valid_dates_2024 = (
        datetime.datetime(2024,3,20),
        datetime.datetime(2024,6,15)
    )
    valid_dates[2024] = valid_dates_2024

    for val in valid_dates.keys():
        cur.execute("INSERT INTO valid_season_dates (year, start_date, end_date) VALUES (%s, %s, %s)", (val, valid_dates[val][0], valid_dates[val][1]))

        





    


    

    




    
load_dotenv()
user= os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
con = psycopg2.connect(user=user, password=password)
cur = con.cursor()
cur.execute("CREATE TABLE valid_season_dates(year integer PRIMARY KEY, start_date date, end_date date);")
get_valid_season_dates()
con.commit()  # Commit the transaction to save the changes
cur.execute("SELECT * FROM valid_season_dates;")
print(cur.fetchall())