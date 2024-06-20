import statsapi;
import datetime;
import psycopg2;
import os;
from dotenv import load_dotenv;

def get_team_ids():
    """
    Get the IDs of all teams in MLB to use with the MLB-StatsAPI
    returns a set containing all IDs for the teams.
    """
    res = statsapi.get("divisions", {}, force=False)

    mlb_teams = statsapi.get('teams',{'sportIds':1,'activeStatus':'Yes','fields':'teams,name,id'})

    all_teams = mlb_teams["teams"]
    
    team_dict = {}

    for team in all_teams:
        team_dict[team['name']] = team['id']
    
    team_dict = sorted(team_dict.items(), key = lambda x:x[1])
    team_dict = dict(team_dict)
    team_ids = set(team_dict.values())
    return team_ids


def get_team_history(player_id):
    """
    Get an MLB player's team history.
    Returns a list of entries containing the team name, team ID, start date, and end date of the player playing on those teams
    """
    # Specify the endpoint and parameters
    endpoint = 'people'
    params = {'personIds': player_id, 'hydrate': 'rosterEntries,transactions'}

    # Make a GET request using statsapi.get
    response = statsapi.get(endpoint, params)

    # Extract information for each roster entry
    roster_entries = response['people'][0]['rosterEntries']
    print(roster_entries)
    team_history = []
    
    team_ids = get_team_ids()
    
    for entry in roster_entries:
        print(entry)
        if entry['team']['id'] in team_ids:
            team_entry = (
                entry['team']['name'],        # Team Name
                entry['team']['id'],          # Team ID
                entry['startDate'],           # Start Date
                entry.get('endDate', None)    # End Date (if available)
            )
            team_history.append(team_entry)

    return team_history


def get_teammates_by_team_history(team_history, teammates):
    """
    Get an MLB player's teammates given a team_history, which contains team name, team ID, start date, and end date
    Returns a set of unique player names for which the player played with in his career.
    """
    #For each team stint the player's been on
    for entry in team_history:
        team_id = entry[1]
        year, month, date = entry[2].split("-")
        start_date = datetime.datetime(int(year), int(month), int(date))
        if entry[3]:
            year, month, date = entry[3].split("-")
            end_date = datetime.datetime(int(year), int(month), int(date))
        else:
            end_date = datetime.datetime.now()
        date_range = [start_date + datetime.timedelta(days=x) for x in range((end_date-start_date).days + 1)]

        for date in date_range:
            team_roster = get_daily_active_roster_as_set(team_id, date, teammates)
            for player in team_roster:
                new = teammates[player].union(team_roster)
                teammates[player] = new
        print(teammates)
    return teammates

def get_player_id(name_query):
    return statsapi.lookup_player(name_query[0], gameType="R", season=name_query[1], sportId=1)[0]['id']

def get_daily_active_roster_as_set(team_id, date, teammates):
    team_roster = set()
    formatted_date = date.strftime("%m/%d/%Y")
    roster = (statsapi.roster(team_id, date=formatted_date, rosterType="active"))
    roster = roster.split()
    x = 2
    while x < len(roster):
        if x + 2 < len(roster) and '#' not in roster[x + 2]:
            player_string = roster[x] + " " + roster[x+1] +  " " + roster[x+2]
            x += 5
        else:
            player_string = roster[x] + " " + roster[x+1]
            x += 4
        team_roster.add(player_string)
        if player_string not in teammates.keys():
                    teammates[player_string] = set()
    return team_roster
            

def fill_teammates_for_season(opening_day, last_day, teammates, players_checked):
    date_range = [opening_day + datetime.timedelta(days = x) for x in range((last_day - opening_day).days + 1)]
    team_ids = get_team_ids()
    for date in date_range:
        for team_id in team_ids:
           print("Checking", team_id, "on ", date, "at ", datetime.datetime.now())
           daily_roster = get_daily_active_roster_as_set(team_id, date, teammates)
           for player in daily_roster:
               if player not in players_checked:
                players_checked.add(player)
                print("Checking", player, "at " , datetime.datetime.now())
                player_query = (
                   player,
                   date.year
               )
                id = get_player_id(player_query)
                team_history = get_team_history(id)
                teammates = get_teammates_by_team_history(team_history, teammates)
                print("Finished", player, "at ", datetime.datetime.now())
    return teammates, players_checked

def get_valid_season_dates():
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
#teammates = {}
#players_checked = set()
#history = get_team_history(116539)
#print(history)

#teammates,players_checked = get_teammates_by_team_history(history, teammates)
#print(teammates['David Ortiz'])
#date = datetime.datetime(2019,3,15)
#formatted_date = date.strftime("%m/%d/%Y")
#roster = (statsapi.roster(147, date = formatted_date, rosterType="active"))
#roster = roster.split()
#print(roster)

#opening_day_2024 = datetime.datetime(2024,3,28)
#last_day = datetime.datetime(2024,6,18)
#print("Start Time", datetime.datetime.now())
#teammates_2024, players_in_2024 = fill_teammates_for_season(opening_day_2024, last_day, teammates, players_checked)
#print(len(teammates_2024), len(players_in_2024))
#print("End Time", datetime.datetime.now())
#print(opening_day_2024 > last_day)
#print(last_day.year)
#print(type(last_day.year))