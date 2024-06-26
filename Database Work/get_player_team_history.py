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


def get_teammates_by_team_history(team_history, teammates, cursor):
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
                new_teammates = team_roster.difference(teammates[player])
                update_player_teammates(date, player, new_teammates, cursor)
                new = teammates[player].union(team_roster)
                teammates[player] = new
    return teammates

def update_player_teammates(date, player_name, teammates_to_add, cursor):
    player_query = (player_name, date.year)
    player_id = get_player_id(player_query)
    for teammate in teammates_to_add:
        player_query = (teammate, date.year)
        teammate_id = get_player_id(player_query)
        cursor.execute ("INSERT INTO Teammates (player_id, teammate_id) VALUES (%s, %s)", (player_id, teammate_id))
        con.commit()


def get_player_id(name_query):
    player_return = statsapi.lookup_player(name_query[0], gameType="R", season=name_query[1], sportId=1)
    #If only one unique player is returned from the query
    if len(player_return) == 1:
        return player_return[0]['id']
    #If multiple players are returned by the query, ie same name, have the user manually select which one they want
    else:
        for index, entry in enumerate(player_return):
            print("Player at index", index, " : ", entry)
        index_entry = int(input(("Type the index of the player you want:")))
        return player_return[index_entry]['id']


def get_daily_active_roster_as_set(team_id, date, teammates, cursor):
    team_roster = set()
    formatted_date = date.strftime("%m/%d/%Y")
    roster = (statsapi.roster(team_id, date=formatted_date, rosterType="active"))
    roster = roster.split()
    team_string = team_id + "-" + date.year()

    cursor.execute("INSERT INTO Teams (team_name) VALUES (%s) ON CONFLICT (team_name) DO NOTHING RETURNING team_id;", (team_string,))
    generated_team_id = cursor.fetchone()
    # If no team_id was returned, select the existing team_id
    if generated_team_id is None:
        cursor.execute("SELECT team_id FROM Teams WHERE team_name = %s;", (team_string,))
        generated_team_id = cursor.fetchone()[0]
    else:
        generated_team_id = generated_team_id[0]
    print("Added", team_string, "to the Teams database")
    con.commit()

    x = 2
    while x < len(roster):
        if x + 2 < len(roster) and '#' not in roster[x + 2]:
            player_string = roster[x] + " " + roster[x+1] +  " " + roster[x+2]
            player_position = roster[x - 1]
            player_number = roster[x-2]
            x += 5
        else:
            player_string = roster[x] + " " + roster[x+1]
            player_position = roster[x - 1]
            player_number = roster[x-2]
            x += 4
        team_roster.add(player_string)
        if player_string not in teammates.keys():
                    teammates[player_string] = set()
                    player_query = (player_string, date.year)
                    player_id = get_player_id(player_query)
                    cursor.execute("INSERT INTO MLB_Players (player_id, name, position, number) VALUES (%s, %s, %s, %s)", (player_id, player_string, player_position, player_number))
                    con.commit()
                    cur.execute("INSERT INTO Player_Team_Join (player_id, team_id, start_date, end_date) VALUES (%s, %s, %s, %s) ON CONFLICT (player_id, team_id) DO UPDATE SET end_date = EXCLUDED.end_date;", (player_id, team_id, date, date))
                    print("Added", player_id, player_string, "played on ", team_id, "on", date, "to the database")
                    print("Added", player_id, player_string, player_position, player_number, "to the database")
    return team_roster
            

def fill_teammates_for_season(opening_day, last_day, teammates, players_checked, cursor):
    date_range = [opening_day + datetime.timedelta(days = x) for x in range((last_day - opening_day).days + 1)]
    team_ids = get_team_ids()
    for date in date_range:
        for team_id in team_ids:
           print("Checking", team_id, "on ", date, "at ", datetime.datetime.now())
           daily_roster = get_daily_active_roster_as_set(team_id, date, teammates, cursor)
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
                teammates = get_teammates_by_team_history(team_history, teammates, cursor)
                print("Finished", player, "at ", datetime.datetime.now())
    return teammates, players_checked

def populate_database ():
    load_dotenv()
    user= os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    con = psycopg2.connect(user=user, password=password)
    cur = con.cursor()
    season = 2024
    teammates = {}
    players_checked = set()
    while season > 1989:
        cur.execute("SELECT start_date,end_date from valid_season_dates WHERE year = %s", (season,))
        valid_dates = cur.fetchall() 
        opening_day = valid_dates[0][0]
        last_day = valid_dates[0][1]
        fill_teammates_for_season(opening_day, last_day,teammates, players_checked, cur)
        season -= 1
    con.commit()  # Commit the transaction to save the changes\

populate_database()