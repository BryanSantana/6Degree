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
    team_history = []
    
    team_ids = get_team_ids()

    for entry in roster_entries:
        if entry['team']['id'] in team_ids and entry['isActive'] == True:
            team_entry = (
                entry['team']['name'],        # Team Name
                entry['team']['id'],          # Team ID
                entry['startDate'],           # Start Date
                entry.get('endDate', None)    # End Date (if available)
            )
            team_history.append(team_entry)

    return team_history


def get_teammates_by_team_history(team_history, teammates, cursor, con):
    """
    Get an MLB player's teammates given a team_history, which contains team name, team ID, start date, and end date
    Returns a set of unique player names for which the player played with in his career.
    """
    #For each team stint the player's been on
    for entry in team_history:
        team_id = entry[1]
        #Extract the start date on the team
        year, month, day = entry[2].split("-")
        start_date = datetime.date(int(year), int(month), int(day))
        #Make sure start date is a valid in-season date, otherwise format it to be that year's opening day
        cursor.execute("SELECT start_date from valid_season_dates WHERE year = %s", (year,))
        opening_day = cursor.fetchone()[0] 
        if start_date < opening_day:
            start_date = opening_day
        if entry[3]:
            #Extract the end date on the team if it exists (otherwise the player is still on the team)
            year, month, date = entry[3].split("-")
            end_date = datetime.date(int(year), int(month), int(day))
            #Make sure the end date is a valid last_day, otherwise format to be that year's last day
            opening_day, last_day = get_valid_dates(year, cursor)
            #Example, november or december would get rounded back to the last_day of the season
            if end_date > last_day:
                end_date = last_day
            #Example, if a player was cut in february but the season starts in april, round it back to the last_day of the last season
            elif end_date < opening_day:
              opening_day, last_day = get_valid_dates(year - 1, cursor)
              end_date = last_day    
        else:
            end_date = datetime.date.today()
        
        #Create the date range to iterate over
        date_range = [start_date + datetime.timedelta(days=x) for x in range((end_date-start_date).days + 1)]

        for date in date_range:
            first_valid, last_valid = get_valid_dates(date.year,cursor)
            #This will prevent awry entries like a player playing on a team before or after the season ended
            if date > first_valid and date < last_valid:
                team_roster = get_daily_active_roster_as_set(team_id, date, teammates, cursor, con)
                for player in team_roster:
                    #New teammates will be those in team_roster that aren't already in teammates[player]
                    new_teammates = team_roster.difference(teammates[player])
                    update_player_teammates(date, player, new_teammates, cursor, con)
                    new = teammates[player].union(team_roster)
                    teammates[player] = new
    return teammates

def get_valid_dates (year, cursor):
    """
    Returns the valid season dates given a year, the table cursor, and the connection
    """
    cursor.execute("SELECT start_date, end_date from valid_season_dates WHERE year = %s", (year,))
    valid_dates = cursor.fetchall() 
    opening_day = valid_dates[0][0]
    last_day = valid_dates[0][1]
    return opening_day, last_day

def update_player_teammates(date, player_name, teammates_to_add, cursor, con):
    """
    Updates the player to teammate relationship in the Teammates database. 
    """
    player_query = (player_name, date.year)
    player_id = get_player_id(player_query)
    if player_id:
        for teammate in teammates_to_add:
            player_query = (teammate, date.year)
            teammate_id = get_player_id(player_query)
            if teammate_id:
                cursor.execute ("INSERT INTO Teammates (player_id, teammate_id) VALUES (%s, %s)", (player_id, teammate_id))
                con.commit()


def get_player_id(name_query):
    """
    Gets a player ID based on a query containing the name and the year. If multiple players with the same name arise, it'll prompt the user to make a choice.
    """
    player_return = statsapi.lookup_player(name_query[0], gameType="R", season=name_query[1], sportId=1)
    if len(player_return) == 0:
        print("Failed to find", name_query)
        return None
    #If only one unique player is returned from the query
    elif len(player_return) == 1:
        return player_return[0]['id']
    #If multiple players are returned by the query, ie same name, have the user manually select which one they want
    else:
        if player_return[0]['fullName'] == player_return[1]['fullName']:
            for index, entry in enumerate(player_return):
                print("Player at index", index, " : ", entry)
            index_entry = int(input(("Type the index of the player you want: (looking for: )", name_query)))
        else:
            index_entry = 0
        return player_return[index_entry]['id']


def get_daily_active_roster_as_set(team_id, date, teammates, cursor, con):
    """
    Pass in a team_id, date, the teammates dictionary, the cursor, and the connection to the database, and this function will
    update the new players into the Players table and return the daily active roster for processing in another function 
    """
    team_roster = set()
    formatted_date = date.strftime("%m/%d/%Y")
    roster = (statsapi.roster(team_id, date=formatted_date, rosterType="active"))
    roster = roster.split()
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
        if player_string not in teammates.keys():
                    player_query = (player_string, date.year)
                    player_id = get_player_id(player_query)
                    if player_id:
                        teammates[player_string] = set()
                        team_roster.add(player_string)
                        cursor.execute("INSERT INTO MLB_Players (player_id, name, position, number) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (player_id, player_string, player_position, player_number))
                        con.commit()
                        print("Added", player_id, player_string, player_position, player_number, "to the database")
                        table_team_id = add_team_to_teams_table(team_id, date, cursor, con)
                        cursor.execute("""
                        INSERT INTO Player_Team_Join (player_id, team_id, start_date, end_date) 
                        VALUES (%s, %s, %s, %s) 
                        ON CONFLICT (player_id, team_id) 
                        DO UPDATE SET 
                        end_date = CASE 
                        WHEN EXCLUDED.end_date > Player_Team_Join.end_date THEN EXCLUDED.end_date 
                         ELSE Player_Team_Join.end_date 
                        END,
                        start_date = CASE 
                        WHEN EXCLUDED.start_date < Player_Team_Join.start_date THEN EXCLUDED.start_date 
                        ELSE Player_Team_Join.start_date 
                        END;
                        """, (player_id, table_team_id, date, date))
                        con.commit()
                        print("Added", player_id, player_string, "played on ", table_team_id, "on", date, "to the database")
    return team_roster

def add_team_to_teams_table(team_id, date, cursor, con):
    """
    Adds a team to the team table by constructing the team_name
    """
    team_string = str(team_id) + "-" + str(date.year)
    cursor.execute("INSERT INTO Teams (team_name) VALUES (%s) ON CONFLICT (team_name) DO NOTHING RETURNING team_id;", (team_string,))
    con.commit()
    generated_team_id = cursor.fetchone()
    # If no team_id was returned, select the existing team_id
    if generated_team_id is None:
        cursor.execute("SELECT team_id FROM Teams WHERE team_name = %s;", (team_string,))
        generated_team_id = cursor.fetchone()[0]
    else:
        generated_team_id = generated_team_id[0]
    return generated_team_id

def fill_teammates_for_season(opening_day, last_day, teammates, players_checked, cursor, con):
    """
    Given valid season date parameters (parsed from valid_season_dates table) this function will fill in all 4 tables
    for a given season by calling other functions, it will also return a teammate dictionary as well as a set of players_checked 
    """
    date_range = [opening_day + datetime.timedelta(days = x) for x in range((last_day - opening_day).days + 1)]
    team_ids = get_team_ids()
    for date in date_range:
        for team_id in team_ids:
            generated_team_id = add_team_to_teams_table(team_id, date, cursor, con)
            print("Added", generated_team_id, "to the Teams database")
            con.commit()
            print("Checking", team_id, "on ", date, "at ", datetime.datetime.now())
            daily_roster = get_daily_active_roster_as_set(team_id, date, teammates, cursor, con)
            for player in daily_roster:
                if player not in players_checked:
                    players_checked.add(player)
                    print("Checking", player, "at " , datetime.datetime.now())
                    player_query = (
                    player,
                    date.year
                )
                    player_id = get_player_id(player_query)
                    if player_id:
                        cursor.execute("""
                        INSERT INTO Player_Team_Join (player_id, team_id, start_date, end_date) 
                        VALUES (%s, %s, %s, %s) 
                        ON CONFLICT (player_id, team_id) 
                        DO UPDATE SET 
                        end_date = CASE 
                        WHEN EXCLUDED.end_date > Player_Team_Join.end_date THEN EXCLUDED.end_date 
                         ELSE Player_Team_Join.end_date 
                        END,
                        start_date = CASE 
                        WHEN EXCLUDED.start_date < Player_Team_Join.start_date THEN EXCLUDED.start_date 
                        ELSE Player_Team_Join.start_date 
                        END;
                        """, (player_id, generated_team_id, date, date))
                        con.commit()
                        print("Added", player_id, player, "played on ", generated_team_id, "on", date, "to the database")
                        team_history = get_team_history(player_id)
                        teammates = get_teammates_by_team_history(team_history, teammates, cursor, con)
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
        opening_day, last_day = get_valid_dates(season, cur)
        fill_teammates_for_season(opening_day, last_day,teammates, players_checked, cur, con)
        print(season,"Season Done")
        season -= 1
    con.commit()  # Commit the transaction to save the changes

#populate_database()
#load_dotenv()
#user= os.getenv('POSTGRES_USER')
#password = os.getenv('POSTGRES_PASSWORD')
#con = psycopg2.connect(user=user, password=password)
#cur = con.cursor()
#cur.execute("Delete from player_team_join")
#cur.execute("Delete from teammates")
#cur.execute("Delete from teams")
#cur.execute("Delete from MLB_Players")
#cur.execute("ALTER SEQUENCE teams_team_id_seq RESTART")
#con.commit()