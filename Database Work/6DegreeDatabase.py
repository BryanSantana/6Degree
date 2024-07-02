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

def get_valid_dates (year, cursor):
    """
    Returns the valid season dates given a year, the table cursor, and the connection
    """
    cursor.execute("SELECT start_date, end_date from valid_season_dates WHERE year = %s", (year,))
    valid_dates = cursor.fetchall() 
    opening_day = valid_dates[0][0]
    last_day = valid_dates[0][1]
    return opening_day, last_day


def get_player_id(name_query, cursor, con):
    """
    Gets a player ID based on a query containing the name and the year. If multiple players with the same name arise, it'll prompt the user to make a choice.
    """
    query_as_string= str(name_query[0]) + "-" + str(name_query[1]) + "-" + str(name_query[2])
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
            cursor.execute("Select choice from tiebreaker where player_query = %s", (query_as_string,))
            index_entry = cursor.fetchone()
            if not index_entry:
                for index, entry in enumerate(player_return):
                    print("Player at index", index, " : ", entry)
                index_entry = int(input(("Type the index of the player you want: (looking for: )", query_as_string)))
                cursor.execute("Insert into tiebreaker (player_query, choice) VALUES (%s, %s)", (query_as_string, index))
                con.commit()
            else:
                index_entry = index_entry[0]
        else:
            index_entry = 0
        return player_return[index_entry]['id']


def update_daily_active_roster_to_database(team_id, date, cursor, con):
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
        player_query = (player_string, date.year, team_id)
        player_id = get_player_id(player_query, cursor, con)
        if player_id:
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

def fill_teammates_for_season(opening_day, last_day, cursor, con):
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
            update_daily_active_roster_to_database(team_id, date, cursor, con)

def populate_database ():
    load_dotenv()
    user= os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    con = psycopg2.connect(user=user, password=password)
    cur = con.cursor()
    season = 2024
    while season > 1989:
        opening_day, last_day = get_valid_dates(season, cur)
        fill_teammates_for_season(opening_day, last_day, cur, con)
        print(season,"Season Done")
        season -= 1
    con.commit()  # Commit the transaction to save the changes

populate_database()
#name_query=("Bryan Santana", 2024, 135)
#query_as_string= str(name_query[0]) + "-" + str(name_query[1]) + "-" + str(name_query[2])
#print(query_as_string)
#load_dotenv()
#user= os.getenv('POSTGRES_USER')
#password = os.getenv('POSTGRES_PASSWORD')
#con = psycopg2.connect(user=user, password=password)
#cur = con.cursor()
#cur.execute("Create table tiebreaker (player_query varchar PRIMARY KEY, choice int)")
#cur.execute("Delete from player_team_join")
#cur.execute("Delete from teammates")
#cur.execute("Delete from teams")
#cur.execute("Delete from MLB_Players")
#cur.execute("ALTER SEQUENCE teams_team_id_seq RESTART")
#con.commit()