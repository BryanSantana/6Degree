import psycopg2;
import os;
from dotenv import load_dotenv;
def get_player_teammates(player_id):
    load_dotenv()
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    con = psycopg2.connect(user=user, password=password)
    cur = con.cursor()

    # Get the teams and dates the player has been part of
    cur.execute("SELECT team_id, start_date, end_date FROM player_team_join WHERE player_id = %s", (player_id,))
    data = cur.fetchall()

    teammate_name = set()
    for entry in data:
        team_id, start_date, end_date = entry
        # Fetch the teammates within the same team and date range
        cur.execute("""
            SELECT p.name
            FROM player_team_join ptj
            JOIN MLB_Players p ON ptj.player_id = p.player_id
            WHERE ptj.team_id = %s AND ptj.start_date <= %s AND ptj.end_date >= %s AND ptj.player_id != %s
        """, (team_id, end_date, start_date, player_id))
        teammates = cur.fetchall()
        for entry in teammates:
            teammate_name.add(entry[0])
    return teammate_name
#a_j_teammates = get_player_teammates(592450)
#ohtani_teammates = get_player_teammates(660271)
#joint_teammates = ohtani_teammates.intersection(a_j_teammates)
#print(joint_teammates)

def create_puzzle(start_player_id, end_player_id, cursor):
    start_player_teammates = get_player_teammates(start_player_id)
    end_player_teammates = get_player_teammates(end_player_id)
    start_player_name = get_player_info(start_player_id, cursor)
    end_player_name = get_player_info(end_player_id, cursor)
    layer = start_player_teammates.intersection(end_player_teammates)
    if  layer:
        start_puzzle(3,start_player_name[0], end_player_name[0], layer)


def start_puzzle(steps, start_player_name, end_player_name, layers):
    print("The start player is", start_player_name[0])
    print("The end player is", end_player_name[0])
    answer = False
    while answer == False:
        name = input("Enter the name of the player that connects them: ")
        if name in layers:
            answer = True
            print("You did it yaayyyyyyyyy")
        else:
            print ("Nope, ", name, "doesn't connect", start_player_name[0], " to ", end_player_name[0])

def get_player_info (id, cursor):
    cursor.execute("SELECT name, position, number from MLB_Players where player_id = %s", (id,))
    player = cursor.fetchall()
    return player

load_dotenv()
user= os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
con = psycopg2.connect(user=user, password=password)
cur = con.cursor()
#cur.execute("CREATE TABLE Puzzles start_id int, end_id int, separation int")
create_puzzle(545361,592450, cur)
#teammates = get_player_teammates(545361)
#print(teammates)