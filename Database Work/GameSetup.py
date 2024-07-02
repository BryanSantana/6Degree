import psycopg2;
import os;
from dotenv import load_dotenv;
def get_player_teammates(player_id):
    load_dotenv()
    user= os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    con = psycopg2.connect(user=user, password=password)
    cur = con.cursor()
    cur.execute("select * from player_team_join where player_id = %s", (player_id,))
    data = cur.fetchall()
    teammates = set()
    for entry in data:
        cur.execute("""
            SELECT player_id
            FROM player_team_join
            WHERE team_id = %s AND start_date >= %s AND end_date <= %s
        """, (entry[1], entry[2], entry[3]))
        result = cur.fetchall()  # To fetch the results of the query if needed
        for row in result:
            cur.execute("SELECT name FROM MLB_Players WHERE player_id = %s", (row))  # Printing the player_id
            teammate = cur.fetchone()[0]
            teammates.add(teammate)
    return teammates

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
            answer = True9
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
create_puzzle(545361,592450, cur)