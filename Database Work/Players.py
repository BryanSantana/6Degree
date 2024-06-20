import statsapi

res = statsapi.get("divisions", {}, force=False)

mlb_teams = statsapi.get('teams',{'sportIds':1,'activeStatus':'Yes','fields':'teams,name,id'})

all_teams = mlb_teams["teams"]

for team in all_teams:
    print(statsapi.roster(team["id"]))
    
