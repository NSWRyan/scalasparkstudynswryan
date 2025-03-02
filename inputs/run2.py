import pandas as pd
import random
import json
import re

country_map = {"zz" : "zz"}
letters_set = set()
letters_set.add("zz")
base_letter = ord('a')
def randomize(country:str) -> None:
    if country in country_map:
        return
    # Generate random letter length 2.
    random1 = random.randint(0, 25)
    random2 = random.randint(0, 25)
    
    letters = "zz"
    while letters in letters_set:
        # reroll if already exist or letter is zz.
        random1 = random.randint(0, 25)
        random2 = random.randint(0, 25)
        letters = chr(base_letter + random1) + chr(base_letter + random2)
    country_map[country] = letters
    letters_set.add(letters)
    
df_passengers_map = pd.read_csv("id_map.csv")
df_passengers_map["old_id"] = df_passengers_map["old_id"].astype("int")
df_passengers_map["new_id"] = df_passengers_map["new_id"].astype("int")

df_teleports = pd.read_csv("teleportData.csv")
df_teleports["passengerId"] = df_teleports["passengerId"].astype("int64")
df_teleports["teleportId"] = df_teleports["teleportId"].astype("int64")
df_teleports["from"] = df_teleports["from"].astype("str")
df_teleports["to"] = df_teleports["to"].astype("str")
df_teleports["date"] = pd.to_datetime(df_teleports["date"])

all_countries = pd.concat([df_teleports["from"].drop_duplicates(), df_teleports["to"].drop_duplicates()])

# Create new random countries and put it in country_map
for country in all_countries:
    randomize(country)
    
print(json.dumps(indent = 4, obj = country_map))
# map the old countries.
df_teleports["fromNew"] = df_teleports["from"].apply(lambda x: country_map[x])
df_teleports["toNew"] = df_teleports["to"].apply(lambda x: country_map[x])

# Generate new DataFrame
df_teleports_new = df_teleports.merge(
        right = df_passengers_map, 
        left_on = "passengerId", 
        right_on = "old_id", 
        how = "left", 
    ).drop(labels = ["passengerId", "old_id", "from", "to"], axis = 1) \
    .rename(columns = {
        "new_id" : "passengerId",
        "fromNew" : "from",
        "toNew" : "to",
    })[["passengerId", "teleportId", "from", "to", "date"]]
df_teleports_new.to_csv("teleportDataNew.csv", index = False)