import pandas as pd
import random
import json
from faker import Faker
import re

start = 0
end = 97777
id_set = set()
id_map = {}
def randomizeId(id:int):
    new_id = random.randint(start, end)
    while new_id in id_set:
        # Generate again if already exists.
        new_id = random.randint(start, end)
    id_map[id] = new_id
    id_set.add(new_id)
    
name_map = {}
name_set = set()
fake = Faker()
cnt = 0

def randomize_name(name:str):
    global cnt
    newName = fake.name().split(" ")[random.randint(0, 1)]
    while newName in name_set or len(newName) == 0 or newName == " ":
        rand = random.randint(0, 3)
        if cnt > 5:
            newName = newName + chr(ord('a') + random.randint(0, 25))
            continue
        if rand == 0:
            newName = fake.name().split(" ")[random.randint(0, 1)]
        elif rand == 1:
            text = re.split(r"\\n|\ |\.|\(|\)", fake.text())
            if len(text) > 1:
                text = text[random.randint(0, len(text) - 1)]
            else:
                text = text
            newName = text.capitalize()
        elif rand == 2:
            text = re.split(r"\\n|\ |\.|\(|\)", fake.country())
            if len(text) > 1:
                text = text[random.randint(0, len(text) - 1)]
            else:
                text = text[0]
            newName = text.capitalize()
    cnt = cnt + 1
    name_map[name] = newName
    name_set.add(newName)

df_passenger = pd.read_csv("passengers.csv")
df_passenger["passengerId"] = df_passenger["passengerId"].astype("int")

# generate new ids
distinct_id = df_passenger["passengerId"].drop_duplicates()

for id in distinct_id.to_list():
    randomizeId(id)
    
keys = list(id_map.keys())
values = list(id_map.values())
df_id_map = pd.DataFrame({"old_id":keys, "new_id":values})

# Generate new names
all_names = pd.concat([df_passenger["firstName"], df_passenger["lastName"]]).drop_duplicates()
cnt = 0
for name in all_names.to_list():
    randomize_name(name)

df_passenger["newFirst"] = df_passenger["firstName"].apply(lambda x: name_map[x])
df_passenger["newLast"] = df_passenger["lastName"].apply(lambda x: name_map[x])

# Generate new csv for passengers.csv
df_passenger_new = df_passenger.merge(right = df_id_map, left_on = "passengerId", right_on = "old_id", how = "left") \
    .drop(labels = ["passengerId", "old_id", "lastName", "firstName"], axis = 1) \
    .rename(
        columns = {
            "new_id" : "passengerId",
            "newFirst" : "firstName",
            "newLast" : "lastName"
        }
    )[["passengerId", "firstName", "lastName"]]
df_passenger_new.to_csv("new_passenger.csv", index = False)
df_id_map.to_csv("id_map.csv", index = False)
     