import numpy as np
import json
from xeger import Xeger
from collections import Counter

# read json file
with open("schema.json") as json_data:
    d = json.load(json_data)

A = []
TABLE = []
TABLE_SHAPE = d['table']
#print(TABLE_SHAPE)

TABLE_ROWS = TABLE_SHAPE["rows"]
TABLE_COLUMNS = TABLE_SHAPE["columns"]

for i in range((TABLE_ROWS)):
    TABLE.append(A)

#print(TABLE)

TABLE_HEADER = d['columns']

A = []
for key in TABLE_HEADER:
    #print(key)
    A.append(key)

TABLE[0] = A
# print(TABLE)




# INTEGERS :-
int_property = d["Prop1"]
#print(int_property)

RANGE = d["Prop1"]["range"]
#print(RANGE)
DISTRIBUTION = d["Prop1"]["distribution"]

if DISTRIBUTION == "uniform":
    INTEGER = (np.random.uniform(RANGE[0], RANGE[1], d["Prop1"]["n"]))
    for i in range(len(INTEGER)):
        INTEGER[i] = int(INTEGER[i])

    INTEGER = INTEGER.tolist()


    for i in range(len(INTEGER)):
        INTEGER[i] = int(INTEGER[i])

    for i in range(TABLE_ROWS-d["Prop1"]["n"]):
        INTEGER.append('')
    print(INTEGER)



# FLOATING POINT :-

float_property = d["Prop2"]
#print(float_property)

RANGE = d["Prop2"]["range"]
#print(RANGE)
DISTRIBUTION = d["Prop2"]["distribution"]

if DISTRIBUTION == "gaussian":
    mu, sigma = 5, 0.5  # mean and standard deviation
    FLOAT = (np.random.normal(mu, sigma, d["Prop2"]["n"]))

    FLOAT = FLOAT.tolist()
    for i in range(TABLE_ROWS-d["Prop2"]["n"]):
        FLOAT.append('')
    print(FLOAT)



# STRINGS :-
x = Xeger(limit=d["Prop3"]["limit"])

STRINGS = []

for i in range(d["Prop3"]["n"]):
    STRINGS.append(x.xeger(str(d["Prop3"]["matching_regex"][0])))

for i in range(TABLE_ROWS-d["Prop3"]["n"]):
    STRINGS.append('')

print(STRINGS)



# Query :- Top k items with maximum element

k = d["Prop3"]["k"]
Counter = Counter(STRINGS)
most_occur = Counter.most_common(k)




# BOOLEAN :-

BOOLEAN = []
for i in range(d["Prop4"]["n_true"]):
    BOOLEAN.append(True)

for i in range(d["Prop4"]["n"]-d["Prop4"]["n_true"]):
    BOOLEAN.append(False)

for i in range(TABLE_ROWS-d["Prop4"]["n"]):
    BOOLEAN.append('')

print(BOOLEAN)




TABLE = []

TABLE.append(INTEGER)
TABLE.append(FLOAT)
TABLE.append(STRINGS)
TABLE.append(BOOLEAN)

print(TABLE_HEADER)
print(TABLE)        # TABLE OF VALUES

print(most_occur)