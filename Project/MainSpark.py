from pyspark import SparkConf
import csv
import numpy as np
import json
from xeger import Xeger
from collections import Counter
from pyspark import SparkContext


conf = SparkConf().setAppName("DataGenerationApp")
sc = SparkContext(conf=conf)


# USING JSON
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

RANGE = d["Prop1"]["range"]

DISTRIBUTION = d["Prop1"]["distribution"]


def int_gen(seed, nb):
    np.random.seed(seed)
    y = []
    if DISTRIBUTION == "uniform":
        y = np.random.uniform(RANGE[0], RANGE[1], nb)

    for o in range(len(y)):
        y[o] = int(y[o])
    return y

INTEGER = []

n_exec = d["Prop1"]["exec"]
num = range(n_exec)
data_split = int(d["Prop1"]["n"]/n_exec)



rdd = sc.parallelize(num)
INTEGER = rdd.flatMap(lambda x:int_gen(x,data_split)).collect()

for r in range(TABLE_ROWS-d["Prop1"]["n"]):
    INTEGER.append('')

print(INTEGER)
print(len(INTEGER))



# FLOATING POINT :-
DISTRIBUTION = d["Prop2"]["distribution"]
mu = d["Prop2"]["mean"]
sigma = d["Prop2"]["stddev"]


def float_gen(seed, nb):
    np.random.seed(seed)
    y = []
    if DISTRIBUTION == "normal":
        y = np.random.normal(mu, sigma, nb)
    return y


FLOAT = []

n_exec = d["Prop2"]["exec"]
num = range(n_exec)
data_split = int(d["Prop2"]["n"]/n_exec)

rdd = sc.parallelize(num)
FLOAT = rdd.flatMap(lambda x:float_gen(x, data_split)).collect()

for r in range(TABLE_ROWS-d["Prop2"]["n"]):
    FLOAT.append('')

print(FLOAT)
print(len(FLOAT))



# STRINGS :-
x = Xeger(limit=d["Prop3"]["limit"])

STRINGS = []

def str_gen(m):
    temp = []
    for j in range(m):
        temp.append(x.xeger(str(d["Prop3"]["matching_regex"][0])))    
    return temp

M = d["Prop3"]["exec"]    # number of executors
m = int(d["Prop3"]["n"]/M)     # data ratio split among each executor

NUM = [m]*M
rdd = sc.parallelize(NUM)
#print(rdd)

STRINGS = rdd.flatMap(lambda x: str_gen(x)).collect()
le = len(STRINGS)

for i in range(TABLE_ROWS-le):
    STRINGS.append('')

print(STRINGS)
print(len(STRINGS))


# high level queries

# Query :- Top k items with maximum element
k = d["Prop3"]["k"]
Counter = Counter(STRINGS)
most_occur = Counter.most_common(k)
print(most_occur)





# BOOLEAN :-

M = d["Prop4"]["exec"] # Number of executors

def bool_gen(m,val):
    temp = []
    for j in range(m):
        temp.append(val)
    return temp

n_exec_true = int((d["Prop4"]["n_true"]/d["Prop4"]["n"]) * M)    # executors for true generation
n_exec_false = M-n_exec_true      # executors for false generation


BOOLEAN = []
# TRUE SPLIT
m_true = int(d["Prop4"]["n_true"]/n_exec_true)
data_split = [m_true]*n_exec_true

rdd = sc.parallelize(data_split)
BOOLEAN = rdd.flatMap(lambda x: bool_gen(x,True)).collect()


# FALSE SPLIT
m_false = int((d["Prop4"]["n"]-d["Prop4"]["n_true"])/n_exec_false)
data_split = [m_false]*n_exec_false

rdd = sc.parallelize(data_split)
BOOLEAN = BOOLEAN + rdd.flatMap(lambda x:bool_gen(x,False)).collect()

for i in range(TABLE_ROWS-d["Prop4"]["n"]):
    BOOLEAN.append('')


print(BOOLEAN)
print(len(BOOLEAN))


TABLE = []
TABLE.append(INTEGER)
TABLE.append(FLOAT)
TABLE.append(STRINGS)
TABLE.append(BOOLEAN)

print(TABLE)


with open("out.csv", "w") as output:
    writer = csv.writer(output, lineterminator='\n')
    writer.writerows(TABLE)