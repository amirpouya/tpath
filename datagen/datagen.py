# import dask.dataframe as pd
import math
import numpy as np
import os
import pandas as pd
import random
import warnings

warnings.filterwarnings('ignore')
import sys

print("What the fuck", flush=True)


def interval_split(i):
    s = i[0]
    e = i[1]
    out = []
    if e - s <= 0.5: return []
    if e - s < 1: return [(int(s), int(s))]
    if e - s >= 1:
        out = interval_split((s, int(s) + 0.99)) + interval_split((int(s + 1), e))
        return out


def coalese(i):
    return (min(i)[0], max(i)[1])


num_nodes = int(sys.argv[1])
temp_res = int(sys.argv[2])  # minutes
num_rooms = 10
covid_prob = int(sys.argv[3]) / 100.0
num_room_sf = 10

num_rooms = num_rooms * num_room_sf

dirName = "newnew_tr_" + str(temp_res) + "_nn_" + str(num_nodes) + "_nr_" + str(num_rooms) + "_nc_" + str(
    int(covid_prob * 100))
print("config ", dirName, flush=True)

dataframe = pd.read_csv("bigbigfile.csv")

dataframe['UserID'] = dataframe['UserID'].apply(pd.to_numeric)
dataframe['Duration'] = dataframe['Duration'].apply(pd.to_numeric)
dataframe['EntranceGPSTime'] = dataframe['EntranceGPSTime'].apply(pd.to_numeric)
dataframe['ExitGPSTime'] = dataframe['ExitGPSTime'].apply(pd.to_numeric)
dataframe['randCol'] = [random.randint(1, num_room_sf) for k in dataframe.index]
dataframe['BeaconCellName'] = dataframe['BeaconCellName'] + '-' + dataframe['randCol'].map(str)

dataframe = dataframe.astype(
    {'UserID': 'int32', 'EntranceGPSTime': 'int32', 'ExitGPSTime': 'int32', 'Duration': 'float32'})

print(dataframe.dtypes)

dataframe = dataframe[dataframe["UserID"] <= num_nodes]

print("File Loaded", flush=True)

dataframe["EntranceGPSTime"] = dataframe["EntranceGPSTime"] + 1323539
dataframe["ExitGPSTime"] = dataframe["ExitGPSTime"] + 1323539

dataframe['Start'] = pd.to_datetime(dataframe['EntranceGPSTime'], unit='ms')
dataframe['End'] = pd.to_datetime(dataframe['ExitGPSTime'], unit='ms')
dataframe['StartF'] = dataframe.Start.apply(lambda x: (((x.hour) * 60 + (x.minute)) / temp_res))
dataframe['EndF'] = dataframe.End.apply(lambda x: (((x.hour) * 60 + (x.minute)) / temp_res))
dataframe['StartI'] = dataframe.Start.apply(lambda x: math.floor(((x.hour) * 60 + (x.minute)) / temp_res))
dataframe['EndI'] = dataframe.End.apply(lambda x: math.floor(((x.hour) * 60 + (x.minute)) / temp_res))
dataframe['Interval'] = list(zip((dataframe.StartI), (dataframe.EndI)))
dataframe['IntervalF'] = list(zip((dataframe.StartF), (dataframe.EndF)))
dataframe['Interval'] = dataframe.IntervalF.apply(lambda x: (interval_split(x)))
split_df = dataframe[dataframe["Interval"].map(len) > 0]  # .explode('Interval')
split_df["Interval"] = split_df["Interval"].apply(lambda x: (coalese(x)))
split_df["Duration"] = split_df["Interval"].apply(lambda x: x[1] - x[0] + 1)
df = split_df

max_t = df['EndI'].max()

grouper = df.set_index("Start").groupby(["Interval", 'BeaconCellName'])
result = grouper['UserID'].count().unstack("BeaconCellName").mean()
a = result.sort_values(ascending=False).reset_index()
a['id'] = a.groupby(['BeaconCellName']).ngroup()
a['id'] = a['id'] + 1000000
bus = a.iloc[:num_rooms, :].rename(columns={'BeaconCellName': 'loc', 'id': 'dst'})[['loc', 'dst']]
loc = a.iloc[num_rooms + 1:, :].rename(columns={'BeaconCellName': 'loc', 'id': 'dst'})[['loc', 'dst']]

df = df[['UserID', 'BeaconCellName', 'Interval', 'HealthStatus']]
df['test'] = np.random.randint(0, 100, df.shape[0])

df['HealthStatus'] = df['HealthStatus'].apply(lambda x: 'low' if (x == 'HEALTHY') else 'high')
df = df.rename(columns={'UserID': 'src', 'HealthStatus': 'risk', 'BeaconCellName': 'loc'})

df_bus_edge = df.join(bus.set_index('loc'), on='loc').dropna().reset_index(level=-1, drop=True).reset_index().drop(
    ["index"], axis=1)

# df_bus_edge['label']= "visits"
# df_bus_edge['prop1'] = ""
# df_bus_edge['start'] = df_bus_edge['Interval'].map(lambda x: x[0]+1)
# df_bus_edge['end'] = df_bus_edge['Interval'].map(lambda x: x[1]+1)

# dff_bus = df_bus_edge[['src','dst','label','prop1','start','end']]


# # df = df[['UserID','BeaconCellName','Interval','HealthStatus']]
# # df['test'] = np.random.randint(0, 100, df.shape[0])

# # df['HealthStatus'] = df['HealthStatus'].apply(lambda x:'low' if (x == 'HEALTHY') else 'high')
# df = df.rename(columns={'UserID': 'src', 'HealthStatus': 'risk','BeaconCellName':'loc'})
# df_bus_edge = df.join(bus.set_index('loc'), on='loc').dropna().reset_index(level=-1, drop=True).reset_index().drop(["index"],axis=1)

# df_bus_edge['label']= "visits"
# df_bus_edge['prop1'] = ""
# df_bus_edge['start'] = df_bus_edge['Interval'].map(lambda x: x[0]+1)
# df_bus_edge['end'] = df_bus_edge['Interval'].map(lambda x: x[1]+1)

# dff_bus = df_bus_edge[['src','dst','label','prop1','start','end']]


# df_loc = df.join(loc.set_index('loc'), on='loc').dropna().reset_index(level=-1, drop=True).reset_index().drop(["index","dst"],axis=1)[['src','loc','Interval']]
# df_loc2 = df_loc.rename(columns={'src':'dst','Interval':'Interval2'})

# df_loc3 = df_loc.join(df_loc2.set_index('loc'), on='loc').dropna()
# df_loc4 = df_loc3[df_loc3['Interval'] == df_loc3['Interval2']]
# df_loc5 = df_loc4[df_loc4['src']!= df_loc4['dst'] ]

# df_meet =  df_loc5


df_bus_edge['label'] = "visits"
df_bus_edge['prop1'] = ""
df_bus_edge['start'] = df_bus_edge['Interval'].map(lambda x: x[0] + 1)
df_bus_edge['end'] = df_bus_edge['Interval'].map(lambda x: x[1] + 1)

dff_bus = df_bus_edge[['src', 'dst', 'label', 'prop1', 'start', 'end']]

df_loc = \
df.join(loc.set_index('loc'), on='loc').dropna().reset_index(level=-1, drop=True).reset_index().drop(["index", "dst"],
                                                                                                     axis=1)[
    ['src', 'loc', 'Interval']]
# df_loc2 = df_loc.rename(columns={'src':'dst','Interval':'Interval2'})

# df_loc3 = df_loc.join(df_loc2.set_index('loc'), on='loc').dropna()
# df_loc4 = df_loc3[df_loc3['Interval'] == df_loc3['Interval2']]
# df_loc5 = df_loc4[df_loc4['src']!= df_loc4['dst'] ]

# df_meet =  df_loc5
# df_loc3 = df_loc.join(df_loc2.set_index('loc'), on='loc').dropna()

df_meet = df_loc.merge(df_loc, left_on=['loc', 'Interval'], right_on=['loc', 'Interval']).rename(
    columns={'src_x': 'src', 'src_y': 'dst'})
# df_loc4 = df_loc3[df_loc3['Interval'] == df_loc3['Interval2']]
df_meet = df_meet[df_meet['src'] != df_meet['dst']]

df_meet['label'] = "meets"
df_meet['prop1'] = ""
df_meet['start'] = df_meet['Interval'].map(lambda x: x[0] + 1)
df_meet['end'] = df_meet['Interval'].map(lambda x: x[1] + 1)

dff_meet = df_meet[['src', 'dst', 'label', 'prop1', 'start', 'end']]

df_meet['dst'] = df_meet['dst'].apply(lambda x: int(x))

edges = pd.concat([dff_meet, dff_bus]).sort_values(by=["src", "dst", "start", "end"])
edges["eid"] = edges.groupby(["src", "dst"]).ngroup()
edges = edges[['eid', 'src', 'dst', 'label', 'prop1', 'start', 'end']]
edges["dst"] = edges["dst"].astype('int32')

print("df_meet size:", len(dff_meet), flush=True)
print("df_bus size:", len(dff_bus), flush=True)

print("Edges Created")

pos_list = dataframe["UserID"].drop_duplicates()

# df['test'] = df['test'].apply(lambda x:'pos' if (x > 95) else 'neg')
pos_list = list(pos_list.sample(frac=covid_prob, replace=True, random_state=1))

df['label'] = 'person'
df['prop1'] = ''
df['start'] = df['Interval'].map(lambda x: x[0] + 1)
df['end'] = df['Interval'].map(lambda x: x[1] + 1)

df = df.rename(columns={'src': 'nid', 'risk': 'prop2', 'test': 'prop3'})
dff = df[["nid", "label", "prop2", "prop3", "start", "end"]]

df_b = bus
df_b['label'] = 'room'
df_b['prop1'] = ''  # df['loc']
df_b['prop2'] = ''
df_b['prop3'] = ''
df_b['nid'] = df_b['dst']
df_b['start'] = 1
df_b['end'] = 47

dff2 = df_b[["nid", "label", "prop2", "prop3", "start", "end"]]
nodes = pd.concat([dff, dff2]).sort_values(by=["nid", "start", "end"])
aaaa = nodes[nodes["nid"].isin(pos_list)]
aaaa["start_i"] = np.random.randint(1, max_t, size=len(aaaa))
aaaa["prop1"] = "pos"
aaaa = aaaa[['nid', 'start_i', 'prop1']].drop_duplicates()
# aaaa = aaaa.rename(columns={'start':'start_i'})
nnnn = nodes.join(aaaa.set_index(['nid']), on=['nid'])
nnnn.fillna({'start_i': 0, 'prop1': 'neg'}, inplace=True)
nnnn = nnnn.reset_index()
# random.randint(1, max_t)
nnnn[nnnn["start_i"] > nnnn["start"]]["prop1"] = "pos"
# nodes = nnnn
nnnn[nnnn.prop1 == "neg"]
nodes = nnnn[["nid", "label", "prop1", "prop2", "prop3", "start", "end"]]
nodes["prop3"] = ''
nodes = nodes.drop_duplicates();
nodes["nid-x"] = nodes["nid"].shift().fillna(0).astype('int32')
nodes["start-x"] = (nodes["end"].shift().fillna(0) + 1)
nodes["start-x"] = nodes["start-x"].astype('int32')
nodes["cumsum"] = (nodes["start"] != nodes["start-x"]).cumsum()
nn = nodes.groupby(["nid", "label", "prop1", "prop2", "prop3", "cumsum"]).agg(
    {'start': 'min', 'end': 'max'}).reset_index()
nodes = nn

print("Saving", flush=True)

directory = dirName
# Parent Directory path
parent_dir = "./"
path = os.path.join(parent_dir, directory)
try:
    os.mkdir(path)
except:
    pass
nodes.to_csv(path + "/node.csv", index=False, header=False)
edges.to_csv(path + "/edge.csv", index=False, header=False)

print("#edges:", len(edges), "#nodes:", len(nodes), flush=True)
