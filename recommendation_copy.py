import requests, json, os, sys, time, re
import findspark as fs

from pyspark.mllib.recommendation import ALS
from pyspark import SparkContext
import sqlalchemy
import pandas as pd

#fs.find()
#fs.init()
sc = SparkContext()

# set file path
path_user_inventory = './data/user_inventory_sample.txt'  # your crawled user inventory txt file


def parse_raw_string(raw_string):
    user_inventory = json.loads(raw_string)
    return list(user_inventory.items())[0]


user_inventory_rdd = sc.textFile(path_user_inventory).map(parse_raw_string).zipWithIndex()


def id_index(x):
    ((user_id, lst_inventory), index) = x
    return (index, user_id)


dic_id_index = user_inventory_rdd.map(id_index).collectAsMap()


def create_tuple(x):
    ((user_id, lst_inventory), index) = x
    if lst_inventory != None:
        return (
        index, [(i.get('appid'), i.get('playtime_forever')) for i in lst_inventory if i.get('playtime_forever') > 0])
    else:
        return (index, [])


training_rdd = user_inventory_rdd.map(create_tuple).flatMapValues(lambda x: x).map(
    lambda index_appid_time: (index_appid_time[0], index_appid_time[1][0], index_appid_time[1][1]))

model = ALS.train(training_rdd, 5)

dic_recommended = {'g0': {}, 'g1': {}, 'g2': {}, 'g3': {}, 'g4': {}, 'g5': {}, 'g6': {}, 'g7': {}, 'g8': {}, 'g9': {}}
for index in dic_id_index.keys():
    try:
        lst_recommended = [i.product for i in model.recommendProducts(index, 10)]
        user_id = dic_id_index.get(index)
        rank = 0
        for app_id in lst_recommended:
            dic_recommended['g%s' % rank].update({user_id: app_id})
            rank += 1
    except:
        pass

engine = sqlalchemy.create_engine(
    'mysql+pymysql://root:<password>@localhost:3306/game_recommendation?charset=utf8mb4&local_infile=1')

df = pd.DataFrame(dic_recommended)
df.index.name = 'user_id'
df = df.reset_index()
df.to_sql('tbl_recommended_games', engine, if_exists='replace')
