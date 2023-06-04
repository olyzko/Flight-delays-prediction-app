import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import regex as re
from sklearn.model_selection import train_test_split
from sklearn.metrics import *
from xgboost import XGBClassifier
from xgboost import XGBRegressor


def encode(data):
    dep_airport_ohe = pd.get_dummies(data['DEP_AIRPORT_ID'])
    arr_airport_ohe = pd.get_dummies(data['ARR_AIRPORT_ID'])
    dep_airport_ohe = dep_airport_ohe.rename(columns=lambda x: re.sub("^", "DEP_AIRPORT_", str(x)))
    arr_airport_ohe = arr_airport_ohe.rename(columns=lambda x: re.sub("^", "ARR_AIRPORT_", str(x)))
    data = df.drop(['MONTH', 'CARRIER_NAME', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'AIRLINE_ID'], axis=1)
    data = pd.concat([data, dep_airport_ohe], axis=1)
    data = pd.concat([data, arr_airport_ohe], axis=1)

    dep_time_block = pd.get_dummies(data['DEP_TIME_BLK'])
    arr_time_block = pd.get_dummies(data['ARR_TIME_BLK'])
    dep_time_block = dep_time_block.rename(columns=lambda x: re.sub("^", "DEP_TIME_", str(x)))
    arr_time_block = arr_time_block.rename(columns=lambda x: re.sub("^", "ARR_TIME_", str(x)))
    data = pd.concat([data, dep_time_block], axis=1)
    data = pd.concat([data, arr_time_block], axis=1)
    data.drop(['DEP_AIRPORT_ID', 'ARR_AIRPORT_ID', 'DEP_TIME', 'DEP_TIME_BLK',
               'ARR_TIME', 'ARR_TIME_BLK'], axis=1, inplace=True)

    return data


df = pd.read_csv('models/2019-airline-delays-and-cancellations/flights_table.csv')
df = encode(df)

y = df['ARR_DEL15']
X = df.drop(['ARR_DELAY_NEW', 'ARR_DEL15', 'ARR_TIME_Nan'], axis=1)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

xgb_clf = XGBClassifier(eta=0.05, max_depth=6, n_estimators=400, scale_pos_weight=1)
xgb_clf.fit(X_train, y_train)

X_reg = df.loc[df.ARR_DELAY_NEW >= 15].drop(columns=['ARR_DELAY_NEW', 'ARR_DEL15', 'ARR_TIME_Nan'])
y_reg = df.loc[df.ARR_DELAY_NEW >= 15]['ARR_DELAY_NEW']
X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(X_reg, y_reg, test_size=0.25, random_state=42)

xgb_reg = XGBRegressor(n_estimators=200)
xgb_reg.fit(X_train_reg, y_train_reg)

with open('bin/xgb_clf_pkl', 'wb') as files:
    pickle.dump(xgb_clf, files)

with open('bin/xgb_reg_pkl', 'wb') as files:
    pickle.dump(xgb_reg, files)
