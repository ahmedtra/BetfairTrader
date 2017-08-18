import os

from betfair.constants import PriceData, MarketProjection
from betfair.models import PriceProjection, MarketFilter
from datetime import datetime
from time import sleep
from os import listdir
from os.path import isfile, join
import pandas as pd
import json
from common import safe_move, get_hist_files

from data.cassandra_wrapper.access import CassTradesRepository
from data.sql_wrapper.query import DBQuery


class Recorder():
    def __init__(self):
        dir_origin, dir_completed = get_hist_files()
        self.path = dir_origin
        self.path_completed = dir_completed
        self.cass_repository = CassTradesRepository()
        self.query_secdb = DBQuery()

    def read_files(self):

        files = [f for f in listdir(self.path) if isfile(join(self.path, f))]

        for filename in files:
            filepath = join(self.path, filename)

            reader = pd.read_csv(filepath, chunksize=10e3, header=0)

            for rows in reader:

                data = Recorder.create_data_frame(rows)

                self.record_trade(data)

            file_completed = join(self.path_completed, filename)

            safe_move(filepath, file_completed)

    def record_trade(self, data):
        data_list = data.T.to_dict().values()
        self.cass_repository.save_async(data_list)

    @staticmethod
    def create_data_frame(df):

        df_loop = df.copy()
        df_loop = df_loop.dropna()
        df_loop = df_loop[df_loop['SPORTS_ID'] == 1]
        df_loop = df_loop[df_loop['FULL_DESCRIPTION'].str.contains("Metalosport") == False]
        df_loop = df_loop[df_loop['FULL_DESCRIPTION'].str.contains("Gremio Nov") == False]

        df_loop['SETTLED_DATE'] = pd.to_datetime(df_loop['SETTLED_DATE'], format='%d-%m-%Y %H:%M:%S',
                                                 errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
        df_loop['SCHEDULED_OFF'] = pd.to_datetime(df_loop['SCHEDULED_OFF'], format='%d-%m-%Y %H:%M',
                                                  errors='coerce').dt.strftime("%Y-%m-%d %H:%M")
        df_loop['DT ACTUAL_OFF'] = pd.to_datetime(df_loop['DT ACTUAL_OFF'], format='%d-%m-%Y %H:%M:%S',
                                                  errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
        df_loop['LATEST_TAKEN'] = pd.to_datetime(df_loop['LATEST_TAKEN'], format='%d-%m-%Y %H:%M:%S',
                                                 errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")
        df_loop['FIRST_TAKEN'] = pd.to_datetime(df_loop['FIRST_TAKEN'], format='%d-%m-%Y %H:%M:%S',
                                                errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")

        # df_full_desc = df_loop['FULL_DESCRIPTION'].copy()
        df_final_desc = pd.DataFrame(
            columns=['COMPETITION_TYPE', 'COMPETITION', 'FIXTURES', 'EVENT_NAME', 'MARKET_TYPE'],
            index=df_loop.index)

        for i in df_final_desc.index:
            df_final_desc.loc[i] = Recorder.split_description(df_loop['FULL_DESCRIPTION'].loc[i])

        df_loop = pd.concat([df_loop, df_final_desc], axis=1)
        df_loop = df_loop[['SPORTS_ID', 'EVENT_ID', 'SETTLED_DATE', 'FULL_DESCRIPTION', 'SCHEDULED_OFF', 'EVENT', \
                           'DT ACTUAL_OFF', 'SELECTION_ID', 'SELECTION', 'ODDS', 'NUMBER_BETS', 'VOLUME_MATCHED', \
                           'LATEST_TAKEN', 'FIRST_TAKEN', 'WIN_FLAG', 'IN_PLAY', 'COMPETITION_TYPE', 'COMPETITION', \
                           'FIXTURES', 'EVENT_NAME', 'MARKET_TYPE']]


        return df_loop

    @staticmethod
    def split_description(desc):

        list_aux_split = desc.split('/')
        list_aux_split = [item.strip() for item in list_aux_split]
        n_cols = len(list_aux_split)

        res = ['', '', '', '', '']
        # 'COMPETITION_TYPE', 'COMPETITION', 'FIXTURES', 'EVENT_NAME', 'MARKET_TYPE'
        if n_cols == 1:
            res = [list_aux_split[0], '', '', '', '']

        elif n_cols == 2:
            if ' v ' in list_aux_split[0]:
                res = ['', '', '', list_aux_split[0], '']
            else:
                res = [list_aux_split[0], '', '', '', '']
            beg_str = list_aux_split[1][:2]
            try:
                int(beg_str)
                res[2] = list_aux_split[1]
            except ValueError:
                res[4] = list_aux_split[1]

        elif n_cols == 3:
            res = [list_aux_split[0], '', '', '', '']
            if 'Fixtures' in list_aux_split[1]:
                res[2] = list_aux_split[1]
            if ' v ' in list_aux_split[2]:
                res[3] = list_aux_split[2]

        elif n_cols == 4:
            res = [list_aux_split[0], '', '', '', '']
            if 'Fixtures' in list_aux_split[1]:
                res[2] = list_aux_split[1]
            else:
                res[1] = list_aux_split[1]
            if ' v ' in list_aux_split[2]:
                res[3] = list_aux_split[2]
            else:
                res[2] = list_aux_split[2]

            res[3] = list_aux_split[3]


        elif n_cols == 5:
            if 'Segunda' in list_aux_split[1]:
                res = [list_aux_split[0], list_aux_split[1], list_aux_split[3], \
                       list_aux_split[4], '']
            elif 'A League 20' in list_aux_split[1]:
                res = [list_aux_split[0], list_aux_split[1], list_aux_split[3], \
                       list_aux_split[4], '']
            elif 'Brazilian Pernambucano' in list_aux_split[2]:
                res = [list_aux_split[0], list_aux_split[1], list_aux_split[3], \
                       list_aux_split[4], '']
            else:
                res = [list_aux_split[0], list_aux_split[1], list_aux_split[2], \
                       list_aux_split[3], list_aux_split[4]]

        return res



