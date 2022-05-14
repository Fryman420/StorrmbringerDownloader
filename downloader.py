import numpy as np
import pandas as pd
import os
import logging
from logging import StreamHandler, Formatter
import sys
from datetime import datetime as dt

from meta import *

logging.basicConfig(filename=f'logs/Session {str(dt.now())}.log', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)


class downloader:

    def __init__(self, name_of_database: str) -> None:
        self.name = name_of_database
        self.plasmafn = self.name + "_plasma.txt"
        self.magfn = self.name + "_mag.txt"
        if not self.if_db_exists(flag = "plasma"):
            self.create_db(flag = "plasma")

        if not self.if_db_exists(flag = "mag"):
            self.create_db(flag = "mag")

    def if_db_exists(self, flag = "plasma") -> bool:
        if flag == "plasma":
            return os.path.exists(self.plasmafn)
        elif flag == "mag":
            return os.path.exists(self.magfn)
    
    """
    def get_current_df(self) -> pd.DataFrame:
        df = pd.DataFrame()

        df_mag = pd.read_json(link_mag)[1:].reset_index().drop(columns = "index")
        df_mag.columns = ["time_tag","bx_gsm","by_gsm","bz_gsm","lon_gsm","lat_gsm","bt"]
        df[["time_tag","bx_gsm","by_gsm","bz_gsm","lon_gsm","lat_gsm","bt"]] = df_mag[["time_tag","bx_gsm","by_gsm","bz_gsm","lon_gsm","lat_gsm","bt"]]

        df_plasma = pd.read_json(link_plasma)[1:].reset_index().drop(columns = "index")
        df_plasma.columns = ["time_tag", "density","speed","temperature"]
        df[["density","speed","temperature"]] = df_plasma[["density","speed","temperature"]]
        print(df)
        return df_mag
    """
    def get_current_df(self) -> pd.DataFrame:
        df_mag = pd.read_json(link_mag)[1:].reset_index().drop(columns = "index")
        df_mag.columns = ["time_tag","bx_gsm","by_gsm","bz_gsm","lon_gsm","lat_gsm","bt"]

        df_plasma = pd.read_json(link_plasma)[1:].reset_index().drop(columns = "index")
        df_plasma.columns = ["time_tag", "density","speed","temperature"]
        return df_plasma, df_mag

#["time_tag","bx_gsm","by_gsm","bz_gsm","lon_gsm","lat_gsm","bt", "density","speed","temperature"]
    def create_db(self, flag = "plasma") -> None:
        if flag == "plasma":
            df_plasma, df_mag = self.get_current_df()
            df_plasma.to_csv(self.plasmafn, index = False)
            logger.debug(f'[{str(dt.now())}] Database {self.plasmafn} is created, last minute in file is {self.last_minute_in_db(flag="plasma")}')

        if flag == "mag":
            df_mag, df_mag = self.get_current_df()
            df_mag.to_csv(self.magfn, index = False)
            logger.debug(f'[{str(dt.now())}] Database {self.magfn} is created, last minute in file is {self.last_minute_in_db(flag="mag")}')
    
    def update_db(self, flag = "plasma") -> None:
        if flag == "plasma":
            fname = self.plasmafn
        elif flag == "mag":
            fname = self.magfn
        old_data = pd.read_csv(fname)
        if flag == "plasma":
            new_data, _ = self.get_current_df()
        elif flag == "mag":
            _, new_data = self.get_current_df()
        df = pd.concat([old_data, new_data]).reset_index().drop(columns = "index")
        df = df.drop_duplicates(subset=['time_tag'])
        df.to_csv(fname, index = False)
        logger.debug(f'[{str(dt.now())}] Database {fname} is updated, last minute in file is {self.last_minute_in_db()}')
    
    def last_minute_in_db(self, flag = "plasma") -> str:
        if flag == "plasma":
            df = pd.read_csv(self.plasmafn)
            return df.iloc[-1]["time_tag"]
        elif flag == "mag":
            df = pd.read_csv(self.magfn)
            return df.iloc[-1]["time_tag"]
        else:
            raise ValueError

    def last_minute_on_site(self, flag="plasma") -> str:
        if flag == "plasma":
            df, _ = self.get_current_df()
            return df.iloc[- 1]["time_tag"]
        if flag == "mag":
            _, df = self.get_current_df()
            return df.iloc[- 1]["time_tag"]

    def if_need_update(self, flag = "plasma") -> bool:
        if flag == "plasma":
            df = pd.read_csv(self.plasmafn)
            if self.last_minute_on_site() != self.last_minute_in_db():
                logger.debug(f'[{str(dt.now())}] Database {self.plasmafn} is not up to date, last minute in file is {self.last_minute_in_db()}')
                return True
            return False
        elif flag == "mag":
            df = pd.read_csv(self.magfn)
            if self.last_minute_on_site() != self.last_minute_in_db(flag="mag"):
                logger.debug(f'[{str(dt.now())}] Database {self.magfn} is not up to date, last minute in file is {self.last_minute_in_db(flag="mag")}')
                return True
            return False
