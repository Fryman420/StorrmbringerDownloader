import downloader
from meta import *
from time import sleep
if __name__ == "__main__":

    a = downloader.downloader(db_name)
    
    while True:
        if a.if_need_update(flag="plasma"):
            a.update_db(flag="plasma")
        if a.if_need_update(flag="mag"):
            a.update_db(flag="mag")
        sleep(10)
