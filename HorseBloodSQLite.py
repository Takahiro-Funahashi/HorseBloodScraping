import copy
import os
import pathlib
import platform
import sqlite3

unknown_mac = 'ffffffffffff'
primaryTbl = None
not_null = 'NOT NULL'
primary_key = 'PRIMARY KEY'
auto_increment = 'AUTOINCREMENT'
uniaue = 'UNIQUE'
default = 'DEFAULT'
null = 'NULL'
foreign_key = 'FOREIGN KEY'
referances = 'REFERENCES'
insert = 'INSERT INTO'
select = 'SELECT'
update = 'UPDATE'
delete = 'DELETE'

HorseNameTbl = 'HorseNameTbl'
HorseBloodTbl = 'HorseBloodTbl'
RequestTbl = 'RequestTbl'
RaceTbl = 'RaceTbl'
RequestRaceTbl = 'RequestRaceTbl'
RequestRaceRankingTbl = 'RequestRaceRankingTbl'
JockeyTbl = 'JockeyTbl'
TrainerTbl = 'TrainerTbl'
OwnerTbl = 'OwnerTbl'

class class_SQLite():
    def __init__(self):
        path = os.getcwd()
        psys = platform.system()
        if psys == 'Linux':
            path = path + '/data'
            pass
        elif psys == 'Windows':
            path = path + '\\data'
        ret = pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        if psys == 'Linux':
            self.database_name = path + '/HorseBlood.db'
        elif psys == 'Windows':
            self.database_name = path + '\\HorseBlood.db'

        self.link_dict_after = dict()
        self.link_dict_race_after = dict()
        return

    def set_link_dict(self,link_dict):
        self.link_dict_after = copy.deepcopy(link_dict)

    def set_link_dict_race(self,link_dict):
        self.link_dict_race_after = copy.deepcopy(link_dict)

    def connect_DB(self):
        self.DBconnect = sqlite3.connect(self.database_name)
        self.DBcursor = self.DBconnect.cursor()
        return

    def disconnect_DB(self):
        self.DBconnect.close()

    def commit_DB(self):
        self.DBconnect.commit()

    def create_Horse_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{HorseNameTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"Name" TEXT {not_null}'
        sql_cmd = sql_cmd + f',"Birthday" TEXT {not_null}'
        sql_cmd = sql_cmd + f',"URL" TEXT )'

        self.DBcursor.execute(sql_cmd)

        return

    def create_HorseBlood_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{HorseBloodTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"horse_pkey" INTEGER {not_null}'
        sql_cmd = sql_cmd + f',"f" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"m" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"ff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"ffm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fmf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fmm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mfm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mmf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mmm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"ffff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fffm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"ffmf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"ffmm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fmff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fmfm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fmmf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"fmmm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mfff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mffm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mfmf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mfmm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mmff" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mmfm" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mmmf" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"mmmm" INTEGER {default} {null})'

        self.DBcursor.execute(sql_cmd)

        return

    def create_Request_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{RequestTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"Name" TEXT {not_null}'
        sql_cmd = sql_cmd + f',"URL" TEXT '
        sql_cmd = sql_cmd + f',"horse_pkey" INTEGER {default} {null})'

        self.DBcursor.execute(sql_cmd)

    def create_Race_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{RaceTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"Name" TEXT '
        sql_cmd = sql_cmd + f',"Date" TEXT '
        sql_cmd = sql_cmd + f',"Held" TEXT '
        sql_cmd = sql_cmd + f',"RaceNumber" TEXT '
        sql_cmd = sql_cmd + f',"URL" TEXT )'

        self.DBcursor.execute(sql_cmd)

        return

    def create_Request_Race_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{RequestRaceTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"race_pkey" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"held" TEXT '
        sql_cmd = sql_cmd + f',"date" TEXT '
        sql_cmd = sql_cmd + f',"race_grade" TEXT '
        sql_cmd = sql_cmd + f',"course_type_A_" TEXT '
        sql_cmd = sql_cmd + f',"course_type_B" TEXT '
        sql_cmd = sql_cmd + f',"turf_condition" TEXT '
        sql_cmd = sql_cmd + f',"dirt_state" TEXT '
        sql_cmd = sql_cmd + f',"weather" TEXT '
        sql_cmd = sql_cmd + f',"race_conditions_A" TEXT '
        sql_cmd = sql_cmd + f',"race_conditions_B" TEXT '
        sql_cmd = sql_cmd + f',"race_ranking_corner_1" TEXT '
        sql_cmd = sql_cmd + f',"race_ranking_corner_2" TEXT '
        sql_cmd = sql_cmd + f',"race_ranking_corner_3" TEXT '
        sql_cmd = sql_cmd + f',"race_ranking_corner_4" TEXT '
        sql_cmd = sql_cmd + f',"race_laptime" TEXT '
        sql_cmd = sql_cmd + f',"race_pace" TEXT '
        sql_cmd = sql_cmd + f')'

        self.DBcursor.execute(sql_cmd)

    def create_Race_Ranking_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{RequestRaceRankingTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"race_pkey" INTEGER {default} {null}'
        sql_cmd = sql_cmd + f',"ranking" TEXT '
        sql_cmd = sql_cmd + f',"frame_number" TEXT '
        sql_cmd = sql_cmd + f',"horse_number" TEXT '
        sql_cmd = sql_cmd + f',"horse_pkey" INTEGER '
        sql_cmd = sql_cmd + f',"sexual_age" TEXT '
        sql_cmd = sql_cmd + f',"weight" TEXT '
        sql_cmd = sql_cmd + f',"horse_weight" TEXT '
        sql_cmd = sql_cmd + f',"time" TEXT '
        sql_cmd = sql_cmd + f',"passing" TEXT '
        sql_cmd = sql_cmd + f',"last_spurt" TEXT '
        sql_cmd = sql_cmd + f',"jockey_pkey" INTEGER '
        sql_cmd = sql_cmd + f',"trainer_pkey" INTEGER '
        sql_cmd = sql_cmd + f',"owner_pkey" INTEGER '
        sql_cmd = sql_cmd + f',"prize_money" TEXT '
        sql_cmd = sql_cmd + f',"win_raito" TEXT '
        sql_cmd = sql_cmd + f',"popular" TEXT )'

        self.DBcursor.execute(sql_cmd)

    def create_Jockey_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{JockeyTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"Name" TEXT '
        sql_cmd = sql_cmd + f',"URL" TEXT )'

        self.DBcursor.execute(sql_cmd)

        return

    def create_Trainer_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{TrainerTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"Name" TEXT '
        sql_cmd = sql_cmd + f',"URL" TEXT )'

        self.DBcursor.execute(sql_cmd)

        return

    def create_Owner_Tbl(self):
        sql_cmd = f'CREATE TABLE IF NOT EXISTS "{OwnerTbl}" '
        sql_cmd = sql_cmd + \
            f'("pkey" INTEGER {not_null} {primary_key} {auto_increment} {uniaue}'
        sql_cmd = sql_cmd + f',"Name" TEXT '
        sql_cmd = sql_cmd + f',"URL" TEXT )'

        self.DBcursor.execute(sql_cmd)

        return

    def replace_Horse_Tbl(self,HorseName,Birthday,url,horse_pkey):
        if Birthday is None:
            Birthday = f'{null}'
        if url is None:
            url = f'{null}'

        if horse_pkey is None:
            sql_cmd = f'{select} * FROM {HorseNameTbl} WHERE Name = "{HorseName}" AND URL = "{url}"'
        else:
            sql_cmd = f'{select} * FROM {HorseNameTbl} WHERE pkey = "{horse_pkey}"'
        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        if TblInfo:
            pkey = TblInfo[0][0]
            if TblInfo[0][2] != 'NULL' and TblInfo[0][2]:
                Birthday = TblInfo[0][2]
            sql_cmd = f'{update} {HorseNameTbl} SET (Name,Birthday,URL) = ("{HorseName}","{Birthday}","{url}") WHERE pkey={pkey}'
            self.DBcursor.execute(sql_cmd)
        else:
            sql_cmd = f'{insert} {HorseNameTbl} (Name,Birthday,URL) VALUES ("{HorseName}","{Birthday}","{url}")'
            self.DBcursor.execute(sql_cmd)

            sql_cmd = f'{select} pkey FROM {HorseNameTbl} WHERE rowid = last_insert_rowid()'
            self.DBcursor.execute(sql_cmd)

            TblInfo = self.DBcursor.fetchall()
            if TblInfo:
                pkey = TblInfo[0][0]

        return pkey

    def replace_HorseBoold_Tbl(self,HorseName,Birthday,horse_blood,link_dict,url,horse_pkey):

        if horse_pkey is None:
            sql_cmd = f'{select} * FROM {HorseNameTbl} WHERE Name = "{HorseName}" AND URL = "{url}"'
        else:
            sql_cmd = f'{select} * FROM {HorseNameTbl} WHERE pkey = "{horse_pkey}" '

        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        if TblInfo:
            horse_pkey = TblInfo[0][0]

            set_param = ''

            for horse_name in horse_blood:
                if horse_name is not None:
                    horse_name = horse_name.replace(' ','_')
                    URL, hPkey = link_dict[horse_name]
                    set_param += f'{hPkey},'
                else:
                    set_param += f'{null},'
            else:
                set_param = set_param.rstrip(',')

            sql_cmd = f'{select} pkey FROM {HorseBloodTbl} WHERE horse_pkey = {horse_pkey}'
            self.DBcursor.execute(sql_cmd)
            TblInfo = self.DBcursor.fetchall()
            if TblInfo:
                pkey = TblInfo[0][0]
                sql_cmd = f'{update} {HorseBloodTbl} SET '\
                    + f'(f,m,ff,fm,mf,mm)=({set_param}) WHERE horse_pkey={horse_pkey}'
            else:
                sql_cmd = f'{insert} {HorseBloodTbl} '\
                    + f'(horse_pkey,f,m,ff,fm,mf,mm) VALUES ({horse_pkey},{set_param})'

            self.DBcursor.execute(sql_cmd)

        return

    def replace_request_Tbl(self,link_dict):

        add_dict = dict()
        remove_dict = dict()

        for k, v in link_dict.items():
            if k not in self.link_dict_after:
                add_dict.setdefault(k, v)

        for k, v in self.link_dict_after.items():
            if k not in link_dict:
                remove_dict.setdefault(k, v)

        self.link_dict_after = copy.deepcopy(link_dict)

        for k, v in add_dict.items():
            url, key = v
            #sql_cmd = f'{select} * FROM {tbl_name} WHERE Name = "{k}" AND URL = "{v}"'
            sql_cmd = f'{select} * FROM {RequestTbl} WHERE Name = "{k}" AND horse_pkey = {key}'
            self.DBcursor.execute(sql_cmd)
            TblInfo = self.DBcursor.fetchall()
            if TblInfo:
                pkey = TblInfo[0][0]
                sql_cmd = f'{update} {RequestTbl} SET (Name,URL,horse_pkey) = ("{k}","{url}",{key}) WHERE pkey={pkey}'
            else:
                sql_cmd = f'{insert} {RequestTbl} (Name,URL,horse_pkey) VALUES ("{k}","{url}",{key})'

            self.DBcursor.execute(sql_cmd)

        for k, v in remove_dict.items():
            url, key = v
            sql_cmd = f'{delete} FROM {RequestTbl} WHERE Name = "{k}" AND horse_pkey = {key}'
            self.DBcursor.execute(sql_cmd)

        return

    def replace_Race_Tbl(self,RaceName,RaceDate,RaceHeld,RaceNumber,url,race_pkey):
        if RaceName is None:
            RaceName = f'{null}'
        if RaceDate is None:
            RaceDate = f'{null}'
        if RaceHeld is None:
            RaceHeld = f'{null}'
        if RaceNumber is None:
            RaceNumber = f'{null}'
        if url is None:
            url = f'{null}'

        if race_pkey is None:
            sql_cmd = f'{select} * FROM {RaceTbl} WHERE Name = "{RaceName}" AND URL = "{url}"'
        else:
            sql_cmd = f'{select} * FROM {RaceTbl} WHERE pkey = "{race_pkey}"'
        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        if TblInfo:
            pkey = TblInfo[0][0]
            if TblInfo[0][2] != 'NULL' and TblInfo[0][2]:
                Birthday = TblInfo[0][2]
            sql_cmd = f'{update} {RaceTbl} SET (Name,Date,Held,RaceNumber,URL) = ' \
                + f'("{RaceName}","{RaceDate}","{RaceHeld}","{RaceNumber}","{url}") WHERE pkey={pkey}'
            self.DBcursor.execute(sql_cmd)
        else:
            sql_cmd = f'{insert} {RaceTbl} (Name,Date,Held,RaceNumber,URL) VALUES ' +\
                f'("{RaceName}","{RaceDate}","{RaceHeld}","{RaceNumber}","{url}")'
            self.DBcursor.execute(sql_cmd)

            sql_cmd = f'{select} pkey FROM {RaceTbl} WHERE rowid = last_insert_rowid()'
            self.DBcursor.execute(sql_cmd)

            TblInfo = self.DBcursor.fetchall()
            if TblInfo:
                pkey = TblInfo[0][0]

        return pkey

    def replace_request_Race_Tbl(self,link_dict):

        add_dict = dict()
        remove_dict = dict()

        for k, v in link_dict.items():
            if k not in self.link_dict_race_after:
                add_dict.setdefault(k, v)

        for k, v in self.link_dict_race_after.items():
            if k not in link_dict:
                remove_dict.setdefault(k, v)

        self.link_dict_race_after = copy.deepcopy(link_dict)

        for k, v in add_dict.items():
            url, key = v
            #sql_cmd = f'{select} * FROM {tbl_name} WHERE Name = "{k}" AND URL = "{v}"'
            sql_cmd = f'{select} * FROM {RequestRaceTbl} WHERE Name = "{k}" AND race_pkey = {key}'
            self.DBcursor.execute(sql_cmd)
            TblInfo = self.DBcursor.fetchall()
            if TblInfo:
                pkey = TblInfo[0][0]
                sql_cmd = f'{update} {RequestRaceTbl} SET (Name,URL,race_pkey) = ("{k}","{url}",{key}) WHERE pkey={pkey}'
            else:
                sql_cmd = f'{insert} {RequestRaceTbl} (Name,URL,race_pkey) VALUES ("{k}","{url}",{key})'

            self.DBcursor.execute(sql_cmd)

        for k, v in remove_dict.items():
            url, key = v
            sql_cmd = f'{delete} FROM {RequestRaceTbl} WHERE Name = "{k}" AND race_pkey = {key}'
            self.DBcursor.execute(sql_cmd)

        return

    def replace_Jockey_Tbl(self,jockey_link_list):
        if not jockey_link_list and not isinstance(jockey_link_list,list):
            return

        for jockey in jockey_link_list:
            if isinstance(jockey,dict):
                jockey_name = list(jockey.keys())[0]
                jockey_url = jockey[jockey_name]

                sql_cmd = f'{select} * FROM {JockeyTbl} WHERE Name = "{jockey_name}" AND URL = "{jockey_url}"'

                self.DBcursor.execute(sql_cmd)
                TblInfo = self.DBcursor.fetchall()

                if TblInfo:
                    pkey = TblInfo[0][0]
                    sql_cmd = f'{update} {JockeyTbl} SET (Name,URL) = ("{jockey_name}","{jockey_url}") WHERE pkey={pkey}'
                    self.DBcursor.execute(sql_cmd)
                else:
                    sql_cmd = f'{insert} {JockeyTbl} (Name,URL) VALUES ("{jockey_name}","{jockey_url}")'
                    self.DBcursor.execute(sql_cmd)

                    sql_cmd = f'{select} pkey FROM {JockeyTbl} WHERE rowid = last_insert_rowid()'
                    self.DBcursor.execute(sql_cmd)

                    TblInfo = self.DBcursor.fetchall()
                    if TblInfo:
                        pkey = TblInfo[0][0]

        return

    def replace_Trainer_Tbl(self,trainer_link):
        if not trainer_link and not isinstance(trainer_link,list):
            return

        for trainer in trainer_link:
            if isinstance(trainer,dict):
                trainer_name = list(trainer.keys())[0]
                trainer_url = trainer[trainer_name]

                sql_cmd = f'{select} * FROM {TrainerTbl} WHERE Name = "{trainer_name}" AND URL = "{trainer_url}"'

                self.DBcursor.execute(sql_cmd)
                TblInfo = self.DBcursor.fetchall()

                if TblInfo:
                    pkey = TblInfo[0][0]
                    sql_cmd = f'{update} {TrainerTbl} SET (Name,URL) = ("{trainer_name}","{trainer_url}") WHERE pkey={pkey}'
                    self.DBcursor.execute(sql_cmd)
                else:
                    sql_cmd = f'{insert} {TrainerTbl} (Name,URL) VALUES ("{trainer_name}","{trainer_url}")'
                    self.DBcursor.execute(sql_cmd)

                    sql_cmd = f'{select} pkey FROM {TrainerTbl} WHERE rowid = last_insert_rowid()'
                    self.DBcursor.execute(sql_cmd)

                    TblInfo = self.DBcursor.fetchall()
                    if TblInfo:
                        pkey = TblInfo[0][0]

        return

    def replace_Owner_Tbl(self,owner_link_list):
        if not owner_link_list and not isinstance(owner_link_list,list):
            return

        for owner in owner_link_list:
            if isinstance(owner,dict):
                owner_name = list(owner.keys())[0]
                owner_url = owner[owner_name]

                sql_cmd = f'{select} * FROM {OwnerTbl} WHERE Name = "{owner_name}" AND URL = "{owner_url}"'

                self.DBcursor.execute(sql_cmd)
                TblInfo = self.DBcursor.fetchall()

                if TblInfo:
                    pkey = TblInfo[0][0]
                    sql_cmd = f'{update} {OwnerTbl} SET (Name,URL) = ("{owner_name}","{owner_url}") WHERE pkey={pkey}'
                    self.DBcursor.execute(sql_cmd)
                else:
                    sql_cmd = f'{insert} {OwnerTbl} (Name,URL) VALUES ("{owner_name}","{owner_url}")'
                    self.DBcursor.execute(sql_cmd)

                    sql_cmd = f'{select} pkey FROM {OwnerTbl} WHERE rowid = last_insert_rowid()'
                    self.DBcursor.execute(sql_cmd)

                    TblInfo = self.DBcursor.fetchall()
                    if TblInfo:
                        pkey = TblInfo[0][0]

        return

    def replace_Request_Race_Tbl(self,race_pkey,base_info,passing_info,time_info):
        if not race_pkey:
            return

        sql_cmd = f'{select} pkey FROM {RaceTbl} WHERE pkey = {race_pkey}'

        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        if TblInfo:
            pkey = TblInfo[0][0]
            sql_cmd = f'{select} * FROM {RequestRaceTbl} WHERE race_pkey = {pkey}'

            self.DBcursor.execute(sql_cmd)
            TblInfo = self.DBcursor.fetchall()

            set_clm_name = 'race_pkey,held,date,race_grade,course_type_A_,course_type_B,' \
                + 'turf_condition,dirt_state,weather,race_conditions_A,race_conditions_B'

            if isinstance(base_info,dict):
                held,date,race_grade,course_type_A_,course_type_B,\
                turf_condition,dirt_state,weather,race_conditions_A,race_conditions_B = \
                    None,None,None,None,None,None,None,None,None,None
                if '開催地' in base_info:
                    held = base_info['開催地']
                if '開催日' in base_info:
                    date = base_info['開催日']

                if 'グレード' in base_info:
                    race_grade = base_info['グレード']
                if 'コース種別A' in base_info:
                    course_type_A_ = base_info['コース種別A']
                if 'コース種別B' in base_info:
                    course_type_B = base_info['コース種別B']
                if '芝状態' in base_info:
                    turf_condition = base_info['芝状態']
                if 'ダート状態' in base_info:
                    dirt_state = base_info['ダート状態']
                if '天候' in base_info:
                    weather = base_info['天候']
                if '条件A' in base_info:
                    race_conditions_A = base_info['条件A']
                if '条件B' in base_info:
                    race_conditions_B = base_info['条件B']

                set_param = f'{race_pkey}'

                if held is None:
                    held = f'{null}'
                    set_param += f',{held}'
                else:
                    set_param += f',"{held}"'
                if date is None:
                    date = f'{null}'
                    set_param += f',{date}'
                else:
                    set_param += f',"{date}"'
                if race_grade is None:
                    race_grade = f'{null}'
                    set_param += f',{race_grade}'
                else:
                    set_param += f',"{race_grade}"'
                if course_type_A_ is None:
                    course_type_A_ = f'{null}'
                    set_param += f',{course_type_A_}'
                else:
                    set_param += f',"{course_type_A_}"'
                if course_type_B is None:
                    course_type_B = f'{null}'
                    set_param += f',{course_type_B}'
                else:
                    set_param += f',"{course_type_B}"'
                if turf_condition is None:
                    turf_condition = f'{null}'
                    set_param += f',{turf_condition}'
                else:
                    set_param += f',"{turf_condition}"'
                if dirt_state is None:
                    dirt_state = f'{null}'
                    set_param += f',{dirt_state}'
                else:
                    set_param += f',"{dirt_state}"'
                if weather is None:
                    weather = f'{null}'
                    set_param += f',{weather}'
                else:
                    set_param += f',"{weather}"'
                if race_conditions_A is None:
                    race_conditions_A = f'{null}'
                    set_param += f',{race_conditions_A}'
                else:
                    set_param += f',"{race_conditions_A}"'
                if race_conditions_B is None:
                    race_conditions_B = f'{null}'
                    set_param += f',{race_conditions_B}'
                else:
                    set_param += f',"{race_conditions_B}"'

            add_passing_clm_name = ',race_ranking_corner_1,race_ranking_corner_2,race_ranking_corner_3,race_ranking_corner_4'
            add_time_clm_name = ',race_laptime,race_pace'
            if passing_info:
                set_clm_name += add_passing_clm_name
                for passing in passing_info:
                    if passing is None:
                        set_param += f',{null}'
                    else:
                        set_param += f',"{passing}"'
            if time_info:
                set_clm_name += add_time_clm_name
                for _t_ in time_info:
                    if _t_ is None:
                        set_param += f',{null}'
                    else:
                        set_param += f',"{_t_}"'

            if TblInfo:
                pkey = TblInfo[0][0]
                sql_cmd = f'{update} {RequestRaceTbl} SET ({set_clm_name}) = ({set_param}) WHERE pkey={pkey}'
                self.DBcursor.execute(sql_cmd)
            else:
                sql_cmd = f'{insert} {RequestRaceTbl} ({set_clm_name}) VALUES ({set_param})'
                self.DBcursor.execute(sql_cmd)

                sql_cmd = f'{select} pkey FROM {RequestRaceTbl} WHERE rowid = last_insert_rowid()'
                self.DBcursor.execute(sql_cmd)

                TblInfo = self.DBcursor.fetchall()
                if TblInfo:
                    pkey = TblInfo[0][0]

        return pkey

    def replace_Request_RaceRanking_Tbl(self,race_pkey,race_ranking_info,
        horse_link_list,jockey_link_list,trainer_link_list,owner_link_list):
        if not race_pkey:
            return
        horse_link = { list(h.keys())[0]:list(h.values())[0] for h in horse_link_list}
        jocky_link = { list(h.keys())[0]:list(h.values())[0] for h in jockey_link_list}
        trainer_link = { list(h.keys())[0]:list(h.values())[0] for h in trainer_link_list}
        owner_link = { list(h.keys())[0]:list(h.values())[0] for h in owner_link_list}

        sql_cmd = f'{select} pkey FROM {RaceTbl} WHERE pkey = {race_pkey}'

        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        if TblInfo:
            race_pkey = TblInfo[0][0]

            set_clm_name = 'race_pkey,ranking,frame_number,horse_number,horse_pkey,sexual_age,'\
                + 'weight,horse_weight,time,passing,last_spurt,'\
                + 'jockey_pkey,trainer_pkey,owner_pkey,prize_money,win_raito,popular'

            if isinstance(race_ranking_info,list):
                for race_ranking in race_ranking_info:

                    ranking,frame_number,horse_number,horse_pkey,sexual_age = \
                        None,None,None,None,None
                    weight,horse_weight,_times_,passing,last_spurt = \
                        None,None,None,None,None
                    jockey_pkey,trainer_pkey,owner_pkey,prize_money,win_raito,popular = \
                        None,None,None,None,None,None

                    if '着順' in race_ranking:
                        ranking = race_ranking['着順']
                    if '枠番' in race_ranking:
                        frame_number = race_ranking['枠番']
                    if '馬番' in race_ranking:
                        horse_number = race_ranking['馬番']
                    if '馬名' in race_ranking:
                        horse_name = race_ranking['馬名']
                        if horse_name in horse_link:
                            horse_url = horse_link[horse_name]
                            sql_cmd = f'{select} pkey FROM {HorseNameTbl} WHERE Name = "{horse_name}" AND URL = "{horse_url}"'
                            self.DBcursor.execute(sql_cmd)
                            _horse_ = self.DBcursor.fetchall()
                            if _horse_:
                                horse_pkey = _horse_[0][0]
                    if '性齢' in race_ranking:
                        sexual_age = race_ranking['性齢']
                    if '斤量' in race_ranking:
                        weight = race_ranking['斤量']
                    if '馬体重' in race_ranking:
                        horse_weight = race_ranking['馬体重']
                    if 'タイム' in race_ranking:
                        _times_ = race_ranking['タイム']
                    if '通過' in race_ranking:
                        passing = race_ranking['通過']
                    if '上り' in race_ranking:
                        last_spurt = race_ranking['上り']
                    if '騎手' in race_ranking:
                        jocky_name = race_ranking['騎手']
                        if jocky_name in jocky_link:
                            jocky_url = jocky_link[jocky_name]
                            sql_cmd = f'{select} pkey FROM {JockeyTbl} WHERE Name = "{jocky_name}" AND URL = "{jocky_url}"'
                            self.DBcursor.execute(sql_cmd)
                            _jocky_ = self.DBcursor.fetchall()
                            if _jocky_:
                                jockey_pkey = _jocky_[0][0]
                    if '調教師' in race_ranking:
                        trainer_name = race_ranking['調教師']
                        if trainer_name in trainer_link:
                            trainer_url = trainer_link[trainer_name]
                            sql_cmd = f'{select} pkey FROM {TrainerTbl} WHERE Name = "{trainer_name}" AND URL = "{trainer_url}"'
                            self.DBcursor.execute(sql_cmd)
                            _trainer_ = self.DBcursor.fetchall()
                            if _trainer_:
                                trainer_pkey = _trainer_[0][0]
                    if '馬主' in race_ranking:
                        owner_name = race_ranking['馬主']
                        if owner_name in owner_link:
                            owner_url = owner_link[owner_name]
                            sql_cmd = f'{select} pkey FROM {OwnerTbl} WHERE Name = "{owner_name}" AND URL = "{owner_url}"'
                            self.DBcursor.execute(sql_cmd)
                            _owner_ = self.DBcursor.fetchall()
                            if _owner_:
                                owner_pkey = _owner_[0][0]
                    if '賞金(万円)' in race_ranking:
                        prize_money = race_ranking['賞金(万円)']
                    if '単勝' in race_ranking:
                        win_raito = race_ranking['単勝']
                    if '人気' in race_ranking:
                        popular = race_ranking['人気']

                    set_param = f'{race_pkey}'

                    if ranking is None:
                        ranking = f'{null}'
                        set_param += f',{ranking}'
                    else:
                        set_param += f',"{ranking}"'
                    if frame_number is None:
                        frame_number = f'{null}'
                        set_param += f',{frame_number}'
                    else:
                        set_param += f',"{frame_number}"'
                    if horse_number is None:
                        horse_number = f'{null}'
                        set_param += f',{horse_number}'
                    else:
                        set_param += f',"{horse_number}"'
                    if horse_pkey is None:
                        horse_pkey = f'{null}'
                        set_param += f',{horse_pkey}'
                    else:
                        set_param += f',"{horse_pkey}"'
                    if sexual_age is None:
                        sexual_age = f'{null}'
                        set_param += f',{sexual_age}'
                    else:
                        set_param += f',"{sexual_age}"'
                    if weight is None:
                        weight = f'{null}'
                        set_param += f',{weight}'
                    else:
                        set_param += f',"{weight}"'
                    if horse_weight is None:
                        horse_weight = f'{null}'
                        set_param += f',{horse_weight}'
                    else:
                        set_param += f',"{horse_weight}"'
                    if _times_ is None:
                        _times_ = f'{null}'
                        set_param += f',{_times_}'
                    else:
                        set_param += f',"{_times_}"'
                    if passing is None:
                        passing = f'{null}'
                        set_param += f',{passing}'
                    else:
                        set_param += f',"{passing}"'
                    if last_spurt is None:
                        last_spurt = f'{null}'
                        set_param += f',{last_spurt}'
                    else:
                        set_param += f',"{last_spurt}"'
                    if jockey_pkey is None:
                        jockey_pkey = f'{null}'
                        set_param += f',{jockey_pkey}'
                    else:
                        set_param += f',"{jockey_pkey}"'
                    if trainer_pkey is None:
                        trainer_pkey = f'{null}'
                        set_param += f',{trainer_pkey}'
                    else:
                        set_param += f',"{trainer_pkey}"'
                    if owner_pkey is None:
                        owner_pkey = f'{null}'
                        set_param += f',{owner_pkey}'
                    else:
                        set_param += f',"{owner_pkey}"'
                    if prize_money is None:
                        prize_money = f'{null}'
                        set_param += f',{prize_money}'
                    else:
                        set_param += f',"{prize_money}"'
                    if win_raito is None:
                        win_raito = f'{null}'
                        set_param += f',{win_raito}'
                    else:
                        set_param += f',"{win_raito}"'
                    if popular is None:
                        popular = f'{null}'
                        set_param += f',{popular}'
                    else:
                        set_param += f',"{popular}"'

                    sql_cmd = f'{select} * FROM {RequestRaceRankingTbl} WHERE race_pkey = {race_pkey} ' \
                        + f'AND frame_number = "{frame_number}" AND horse_number = "{horse_number}"'

                    self.DBcursor.execute(sql_cmd)
                    TblInfo = self.DBcursor.fetchall()

                    if TblInfo:
                        pkey = TblInfo[0][0]
                        sql_cmd = f'{update} {RequestRaceRankingTbl} SET ({set_clm_name}) = ({set_param}) WHERE pkey={pkey}'
                        self.DBcursor.execute(sql_cmd)
                    else:
                        sql_cmd = f'{insert} {RequestRaceRankingTbl} ({set_clm_name}) VALUES ({set_param})'
                        self.DBcursor.execute(sql_cmd)

                        sql_cmd = f'{select} pkey FROM {RequestRaceRankingTbl} WHERE rowid = last_insert_rowid()'
                        self.DBcursor.execute(sql_cmd)

                        t = self.DBcursor.fetchall()
                        if t:
                            pkey = t[0][0]

        return pkey

    def get_request_Tbl(self):
        self.connect_DB()

        tbl_name = 'RequestTbl'
        sql_cmd = f'{select} Name,URL,horse_pkey FROM {tbl_name}'
        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()
        if TblInfo:
            for Name,URL,horse_pkey in TblInfo:
                self.link_dict_after.setdefault(Name,[URL,horse_pkey])

        self.disconnect_DB()

        return self.link_dict_after

    def get_request_RaceTbl(self):
        self.connect_DB()

        tbl_name = 'RequestRaceTbl'
        sql_cmd = f'{select} Name,URL,race_pkey FROM {tbl_name}'
        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()
        if TblInfo:
            for Name,URL,race_pkey in TblInfo:
                self.link_dict_race_after.setdefault(Name,[URL,race_pkey])

        self.disconnect_DB()

        return self.link_dict_race_after

    def chk_HorseURL_Tbl(self,url):
        self.connect_DB()

        sql_cmd = f'{select} * FROM {HorseNameTbl} WHERE URL = "{url}" AND NOT Birthday = "{null}"'
        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        self.disconnect_DB()

        if TblInfo:
            return True
        else:
            return False

    def get_HorseURL_Tbl(self,horse_pkey=None,Name=None,url=None,limit_number=None):
        isWhere = False
        self.connect_DB()

        sql_cmd = f'{select} * FROM {HorseNameTbl} '
        if horse_pkey is not None:
            if isinstance(horse_pkey,int):
                sql_cmd += f'WHERE pkey={horse_pkey} '
                isWhere = True
        if Name is not None:
            if isinstance(Name,str):
                if isWhere:
                    sql_cmd += f'AND Name={Name} '
                else:
                    sql_cmd += f'WHERE Name={Name} '
                isWhere = True
        if url is not None:
            if isinstance(url,str):
                if isWhere:
                    sql_cmd += f'AND URL={url} '
                else:
                    sql_cmd += f'WHERE URL={url} '
        if limit_number is not None:
            if isinstance(limit_number,int):
                sql_cmd += f'LIMIT {limit_number} '

        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        HorseNameData = dict()
        for d in TblInfo:
            HorseNameData.setdefault(d[1],{'pkey':d[0],'Birthday':d[2],'url':d[3]})


        self.disconnect_DB()

        return HorseNameData

    def get_horse_blood(self,horse_pkey):
        names = list()

        self.connect_DB()

        sql_cmd = f'{select} horse_pkey,f,m,ff,fm,mf,mm,'\
            + f'fff,ffm,fmf,fmm,mff,mfm,mmf,mmm, ' \
            + f'ffff,fffm,ffmf,ffmm,fmff,fmfm,fmmf,fmmm, ' \
            + f'mfff,mffm,mfmf,mfmm,mmff,mmfm,mmmf,mmmm ' \
            + f'FROM {HorseBloodTbl} WHERE horse_pkey = {horse_pkey}'
        self.DBcursor.execute(sql_cmd)
        data = self.DBcursor.fetchall()

        for blood in data:
            for horse_key in blood:
                if horse_key:
                    sql_cmd = f'{select} Name FROM {HorseNameTbl} WHERE pkey = {horse_key}'
                    self.DBcursor.execute(sql_cmd)
                    horse_names = self.DBcursor.fetchall()
                    if horse_names:
                        horse_name = horse_names[0][0]
                        names.append(horse_name)
                else:
                    names.append(None)

        self.disconnect_DB()

        return names

    def get_RaceURL_Tbl(self,race_pkey=None,Name=None,url=None,limit_number=None):
        isWhere = False
        self.connect_DB()

        sql_cmd = f'{select} * FROM {RaceTbl} '
        if race_pkey is not None:
            if isinstance(race_pkey,int):
                sql_cmd += f'WHERE pkey={race_pkey} '
                isWhere = True
        if Name is not None:
            if isinstance(Name,str):
                if isWhere:
                    sql_cmd += f'AND Name={Name} '
                else:
                    sql_cmd += f'WHERE Name={Name} '
                isWhere = True
        if url is not None:
            if isinstance(url,str):
                if isWhere:
                    sql_cmd += f'AND URL={url} '
                else:
                    sql_cmd += f'WHERE URL={url} '
        if limit_number is not None:
            if isinstance(limit_number,int):
                sql_cmd += f'LIMIT {limit_number} '

        self.DBcursor.execute(sql_cmd)
        TblInfo = self.DBcursor.fetchall()

        RaceNameData = dict()
        for d in TblInfo:
            RaceNameData.setdefault(d[0],{'name':d[1],'date':d[2],'held':d[3],'race_number':d[4],'url':d[5]})

        self.disconnect_DB()

        return RaceNameData

    def horse_pedigree(self,horse_pkey):
        blood_2nd_pedigree = ['ff','fm','mf','mm']
        blood_pedigree = ['f','m','ff','fm','mf','mm']

        clm = ','.join(blood_pedigree)

        sql_cmd = f'{select} {clm} FROM {HorseBloodTbl} '\
            + f'WHERE horse_pkey = {horse_pkey}'

        self.DBcursor.execute(sql_cmd)
        horse_blood = self.DBcursor.fetchall()

        for clm_key in blood_2nd_pedigree:
            blood_clm = ''
            conditions = ''
            for b_clm_key, horse_key in zip(blood_pedigree,horse_blood[0]):
                conditions += f'AND {clm_key}{b_clm_key} is {null} '
                blood_clm += f'{clm_key}{b_clm_key},'
            else:
                blood_clm = blood_clm.rstrip(',')

            sql_cmd = f'{select} pkey FROM {HorseBloodTbl} '\
                + f'WHERE {clm_key} = {horse_pkey} '\
                + conditions
            self.DBcursor.execute(sql_cmd)
            NotBlood = self.DBcursor.fetchall()
            if NotBlood:
                for row in NotBlood:
                    pkey = row[0]
                    str_horse_blood = list()
                    for a in horse_blood[0]:
                        if a is None:
                            str_horse_blood.append(f'{null}')
                        else:
                            str_horse_blood.append(f'{a}')
                    blood_data = ','.join(str_horse_blood)
                    sql_cmd = f'{update} {HorseBloodTbl} SET ({blood_clm}) = ({blood_data}) '\
                        + f'WHERE pkey = {pkey}'
                    self.DBcursor.execute(sql_cmd)

        return

    def corrected_horse_name(self,horse_name,url):
        self.connect_DB()

        sql_cmd = f'{update} {HorseNameTbl} SET (Name) = ("{horse_name}") WHERE URL = url'
        self.DBcursor.execute(sql_cmd)

        self.commit_DB()
        self.disconnect_DB()

        return

    def clear_Request_Tbl(self):
        self.connect_DB()

        sql_cmd = f'{delete} FROM {RequestTbl}'
        self.DBcursor.execute(sql_cmd)

        self.commit_DB()
        self.disconnect_DB()

        return

if __name__ == '__main__':
    obj = class_SQLite()
    names = obj.get_horse_blood(3520)
    print(names)

    def set_horse_pedigree(obj):
        obj.connect_DB()
        sql_cmd = f'{select} horse_pkey FROM {HorseBloodTbl} '
        obj.DBcursor.execute(sql_cmd)
        horse_list = obj.DBcursor.fetchall()

        for horse in horse_list:
            horse_key = horse[0]
            obj.horse_pedigree(horse_key)
            print(horse_key)
            obj.commit_DB()

        obj.disconnect_DB()

    def get_horse_pedigree(obj):
        obj.connect_DB()
        sql_cmd = f'{select} horse_pkey FROM {HorseBloodTbl} '
        obj.DBcursor.execute(sql_cmd)
        horse_list = obj.DBcursor.fetchall()
        obj.disconnect_DB()

        for horse in horse_list:
            horse_key = horse[0]
            names = obj.get_horse_blood(horse_key)
            print(names)


