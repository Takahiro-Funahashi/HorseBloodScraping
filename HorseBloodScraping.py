import cchardet
from bs4 import BeautifulSoup

from fake_useragent import UserAgent

import dill
import os
import platform
import re
import requests
import time

import HorseBloodSQLite as HBDB

class HorseData_scraping():
    def __init__(self,interval_time=1,limit_time = 600):
        self.time_out = 3
        ua = UserAgent()
        self.header = {'user-agent':ua.chrome}
        if interval_time >= 1:
            self.interval_time = interval_time
        else:
            self.interval_time = 1

        if limit_time > 0 and limit_time <= 3600:
            self.limit_time = limit_time
        else:
            self.limit_time = 600

        self.DB = HBDB.class_SQLite()

        return

    def _temp_FileIO(self,url:str,mode:str='wr'):
        """ URLアクセスし取得したrequestsオブジェクトを読み書き
            URLアクセスし取得したrequestsオブジェクトをシリアル化して一時ファイルに保存/読込をする。
            デバッグのために何度もアクセスしなくても良くなる。

        Args:
            url (str): URL
            mode (str, optional): [description]. Defaults to 'wr'.

        Returns:
            [type]: [description]
        """

        if mode not in ['r','w','wr']:
            return False

        soup = response = None

        if 'w' in mode:
            response = requests.get(url,headers=self.header,timeout=self.time_out)
            response.encoding = response.apparent_encoding
            enc = cchardet.detect(response.content)
            encoding = enc['encoding']

        path = os.getcwd()
        psys = platform.system()
        if psys == 'Linux':
            html_path = path +'/_temp_/_temp_.html'
            path = path + '/_temp_/_temp_soup_.pickle'
            pass
        elif psys == 'Windows':
            html_path = path +'\\_temp_\\_temp_.html'
            path = path + '\\_temp_\\_temp_soup_.pickle'

        if 'w' in mode:
            with open(path, 'wb') as fp:
                dill.dump(response,fp)
            with open(html_path, 'w') as fp:
                fp.write(response.text)

        if 'r' in mode:
            with open(path, 'rb') as fp:
                response = dill.load(fp)
        if response is not None:
            soup = BeautifulSoup(response.text, "html.parser")

        return soup

    def get_response(self,url):
        response = requests.get(url,headers=self.header,timeout=self.time_out)
        response.encoding = response.apparent_encoding
        enc = cchardet.detect(response.content)
        encoding = enc['encoding']

        return BeautifulSoup(response.text, "html.parser")

    def get_HorseName(self, soup):
        # タイトルから馬名を取得
        _title_ = soup.find('title').get_text(strip=True)
        _title_ = _title_.strip(' - netkeiba.com')
        t = _title_.split('|')
        horse_name = t[0].rstrip(' ')

        return horse_name

    def get_RaceName(self, soup):
        # タイトルから馬名を取得
        _title_ = soup.find('title').get_text(strip=True)
        _title_ = _title_.strip(' - netkeiba.com')
        t = _title_.split('|')
        t = t[0].split('｜')
        race_name = t[0].rstrip(' ')
        race_date = t[1].rstrip(' ')

        return race_name,race_date

    def get_HorseProfile(self, soup):
        horse_birthday = None
        # プロフィールテーブルを取得
        profile_table = soup.find("table",class_="db_prof_table no_OwnerUnit")
        if profile_table:
            trs = profile_table.find_all('tr')
            for tr in trs:
                th = tr.find('th')
                title = th.get_text()
                if title == '生年月日':
                    td = tr.find('td')
                    horse_birthday = td.get_text()
                    break

        return horse_birthday

    def get_HorseBoold(self, soup):
        link_dict = dict()
        horse_blood = list()

        blood_table = soup.find("table",class_="blood_table")
        links = blood_table.find_all('a')
        for i, link in enumerate(links):
            horse_name = None
            url = link.attrs['href']
            if url.strip('/horse/ped//') != '':
                horse_name = link.get_text()
                if horse_name: #ページの誤植？で簡易血統表から馬名が消えているものがある。
                    url = link.attrs['href']
                    url = "https://db.netkeiba.com" + url

                    url, horse_name = self.get_NextHorseInfo(horse_name,url)

                    horse_blood.append(horse_name)
                    link_dict.setdefault(horse_name,url)
                else:
                    horse_blood.append(None)
            else:
                horse_blood.append(None)

        f_horse,ff_horse,fm_horse,m_horse,mf_horse,mm_horse = horse_blood

        return horse_blood, link_dict

    def get_NextHorseInfo(self,horse_name,url):
        soup = self.get_response(url)
        print('access:%-16s:%s' % (horse_name,url))

        # 簡易血統表では馬名が省略されることがあり、プロフィールの馬名と異なる。
        detail_menu = soup.find('ul',class_='db_detail_menu')
        a_tble = detail_menu.find_all('a')
        for a in a_tble:
            title = a.attrs['title']
            if 'のプロフィールTOP' in title:
                get_horse_name = title.rstrip('のプロフィールTOP')
                if get_horse_name:
                    if horse_name != get_horse_name:
                        horse_name = get_horse_name
                    url = a.attrs['href']
                    url = "https://db.netkeiba.com" + url
                else:
                    #ページエラー　Sister 2 to Blast' 'https://db.netkeiba.com/horse/ped/000a016742/'
                    return None,horse_name
                break

        return url, horse_name

    def get_HorseInfo(self,url,horse_pkey,u_link_dict):
        soup = self.get_response(url)
        horse_name = self.get_HorseName(soup)

        print('access:%-16s:%s' % (horse_name,url))
        horse_name = horse_name.replace(' ','_')
        horse_birthday = self.get_HorseProfile(soup)
        horse_blood, horse_blood_link_dict = self.get_HorseBoold(soup)

        self.DB.connect_DB()

        link_dict = dict()

        horse_pkey = self.DB.replace_Horse_Tbl(horse_name,horse_birthday,url,horse_pkey)
        for b_horse_name, b_url in horse_blood_link_dict.items():
            b_horse_name = b_horse_name.replace(' ','_')
            ｂ_horse_pkey = self.DB.replace_Horse_Tbl(b_horse_name,None,b_url,None)
            link_dict.setdefault(b_horse_name,[b_url,ｂ_horse_pkey])

        self.DB.replace_HorseBoold_Tbl(horse_name,horse_birthday,horse_blood,link_dict,url,horse_pkey)

        race_dict = self.get_RaceInfo(soup)

        r_link_dict =self.get_RaceResultInfo(race_dict)

        link_dict.update(r_link_dict)
        u_link_dict.update(link_dict)

        self.DB.horse_pedigree(horse_pkey)

        self.DB.replace_request_Tbl(u_link_dict)

        self.DB.commit_DB()
        self.DB.disconnect_DB()

        f_horse,ff_horse,fm_horse,m_horse,mf_horse,mm_horse = horse_blood

        print('馬名',horse_name)
        print('生年月日',horse_birthday)
        print('父',f_horse)
        print('父父',ff_horse)
        print('父母',fm_horse)
        print('母',m_horse)
        print('母父',mf_horse)
        print('母母',mm_horse)

        return u_link_dict

    def run(self, url):
        horse_pkey = None
        link_dict = dict()

        self.DB.connect_DB()
        self.DB.create_Horse_Tbl()
        self.DB.create_HorseBlood_Tbl()
        self.DB.create_Request_Tbl()
        self.DB.create_Race_Tbl()
        self.DB.create_Request_Race_Tbl()
        self.DB.create_Race_Ranking_Tbl()
        self.DB.create_Jockey_Tbl()
        self.DB.create_Trainer_Tbl()
        self.DB.create_Owner_Tbl()
        self.DB.commit_DB()
        self.DB.disconnect_DB()

        # 前回の続きのリクエストリストを取得
        link_dict = self.DB.get_request_Tbl()

        if link_dict:
            self.DB.set_link_dict(link_dict)
            next_horse,url,horse_pkey = self._get_next_(link_dict)

        print('スクレイピングを開始します。')

        start_time = time.time()

        horse_name = dict()

        while(True):
            #time.sleep(self.interval_time)
            link_dict = self.get_HorseInfo(url,horse_pkey,link_dict)

            next_horse, url, horse_pkey = self._next_horse_(link_dict)

            print(len(link_dict))
            if len(link_dict) == 0:
                link_dict = self.DB.get_request_Tbl()
                if len(link_dict) == 0:
                    break
                else:
                    next_horse, url, horse_pkey = self._next_horse_(link_dict)
                    if next_horse is None or url is None or horse_pkey is None:
                        break
            else:
                if next_horse in horse_name:
                    horse_name[next_horse] = horse_name[next_horse] +1
                else:
                    horse_name.setdefault(next_horse,1)

            ntime = time.time()

            if ntime - start_time > self.limit_time:
                print(f'規定時間{self.limit_time}秒を経過したため、スクレイピングを終了しします。')
                print(horse_name)
                break

        return

    def _get_next_(self, link_dict):
        next_ = url = pkey = None
        ans = list(link_dict.keys())
        if ans:
            next_ = ans[0]
            url = link_dict[next_][0]
            pkey = link_dict[next_][1]
            link_dict.pop(next_)

        return next_,url,pkey

    def _next_horse_(self,link_dict):
        next_horse, url, horse_pkey = None, None, None
        while(True):
            next_horse, url, horse_pkey = self._get_next_(link_dict)
            if not link_dict and not next_horse and not url and not horse_pkey:
                # 全てスキップになった場合にRequestTblをクリア
                self.DB.clear_Request_Tbl()
                break
            if not self.DB.chk_HorseURL_Tbl(url) and url and url != 'None':
                break
            if not url or url == 'None':
                print(next_horse,'URLがないか異常のため、スキップします。')
            else:
                print(next_horse,'既に登録されているため、スキップします。')

        return (next_horse, url, horse_pkey)

    def run_race_info(self):
        # HorseNameTblのみ取得した際に、HorseNameTblをRequestTblに複製し、
        # RequestTblを削りながら、RaceTblに情報を書き込む

        horse_pkey = None
        link_dict = dict()

        race_pkey = None
        link_dict_race = dict()

        self.DB.connect_DB()
        self.DB.create_Race_Tbl()
        self.DB.create_Request_Race_Tbl()
        self.DB.commit_DB()
        self.DB.disconnect_DB()

        HorseNameData = self.DB.get_HorseURL_Tbl()
        h_link_dict = dict()
        for horse_name, val in HorseNameData.items():
            h_link_dict.setdefault(horse_name,[val['url'],val['pkey']])

        link_dict = self.DB.get_request_Tbl()

        if link_dict:
            self.DB.set_link_dict(link_dict)
        else:
            self.DB.connect_DB()
            self.DB.replace_request_Tbl(h_link_dict)
            self.DB.commit_DB()
            self.DB.disconnect_DB()
            link_dict = h_link_dict

        next_horse,horse_url,horse_pkey = self._get_next_(link_dict)

        print('スクレイピングを開始します。')

        start_time = time.time()

        horse_name = dict()

        while(True):

            link_dict_race = self.get_RaceInfo(horse_url,next_horse,link_dict)

            next_horse,horse_url,horse_pkey = self._get_next_(link_dict)

            print(len(link_dict))
            if len(link_dict) == 0:
                link_dict = self.DB.get_request_Tbl()
                if len(link_dict) == 0:
                    break
                else:
                    next_horse,horse_url,horse_pkey = self._get_next_(link_dict)
                    if next_horse is None or horse_url is None or horse_pkey is None:
                        break
            else:
                if next_horse in horse_name:
                    race_name[next_horse] = horse_name[next_horse] +1
                else:
                    horse_name.setdefault(next_horse,1)

            ntime = time.time()

            if ntime - start_time > self.limit_time:
                print(f'規定時間{self.limit_time}秒を経過したため、スクレイピングを終了しします。')
                print(horse_name)
                break

        return

    def get_RaceInfo(self,soup):
        race_results = soup.find('table',class_='db_h_race_results nk_tb_common')

        race_dict = dict()

        # 競争成績のデータがある場合
        if race_results:
            th_tble = race_results.find_all('th')

            race_table_clms = list()
            for th in th_tble:
                race_table_clms.append(th.get_text())

            require_race_table_clms = ['日付', '開催', 'R', 'レース名']
            require_num_list = [ race_table_clms.index(clm) for clm in race_table_clms if clm in require_race_table_clms]

            tbody_tble = race_results.find_all('tbody')
            for tbody in tbody_tble:
                tr_table = tbody.find_all('tr')
                for tr in tr_table:
                    race_date = race_held = race_number = None
                    race_name = race_url = None
                    td_table = tr.find_all('td')
                    for i, td in enumerate(td_table):
                        if i in require_num_list:
                            clm_name = require_race_table_clms[require_num_list.index(i)]
                            if clm_name == require_race_table_clms[0]:
                                race_date = td.get_text()
                            if clm_name == require_race_table_clms[1]:
                                race_held = td.get_text()
                            if clm_name == require_race_table_clms[2]:
                                race_number = td.get_text()
                            if clm_name == require_race_table_clms[3]:
                                links = td.find('a')
                                if links:
                                    race_name = links.attrs['title']
                                    race_url = "https://db.netkeiba.com" + links.attrs['href']
                            if race_date and race_held and race_number \
                                and race_name and race_url:

                                soup = self.get_response(race_url)
                                print('access:%-16s:%s' % (race_name,race_url))
                                race_name, race_date = self.get_RaceName(soup)

                                race_pkey = self.DB.replace_Race_Tbl(race_name,race_date,race_held,race_number,race_url,None)

                                race_dict.setdefault(race_pkey,race_url)

                                print('レース名:',race_name,'開催日:',race_date)
                                break

        return race_dict

    def _get_RaceInfo(self,horse_url,horse_name,u_link_dict):
        soup = self.get_response(horse_url)
        print('access:%-16s:%s' % (horse_name,horse_url))
        race_results = soup.find('table',class_='db_h_race_results nk_tb_common')

        self.DB.connect_DB()

        # 競争成績のデータがある場合
        if race_results:
            th_tble = race_results.find_all('th')

            race_table_clms = list()
            for th in th_tble:
                race_table_clms.append(th.get_text())

            require_race_table_clms = ['日付', '開催', 'R', 'レース名']
            require_num_list = [ race_table_clms.index(clm) for clm in race_table_clms if clm in require_race_table_clms]


            tbody_tble = race_results.find_all('tbody')
            for tbody in tbody_tble:
                tr_table = tbody.find_all('tr')
                for tr in tr_table:
                    race_date = race_held = race_number = None
                    race_name = race_url = None
                    td_table = tr.find_all('td')
                    for i, td in enumerate(td_table):
                        if i in require_num_list:
                            clm_name = require_race_table_clms[require_num_list.index(i)]
                            if clm_name == require_race_table_clms[0]:
                                race_date = td.get_text()
                            if clm_name == require_race_table_clms[1]:
                                race_held = td.get_text()
                            if clm_name == require_race_table_clms[2]:
                                race_number = td.get_text()
                            if clm_name == require_race_table_clms[3]:
                                links = td.find('a')
                                if links:
                                    race_name = links.attrs['title']
                                    race_url = "https://db.netkeiba.com" + links.attrs['href']
                            if race_date and race_held and race_number \
                                and race_name and race_url:

                                soup = self.get_response(race_url)
                                print('access:%-16s:%s' % (race_name,race_url))
                                race_name, race_date = self.get_RaceName(soup)

                                race_pkey = self.DB.replace_Race_Tbl(race_name,race_date,race_held,race_number,race_url,None)

                                print('レース名:',race_name,'開催日:',race_date)
                                break

        self.DB.replace_request_Tbl(u_link_dict)

        self.DB.commit_DB()
        self.DB.disconnect_DB()

        return u_link_dict

    def analysis_RaceResult(self,soup):
        race_info = dict()

        race_info_title = ['レース名','開催地','開催日','距離','コース種別A','コース種別B','条件A','条件B','周回方向','グレード','芝状態','ダート状態','天候']
        race_name = None
        race_conditions = None

        #====================================
        # レース名、レースの条件等を取得
        isDirection = False #周回方向があるかどうか

        race_title = soup.find('dl',class_='racedata fc')
        if race_title:
            race_title_name = race_title.find('h1')
            if race_title_name:
                race_name = race_title_name.get_text()
            _conditions = race_title.find('span')
            if _conditions:
                _conditions = _conditions.get_text()
                _conditions = _conditions.split('\xa0')
                _conditions = ' '.join(_conditions)
                _conditions = _conditions.split('/')
                _race_conditions_A_ = _race_conditions_B_ = _weather_ = None
                if _conditions:
                    race_conditions = dict()
                    if '障' in _conditions[0]:
                        _course_type_A_ = '障害'
                        _course_type_B_ = '芝/ダート'
                        isDirection = False
                        _distance_ = _conditions[0].lstrip('障芝 ダート')
                    else:
                        _course_type_A_ = '平地'
                        isDirection = True
                        if 'ダ' in _conditions[0]:
                            _dd_ = _conditions[0].lstrip('ダ')
                            _course_type_B_ = 'ダート'
                        elif '芝' in _conditions[0]:
                            _dd_ = _conditions[0].lstrip('芝')
                            _course_type_B_ = '芝'
                        else:
                            return
                        _direction_ = _dd_[0]
                        _distance_ = _dd_[1:]

                    _w_ = _conditions[1].split(':')
                    if len(_w_) >= 2:
                        _weather_ = _w_[1].strip()

                    _ct_ = _conditions[2].split(':')
                    if len(_ct_) == 2:
                        if _course_type_B_ == '芝':
                            _race_conditions_A_ = _ct_[1].strip()
                            _race_conditions_B_ = None
                        elif _course_type_B_ == 'ダート':
                            _race_conditions_A_ = None
                            _race_conditions_B_ = _ct_[1].strip()
                    elif len(_ct_) > 2:
                        _cct_ = _ct_[1].split()
                        if len(_cct_) >= 2:
                            _race_conditions_A_ = _cct_[0].strip()
                        _race_conditions_B_ = _ct_[2].strip()

                    _time_ = _conditions[3].split(':',maxsplit = 1)
                    if _time_:
                        if len(_time_) >= 2:
                            _time_ = _time_[1].strip()
                        else:
                            _time_ = _time_[0]

                    race_conditions.setdefault('レース名',race_name)
                    race_conditions.setdefault('距離',_distance_)
                    race_conditions.setdefault('コース種別A',_course_type_A_)
                    race_conditions.setdefault('コース種別B',_course_type_B_)
                    race_conditions.setdefault('芝状態',_race_conditions_A_)
                    race_conditions.setdefault('ダート状態',_race_conditions_B_)
                    race_conditions.setdefault('天候',_weather_)

        _race_class = soup.find('div',class_='data_intro')

        race_date, race_held, race_class, race_limited = None, None, None, None
        _race_class = _race_class.find('p',class_='smalltxt')
        if _race_class:
            _race_class = _race_class.get_text()
            _race_class = _race_class.split('\xa0\xa0')
            if len(_race_class) >= 2:
                race_date_held_class = _race_class[0]
                r = race_date_held_class.split()
                if len(r) == 3:
                    race_date, race_held, race_class = r
                race_limited = _race_class[1]
                if isinstance(race_conditions,dict):
                    race_conditions.setdefault('条件A',race_class)
                    race_conditions.setdefault('条件B',race_limited)
                    race_conditions.setdefault('開催地',race_held)
                    race_conditions.setdefault('開催日',race_date)

                    grade_chk = race_name[-4:]
                    grade_re = re.compile(r'[(]G[1-3][)]')
                    res = grade_re.match(grade_chk)
                    if res:
                        _grade_ =res.group()
                        race_conditions.setdefault('グレード',_grade_)
                    else:
                        race_conditions.setdefault('グレード',None)

        #====================================

        #====================================
        # レース結果を取得
        race_r_clms, result_data = None, None
        horse_link_list, jockey_link_list, trainer_link_list, owner_link_list = \
            list(), list(), list(),list()

        _race_results_ = soup.find('table',class_='race_table_01 nk_tb_common')
        if _race_results_:
            _race_results_clms = _race_results_.find('tr',class_='txt_c')
            if _race_results_clms:
                _race_results_clms = _race_results_clms.find_all('th')
                if _race_results_clms:
                    race_r_clms = [ clm.get_text() for clm in _race_results_clms]
            _race_results_rows = _race_results_.find_all('tr')
            if _race_results_rows:
                result_data = list()
                for row in _race_results_rows:
                    row_clms = row.find_all('td')
                    if row_clms:
                        set_dict = dict()
                        _result_data_ = list()
                        horse_link, jockey_link, trainer_link, owner_link = None,None,None,None

                        for clm in row_clms:
                            _text_ = clm.get_text().strip()
                            alink = clm.find('a')
                            if alink:
                                link = alink.attrs['href']
                                if '/horse/' in link:
                                    horse_link = "https://db.netkeiba.com" + link
                                    horse_link_list.append({_text_:horse_link})
                                if '/jockey/' in link:
                                    jockey_link = "https://db.netkeiba.com" + link
                                    jockey_link_list.append({_text_:jockey_link})
                                if '/trainer/' in link:
                                    trainer_link = "https://db.netkeiba.com" + link
                                    _t_ = clm.find('a')
                                    if _t_:
                                        _text_ = _t_.get_text()
                                    trainer_link_list.append({_text_:trainer_link})
                                if '/owner/' in link:
                                    owner_link = "https://db.netkeiba.com" + link
                                    owner_link_list.append({_text_:owner_link})
                            _result_data_.append(_text_)
                        for ct, r in zip(race_r_clms,_result_data_):
                            set_dict.setdefault(ct,r)
                        else:
                            result_data.append(set_dict)

        #====================================

        #====================================
        # レース通過順位を取得
        passing_order_list = list()
        _passing_order_ = soup.find('table',class_='result_table_02',summary="コーナー通過順位")
        if _passing_order_:
            _corners_ = _passing_order_.find_all('th')
            _order_ = _passing_order_.find_all('td')
            if _corners_ and _order_:
                for c, o in zip(_corners_,_order_):
                    passing_order_list.append({c.get_text():o.get_text()})

        #====================================
        # レースラップタイムを取得
        lap_time_list = list()
        _lap_time_ = soup.find('table',class_='result_table_02',summary="ラップタイム")
        if _lap_time_:
            _title_ = _lap_time_.find_all('th')
            _time_ = _lap_time_.find_all('td')
            if _title_ and _time_:
                for t, tt in zip(_title_,_time_):
                    lap_time_list.append({t.get_text():tt.get_text()})

        race_info.setdefault('基本情報',race_conditions)
        race_info.setdefault('レース結果',result_data)
        race_info.setdefault('通過順位',passing_order_list)
        race_info.setdefault('ラップタイム',lap_time_list)
        race_info.setdefault('Link',
            {
                '馬':horse_link_list,
                '騎手':jockey_link_list,
                '調教師':trainer_link_list,
                '馬主':owner_link_list,
                }
        )

        #print(race_info)

        return race_info

    def get_RaceResultInfo(self, r_link_dict):
        link_dict = dict()

        while(True):
            keylist = list(r_link_dict.keys())
            if keylist:
                race_pkey = keylist[0]
                race_url = r_link_dict[race_pkey]
                r_link_dict.pop(race_pkey)
            else:
                break

            soup = self.get_response(race_url)
            race_info = self.analysis_RaceResult(soup)

            if isinstance(race_info,dict):
                base_info, passing_info, time_info = None, None, None
                if '基本情報' in race_info:
                    print(race_info['基本情報'])
                    base_info = race_info['基本情報']
                    if '通過順位' in race_info:
                        passing = race_info['通過順位']
                        passing_info = list()
                        keys = ['1コーナー','2コーナー','3コーナー','4コーナー']
                        for corner in passing:
                            k = list(corner.keys())[0]
                            if k in keys:
                                index = keys.index(k)
                                while(len(passing_info) <= index-1):
                                    passing_info.append('')
                            v = list(corner.values())[0]
                            passing_info.append(v)
                    if 'ラップタイム' in race_info:
                        _times_ = race_info['ラップタイム']
                        time_info = list()
                        for _t_ in _times_:
                            v = list(_t_.values())[0]
                            time_info.append(v)
                    self.DB.replace_Request_Race_Tbl(race_pkey,base_info, passing_info, time_info)

                horse_link_list = jockey_link_list = trainer_link_list = owner_link_list = None
                if 'Link' in race_info:
                    Links = race_info['Link']
                    if '馬' in Links:
                        horse_link_list = Links['馬']
                    if '騎手' in Links:
                        jockey_link_list = Links['騎手']
                    if '調教師' in Links:
                        trainer_link_list = Links['調教師']
                    if '馬主' in Links:
                        owner_link_list = Links['馬主']

                if horse_link_list:

                    for horse in horse_link_list:
                        if isinstance(horse,dict):
                            horse_name = list(horse.keys())[0]
                            horse_url = horse[horse_name]
                            horse_pkey = self.DB.replace_Horse_Tbl(horse_name,None,horse_url,None)
                            link_dict.setdefault(horse_name,[horse_url,horse_pkey])

                if jockey_link_list:
                    self.DB.replace_Jockey_Tbl(jockey_link_list)
                if trainer_link_list:
                    self.DB.replace_Trainer_Tbl(trainer_link_list)
                if owner_link_list:
                    self.DB.replace_Owner_Tbl(owner_link_list)

                if 'レース結果' in race_info:
                    race_ranking_info = race_info['レース結果']
                    self.DB.replace_Request_RaceRanking_Tbl(
                        race_pkey,race_ranking_info,
                        horse_link_list,jockey_link_list,trainer_link_list,owner_link_list)
                    pass

        return link_dict

    def run_race_result_info(self):
        self.DB.connect_DB()
        self.DB.create_Race_Tbl()
        self.DB.create_Request_Race_Tbl()
        self.DB.create_Race_Ranking_Tbl()
        self.DB.create_Jockey_Tbl()
        self.DB.create_Owner_Tbl()
        self.DB.create_Trainer_Tbl()
        self.DB.commit_DB()
        self.DB.disconnect_DB()

        RaceNameData = self.DB.get_RaceURL_Tbl()

        r_link_dict = dict()
        for pkey, val in RaceNameData.items():
            r_link_dict.setdefault(pkey,[val['url']])

        u_link_dict = dict()
        if u_link_dict:
            self.DB.set_link_dict(u_link_dict)

        while(True):
            keylist = list(r_link_dict.keys())
            if keylist:
                race_pkey = keylist[0]
                race_url = r_link_dict[race_pkey][0]
                r_link_dict.pop(race_pkey)
            else:
                break


            soup = self.get_response(race_url)
            race_info = self.analysis_RaceResult(soup)

            self.DB.connect_DB()

            if isinstance(race_info,dict):
                base_info, passing_info, time_info = None, None, None
                if '基本情報' in race_info:
                    print(race_info['基本情報'])
                    base_info = race_info['基本情報']
                    if '通過順位' in race_info:
                        passing = race_info['通過順位']
                        passing_info = list()
                        keys = ['1コーナー','2コーナー','3コーナー','4コーナー']
                        for corner in passing:
                            k = list(corner.keys())[0]
                            if k in keys:
                                index = keys.index(k)
                                while(len(passing_info) <= index-1):
                                    passing_info.append('')
                            v = list(corner.values())[0]
                            passing_info.append(v)
                    if 'ラップタイム' in race_info:
                        _times_ = race_info['ラップタイム']
                        time_info = list()
                        for _t_ in _times_:
                            v = list(_t_.values())[0]
                            time_info.append(v)
                    self.DB.replace_Request_Race_Tbl(race_pkey,base_info, passing_info, time_info)

                horse_link_list = jockey_link_list = trainer_link_list = owner_link_list = None
                if 'Link' in race_info:
                    Links = race_info['Link']
                    if '馬' in Links:
                        horse_link_list = Links['馬']
                    if '騎手' in Links:
                        jockey_link_list = Links['騎手']
                    if '調教師' in Links:
                        trainer_link_list = Links['調教師']
                    if '馬主' in Links:
                        owner_link_list = Links['馬主']

                if horse_link_list:
                    link_dict = dict()

                    for horse in horse_link_list:
                        if isinstance(horse,dict):
                            horse_name = list(horse.keys())[0]
                            horse_url = horse[horse_name]
                            horse_pkey = self.DB.replace_Horse_Tbl(horse_name,None,horse_url,None)
                            link_dict.setdefault(horse_name,[horse_url,horse_pkey])

                    u_link_dict.update(link_dict)

                    self.DB.replace_request_Tbl(u_link_dict)

                if jockey_link_list:
                    self.DB.replace_Jockey_Tbl(jockey_link_list)
                if trainer_link_list:
                    self.DB.replace_Trainer_Tbl(trainer_link_list)
                if owner_link_list:
                    self.DB.replace_Owner_Tbl(owner_link_list)

                if 'レース結果' in race_info:
                    race_ranking_info = race_info['レース結果']
                    self.DB.replace_Request_RaceRanking_Tbl(
                        race_pkey,race_ranking_info,
                        horse_link_list,jockey_link_list,trainer_link_list,owner_link_list)
                    pass

            self.DB.commit_DB()
            self.DB.disconnect_DB()
        return

if __name__ == '__main__':
    hScr = HorseData_scraping(limit_time=3000)
    
    start_url = "https://db.netkeiba.com/horse/2002100816/" #ディープインパクト
    #start_url = "https://db.netkeiba.com/horse/000a015efd/" #Old Bald Peg 簡易血統表なし
    #start_url = "https://db.netkeiba.com/horse/000a015b48/" #Frolic 母母Sister 2 to Blastのプロフィールページ異常
    #start_url = "https://db.netkeiba.com/horse/000a016741/" #Northumberland Arabian Mareのプロフィールページ異常

    hScr.run(start_url)
    '''
    #hScr.run_race_info()

    #start_url = "https://db.netkeiba.com/race/200606050809/" #第51回有馬記念(G1)
    #start_url = "https://db.netkeiba.com/race/200609010804/" #障害4歳以上オープン
    #soup = hScr._temp_FileIO(start_url,'wr')
    #hScr.analysis_RaceResult(soup)

    hScr.run_race_result_info()
    '''



