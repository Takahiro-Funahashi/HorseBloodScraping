import cchardet
from bs4 import BeautifulSoup

from fake_useragent import UserAgent

import dill
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

    def get_RaceInfo(self,horse_url,horse_name,u_link_dict):
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


        '''
        HorseNameData = self.DB.get_HorseURL_Tbl(limit_number = 1)

        for horse_name, val in HorseNameData.items():
            url = val['url']

            response = requests.get(url,headers=self.header,timeout=self.time_out)
            response.encoding = response.apparent_encoding
            enc = cchardet.detect(response.content)
            encoding = enc['encoding']

            print('access:%-16s:%s' % (horse_name,url))

            with open('_temp_soup_.pickle', 'wb') as fp:
                dill.dump(response,fp)
        '''
        '''
        with open('_temp_soup_.pickle', 'rb') as fp:
            response = dill.load(fp)
            soup = BeautifulSoup(response.text, "html.parser")
        '''

        return u_link_dict

if __name__ == '__main__':
    '''
    #start_url = "https://db.netkeiba.com/horse/2002100816/"
    #start_url = "https://db.netkeiba.com/horse/000a015efd/"
    #start_url = "https://db.netkeiba.com/horse/000a00111d/"
    #start_url = "https://db.netkeiba.com/horse/000a015b48/"
    start_url = "https://db.netkeiba.com/horse/1990103355/"

    hScr = HorseData_scraping(limit_time=3000)
    hScr.run(start_url)
    '''
    hScr = HorseData_scraping(limit_time=3000)
    hScr.run_race_info()
