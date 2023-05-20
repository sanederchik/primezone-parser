import config as cfg
import requests
from bs4 import BeautifulSoup
import re
from time import sleep
from copy import deepcopy
from hashlib import md5
import json

class PZLoader:

    def __init__(self, authObj: object):
        self.authObj = authObj
        self.auth()

    def auth(self):

        _msg = ''

        r = requests.request(
            method = self.authObj['method'],
            url = self.authObj['url'],
            headers = self.authObj['headers'],
            data =  self.authObj['body']
        )

        if r.status_code == 200:
            self.authObj['headers'].update({
                'cookie': ';'.join([k+'='+v for k, v in r.cookies.items()])
            })

            return self

        else:
            raise ValueError(self._errMsg(r))

    def loadPage(self, reqObj: object, queryParams: dict = {}, sleepSec: int = 0):

        if queryParams == {}:
            _url = deepcopy(reqObj['url'])
        else:
            _url = reqObj['url'] + '?' + '&'.join([k+'='+str(v) for k, v in queryParams.items()])

        r = requests.request(
            method=reqObj['method'],
            url=_url,
            headers=reqObj['headers'],
            data=reqObj['body']
        )

        sleep(sleepSec)

        if r.status_code == 200:

            print(f'Данные страницы {_url} успешно получены')
            return r

        else:
            raise ValueError(self._errMsg(r))

    def _errMsg(self, r):
        return 'Ошибка! Дополнительные данные: \n' + \
                f'Статус запроса: {str(r.status_code)} \n ' + \
                f'Текст ошибки: {r.text}'


    def getStage1Data(self, cfgObj: object):

        host = cfgObj['host']
        data = {host: {}}
        reqObj = {}

        for city in cfgObj['queryParams']['cities']:

            _p = 1
            lastP = 100
            data[host][city] = []

            while True:

                reqObj = {
                    'method': cfgObj['method'],
                    'url': cfgObj['url'],
                    'headers': self.authObj['headers'],
                    'body': None
                }

                r = self.loadPage(
                    reqObj,
                    {
                        'city': city,
                        'page': _p
                    }
                )

                data[host][city].append(r.text)

                # определяем последнюю страницу
                if _p == 1:
                    bs = BeautifulSoup(r.text, 'html.parser')
                    lastP = int(bs.find('li', {'class': 'last'}).find('a')['href'].rpartition('page=')[-1])

                # условие выхода
                if _p >= lastP:
                    break

                _p += 1

        return data

    #данные по адресам
    def getStage2Data(self, data: list):

        res = {}

        for l1 in data:

            _url = l1['sale_add_info_url']

            reqObj = {
                'method': 'GET',
                'url': _url,
                'headers': self.authObj['headers'],
                'body': None
            }

            r = self.loadPage(
                reqObj,
                sleepSec=cfg.SLEEP_SEC_AMONG_REQUESTS
            )

            res[_url] = {
                'ctx': l1,
                'txt': r.text
            }

        return res

class YandexMapsLoader:

    def __init__(self, authObj: object):
        self.authObj = authObj
        self.auth()

    def auth(self):
        return self

    def makeAPIRequest(self, reqObj: object, queryParams: dict = {}, sleepSec: int = 0):

        if queryParams == {}:
            _url = deepcopy(reqObj['url'])
        else:
            _url = reqObj['url'] + '&' + '&'.join([k+'='+str(v) for k, v in queryParams.items()])

        r = requests.request(
            method=reqObj['method'],
            url=_url,
            headers=reqObj['headers'],
            data=reqObj['body']
        )

        sleep(sleepSec)

        if r.status_code == 200:

            print(f'Данные страницы {_url} успешно получены')
            return r

        else:
            raise ValueError(self._errMsg(r))

    def getLocationsData(self, data: list, nRes: int = 1):

        res = {}

        for l1 in data:

            _cityObj = cfg.CITIES[l1['ctx']['city_name']]
            _bbox = f'{_cityObj["x1y1"]}~{_cityObj["x2y2"]}'

            reqObj = {
                'method': 'GET',
                'url': self.authObj['url'],
                'headers': self.authObj['headers'],
                'body': None
            }

            if l1['address_txt'] in res:
                continue

            if l1['is_multiple_address']:

                _res = []
                _skip = 0
                _limit = self.authObj['limit']
                _maxEnd = nRes


                while True:
                    r = self.makeAPIRequest(
                        reqObj,
                        queryParams={
                            'geocode': l1['address_txt'],
                            'results': _limit,
                            'skip': _skip,
                            'bbox': _bbox
                        }
                    )

                    if r.status_code != 200:
                        raise ValueError(self._errMsg(r))

                    r = r.json()

                    if _skip == 0:
                        _maxEnd = int(r['response']['GeoObjectCollection']['metaDataProperty']['GeocoderResponseMetaData']['results'])

                    _skip += _limit
                    _res.append(r)

                    #условие выхода
                    if _skip >= _maxEnd:
                        break

                res[l1['address_txt']] = {
                    'res': deepcopy(_res),
                    'ctx': l1['ctx']
                }

            else:

                r = self.makeAPIRequest(
                    reqObj,
                    queryParams={
                        'geocode': l1['address_txt'],
                        'results': 1,
                        'bbox': _bbox
                    }
                )

                res[l1['address_txt']] = {
                    'res': [r.json()],
                    'ctx': l1['ctx']
                }

        return res

    def _errMsg(self, r):
        return 'Ошибка! Дополнительные данные: \n' + \
                f'Статус запроса: {str(r.status_code)} \n ' + \
                f'Текст ошибки: {r.text}'


class Processor:

    def __init__(self, data):
        self.data = data

    def parseStage1Data(self):

        data = []

        for host, l0 in self.data.items():
            for city, l1 in l0.items():
                for l2 in l1:
                    bs = BeautifulSoup(l2, 'html.parser')

                    l3 = bs.find('div', {'class': 'row deal-containers-list'})
                    l3 = l3.find_all('a', {'class': 'coupon-thumb'})

                    for l4 in l3:
                        data.append({
                            'host': host,
                            'org_name': l4.select_one('.coupon-title').text,
                            'city_name': city,
                            'org_img_url': l4.select_one('.deal-img-inner').find('img')['src'],
                            'sale_desc': l4.select_one('.coupon-desciption').text, #опечатка в источнике в слове description
                            'sale_perc': self._getSaleData(l4.select_one('.coupon-save').text),
                            'sale_add_info_url': f'https://{host}{l4["href"]}?city={city}'
                        })

        return data

    def parseStage2Data(self):

        data = []

        for req, l1 in self.data.items():
            bs = BeautifulSoup(l1['txt'], 'html.parser')

            l2 = bs.find('div', {'class': 'active city-with-spots row'})

            #если не нашел, то ставим None, в дальнейшем считаем, что действует во всех местах
            if l2 is None:
                data.append({
                    'url': req,
                    'is_multiple_address': True,
                    'address_txt': cfg.CITIES[l1['ctx']['city_name']]['name'] + ' ' + l1['ctx']['org_name'],
                    'address_subways_txt': None,
                    'ctx': l1['ctx']
                })

                continue

            l3 = l2.find_all('div', {'class': 'deals-location'})

            for l4 in l3:
                data.append({
                    'url': req,
                    'is_multiple_address': False,
                    'address_txt': l4.select_one('.deals-location-address').text.replace('\n', ''),
                    'address_subways_txt': l4.select_one('.deals-location-subways').text.replace('\n', ''),
                    'ctx': l1['ctx']
                })

        return data

    def parseLocationsData(self):

        data = []

        for req, l1 in self.data.items():
            bs = BeautifulSoup(l1, 'html.parser')

            l2 = bs.find('div', {'class': 'active city-with-spots row'})

            #если не нашел, то ставим None, в дальнейшем считаем, что действует во всех местах
            if l2 is None:
                data.append({
                    'url': req,
                    'address_txt': None,
                    'address_subways_txt': None
                })

                continue

            l3 = l2.find_all('div', {'class': 'deals-location'})

            for l4 in l3:
                data.append({
                    'url': req,
                    'address_txt': l4.select_one('.deals-location-address').text.replace('\n', ''),
                    'address_subways_txt': l4.select_one('.deals-location-subways').text.replace('\n', '')
                })

        return data

    def parseAddressCoordsToFinalData(self):

        data = {}

        for k1, l1 in self.data.items():
            for l2 in l1['res']:
                for l3 in l2['response']['GeoObjectCollection']['featureMember']:

                    _coords = l3['GeoObject']['Point']['pos'].split(' ')

                    coordsObj = {
                        'url': l1['ctx']['sale_add_info_url'],
                        'address_txt': k1,
                        'coords': [float(_coords[1]), float(_coords[0])]
                    }

                    finalObj = {**coordsObj, **l1['ctx']}
                    key = md5('_'.join([finalObj['sale_add_info_url'], json.dumps(finalObj['coords']), finalObj['address_txt']]).encode('utf-8')).hexdigest()
                    data[key] = finalObj

        return data

    def _getSaleData(self, txt: str, onlyNumber: bool = True):
        try:
            saleStr = re.findall('(\d{1,2}\%)', txt)[0]

            if onlyNumber:
                return int(re.findall('(\d{1,2})', saleStr)[0])

            return saleStr
        except:
            return None