import json
from io import BytesIO
from datetime import date

from loader import PZLoader, YandexMapsLoader, Processor
from S3 import S3
import config as cfg

def objToBytesIO(o: any):
    return BytesIO(json.dumps(o, ensure_ascii=False).encode())

def main():

    # объект загрузчик в S3
    S3O = S3({
        'ACCESS_KEY_ID': cfg.ycloud.ACCESS_KEY_ID,
        'SECRET_ACCESS_KEY': cfg.ycloud.SECRET_ACCESS_KEY,
        'ENDPOINT_URL': cfg.ycloud.endpointUrl
    })

    #подгружаем изначальные данные
    o = PZLoader(cfg.urls.pz_auth)

    stage1cfg = cfg.urls.eda
    data = o.getStage1Data(stage1cfg)

    #обработка полученных данных
    p = Processor(data)
    data = p.parseStage1Data()

    S3O.uploadFile(
        objToBytesIO(data),
        cfg.ycloud.bucketName,
        'sales.json'
    )

    # #делаем еще раз запросы с уточнением адресов ресторанов
    data = o.getStage2Data(data)

    #обрабатываем полученные данные
    p = Processor(data)
    data = p.parseStage2Data()

    S3O.uploadFile(
        objToBytesIO(data),
        cfg.ycloud.bucketName,
        'addresses.json'
    )

    # получаем данные по точкам из Яндекс Карт
    # не делая запросы по тем адресам, которые уже известны

    dataUsed = S3O.getObj(
        cfg.ycloud.bucketName,
        'addresses_coords.json'
    )

    #2 части: первая - где отсутствуют данные, т.е. новые адреса, вторая - где этих адресов несколько
    data1 = list(filter(lambda x: x['address_txt'] not in dataUsed, data))
    data2 = list(filter(lambda x: x['is_multiple_address'], data))
    data = data1 + data2

    y = YandexMapsLoader(cfg.urls.ya_geocode)
    data = y.getLocationsData(data)

    data = {**dataUsed, **data}

    S3O.uploadFile(
        objToBytesIO(data),
        cfg.ycloud.bucketName,
        'addresses_coords.json'
    )

    # обрабатываем полученные данные в финальный файл
    p = Processor(data)
    data = p.parseAddressCoordsToFinalData()

    S3O.uploadFile(
        objToBytesIO(data),
        cfg.ycloud.finalBucketName,
        'data/data.json'
    )

    #ставим в файл config.json последнюю дату обновления
    S3O.uploadFile(
        objToBytesIO({
            'updated_at': date.today().isoformat()
        }),
        cfg.ycloud.finalBucketName,
        'data/config.json'
    )