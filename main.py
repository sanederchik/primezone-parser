import json

from loader import PZLoader, YandexMapsLoader, Processor
import config as cfg

def main():

    #подгружаем изначальные данные
    o = PZLoader(cfg.urls.pz_auth)

    stage1cfg = cfg.urls.eda
    data = o.getStage1Data(stage1cfg)

    #обработка полученных данных
    p = Processor(data)
    data = p.parseStage1Data()

    with open(f'{cfg.roots.data_root}/sales.json', encoding=cfg.BASE_ENCODING, mode='w+') as f:
        json.dump(data, f, ensure_ascii=False)
        f.close()

    # #делаем еще раз запросы с уточнением адресов ресторанов
    data = o.getStage2Data(data)

    #обрабатываем полученные данные
    p = Processor(data)
    data = p.parseStage2Data()

    with open(f'{cfg.roots.data_root}/addresses.json', encoding=cfg.BASE_ENCODING, mode='w+') as f:
        json.dump(data, f, ensure_ascii=False)
        f.close()

    #получаем данные по точкам из Яндекс Карт
    #не делая запросы по тем адресам, которые уже известны
    with open(f'{cfg.roots.data_root}/addresses.json', mode='r', encoding=cfg.BASE_ENCODING) as f:
        data = json.load(f)
        f.close()

    with open(f'{cfg.roots.data_root}/addresses_coords.json', mode='r', encoding=cfg.BASE_ENCODING) as f:
        dataUsed = json.load(f)
        f.close()

    #2 части: первая - где отсутствуют данные, т.е. новые адреса, вторая - где этих адресов несколько
    data1 = list(filter(lambda x: x['address_txt'] not in dataUsed, data))
    data2 = list(filter(lambda x: x['is_multiple_address'], data))
    data = data1 + data2

    y = YandexMapsLoader(cfg.urls.ya_geocode)
    data = y.getLocationsData(data)

    data = {**dataUsed, **data}

    with open(f'{cfg.roots.data_root}/addresses_coords.json', encoding=cfg.BASE_ENCODING, mode='w+') as f:
        json.dump(data, f, ensure_ascii=False)
        f.close()

    # обрабатываем полученные данные в финальный файл
    p = Processor(data)
    data = p.parseAddressCoordsToFinalData()

    with open(f'{cfg.roots.data_root}/data.json', encoding=cfg.BASE_ENCODING, mode='w+') as f:
        json.dump(data, f, ensure_ascii=False)
        f.close()

if __name__ == '__main__':
    main()