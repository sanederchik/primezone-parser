from secret.secret import login, password, yandex_api_key

BASE_ENCODING = 'utf-8'
PZ_HOST = 'accenture.primezone.ru'
PZ_BASE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded",
    "pragma": "no-cache",
    "sec-ch-ua": "\" Not;A Brand\";v=\"99\", \"Google Chrome\";v=\"97\", \"Chromium\";v=\"97\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "sec-gpc": "1",
    "upgrade-insecure-requests": "1"
  }

YA_BASE_HEADERS = {
    "content-type": "application/json"
}

PZ_AUTH_BODY = {
    'user[email]': login,
    'user[password]': password
}

class urls:

    pz_auth = {
        'method': 'POST',
        'url': f'https://{PZ_HOST}/users/sign_in',
        'headers': PZ_BASE_HEADERS,
        'body': PZ_AUTH_BODY
    }

    eda = {
        'method': 'GET',
        'host': PZ_HOST,
        'url': f'https://{PZ_HOST}/eda',
        'queryParams': {
            'cities': ['Moscow']
        }
    }

    ya_geocode = {
        'method': 'GET',
        'url': f'https://geocode-maps.yandex.ru/1.x?apikey={yandex_api_key}&format=json&rspn=1',
        'headers': YA_BASE_HEADERS,
        'limit': 100
    }

SLEEP_SEC_AMONG_REQUESTS = 1


CITIES = {
    'Moscow': {
        'name': 'Москва',
        'x1y1': '36.839341,56.109413',
        'x2y2': '38.169894,55.103048'
    }
}

class roots:
    data_root = 'data'