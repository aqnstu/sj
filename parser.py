# coding: utf-8
from loguru import logger
import concurrent.futures
import datetime
import pandas as pd
import requests as r
import sys
import timeit
# ? https://github.com/ThibTrip/pangres/wiki/Upsert

from configs import SJ, DB

logger.add(
    'parser.log',
    backtrace=True,
    diagnose=True,
    level='DEBUG',
    encoding='utf-8'
    
)

def get_access_token(auth_params: dict) -> str:
    """
    Получить токен доступа к данными API SuperJob.
    
    Параметры
    ---------
    auth_params : dict
        Словарь с данными для авторизации в API SuperJob: "login", "password", "client_id", "client_secret".

    Возвращаемые значения
    ---------------------
    str
        Токен для получения информации через API SuperJob.
    """
    response = r.get('https://api.superjob.ru/2.20/oauth2/password/', auth_params)
    response.raise_for_status()
    return response.json()['access_token']


def get_vacancies(access_token: str, client_secret: str, catalogues_id: int, page: int) -> list:
    """
    Получить вакансии с страницы <page> выдачи API SuperJob по всем каталогам (отраслям).
    
    Параметры
    ---------
    access_token : str
        Токен для получения информации через API SuperJob.
    client_secret : str
        Ключ клиентского приложения SuperJob.
    catalogues_id: int
        ID отрасли в фильтре выдачи API.
    page: int
        Порядковый номер страницы выдачи в API SuperJob {0, ..., 4}.

    Возвращаемые значения
    ---------------------
    list
        Список вакансий в формате "записей": [{...} {...}, {...}, ...].
    """
    vacs_list = []
    for catalogue_id in catalogues_id:
        headers = {
            'X-Api-App-Id': client_secret,
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'peroid':       0,
            'town':         13,
            'count':        100,
            'catalogues':   catalogue_id,
            'page':         page
        }

        response = r.get(
            'https://api.superjob.ru/2.20/vacancies/',
            headers=headers,
            params=params
        )
        vacs_list.extend(response.json()['objects'])
    return vacs_list


def main():
    try:
        access_token = get_access_token(auth_params=SJ)
    except:
        logger.exception('Проблема с получение access_token')
        sys.exit(1)
    
    try:
        # ID отраслей в фильтре API SuperJob
        catalogues_id = [
            1, 11, 33,
            62, 76, 86,
            100, 136, 151,
            175, 182, 197,
            205, 222, 234,
            260, 270, 284,
            306, 327, 362,
            381, 414, 426,
            438, 471, 478,
            505, 512, 548
        ]
        vacs_list = []
        start_time = timeit.default_timer()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            result_futures = list(
                map(
                    lambda x: executor.submit(
                        get_vacancies,
                        access_token,
                        SJ['client_secret'],
                        catalogues_id,
                        x
                    ),
                    range(5)
                )
            )
        for future in concurrent.futures.as_completed(result_futures):
            vacs_list.extend(future.result())
        df = pd.DataFrame.from_records(vacs_list).drop_duplicates(subset=['id'])
        end_time = timeit.default_timer()
        logger.info(f'Время получения данные с SuperJob: {end_time - start_time} сек. Всего вакансий: {df.shape[0]}')
    except:
        logger.exception('Проблема с полученим вакансий с API SuperJob')
        sys.exit(2)
        
    try:
        vacs_df = df[[
            'id', 'id_client',
            'profession', 'candidat',
            'work', 'compensation', 
            'education','experience',
            'type_of_work', 'place_of_work',
            'maritalstatus', 'children',
            'gender', 'driving_licence',
            'age_from', 'age_to', 
            'moveable', 'agreement', 'agency',
            'town', 'payment_from',
            'payment_to', 'currency',
            'address', 'latitude',
            'longitude', 'metro',
            'link', 'date_pub_to',
            'date_published', 'date_archived',
            'is_closed', 'catalogues'
        ]]
        vacs_df.education = vacs_df.education.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.experience = vacs_df.experience.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.type_of_work = vacs_df.type_of_work.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.place_of_work = vacs_df.place_of_work.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.maritalstatus = vacs_df.maritalstatus.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.children = vacs_df.children.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.gender = vacs_df.gender.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.driving_licence = vacs_df.driving_licence.apply(lambda x: ', '.join(x) if len(x) else None)
        vacs_df['catalogues_id'] = vacs_df.catalogues.apply(lambda x: '; '.join([str(d['id']) for d in x]) if x else None)
        vacs_df['catalogues_name'] = vacs_df.catalogues.apply(lambda x: '; '.join([d['title'] for d in x]) if x else None)
        vacs_df.agency = vacs_df.agency.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.town = vacs_df.town.apply(lambda x: x.get('title') if not pd.isnull(x) else None)
        vacs_df.payment_from = vacs_df.payment_from.apply(lambda x: x if not pd.isnull(x) != 0 else None)
        vacs_df.payment_from = vacs_df.payment_from.astype('Int64')
        vacs_df.payment_to = vacs_df.payment_to.apply(lambda x: x if not pd.isnull(x) != 0 else None)
        vacs_df.payment_to = vacs_df.payment_to.astype('Int64')
        vacs_df.metro = vacs_df.metro.apply(lambda x: '; '.join([d['title'] for d in x]) if x else None)
        vacs_df.date_pub_to = vacs_df.date_pub_to.apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        vacs_df.date_published = vacs_df.date_published.apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        vacs_df.date_archived = vacs_df.date_archived.apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
        vacs_df['download_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        orgs_df = df[['client', 'client_logo']]
        orgs_df['id'] = orgs_df.client.apply(lambda x: x.get('id') if not pd.isnull(x) else None)
        orgs_df.id = orgs_df.id.astype('Int64')
        orgs_df['name'] = orgs_df.client.apply(lambda x: x.get('title')if not pd.isnull(x) else None)
        orgs_df['description'] = orgs_df.client.apply(lambda x: x.get('description') if not pd.isnull(x) else None)
        orgs_df['vacancy_count'] = orgs_df.client.apply(lambda x: x.get('vacancy_count') if not pd.isnull(x) else None)
        orgs_df.vacancy_count = orgs_df.vacancy_count.astype('Int64')
        orgs_df['staff_count'] = orgs_df.client.apply(lambda x: x.get('staff_count') if not pd.isnull(x) else None)
        orgs_df['client_logo'] = orgs_df.client.apply(lambda x: x.get('client_logo') if not pd.isnull(x) else None)
        orgs_df['main_address'] = orgs_df.client.apply(lambda x: x.get('address') if not pd.isnull(x) else None)
        orgs_df['addresses'] = orgs_df.client.apply(lambda x: x.get('addresses') if not pd.isnull(x) else None)
        orgs_df['url'] = orgs_df.client.apply(lambda x: x.get('url') if not pd.isnull(x) else None)
        orgs_df['link'] = orgs_df.client.apply(lambda x: x.get('link') if not pd.isnull(x) else None)
        orgs_df['registered_date'] = orgs_df.client.apply((lambda x: x.get('registered_date') if not pd.isnull(x) else None))
        orgs_df.registered_date = orgs_df.registered_date.apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else None)
        orgs_df['download_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        orgs_df = orgs_df.drop(columns=['client', 'client_logo'])
        print(orgs_df)
    except:
        logger.exception('Проблема с формированием новых датафреймов')
        sys.exit(2)

if __name__ == "__main__":
    main()