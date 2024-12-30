import json
import requests
from bs4 import BeautifulSoup
from airflow.providers.postgres.hooks.postgres import PostgresHook

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'
POSTGRES_CONN_ID = 'postgres_default'

def get_wikipedia_page(url):
    print("Getting wikipedia page...", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Check if status is successful
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch wikipedia page: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error fetching wikipedia page: {e}")
        return None

def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable"})[1]
    table_rows = table.find_all('tr')
    return table_rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp;', '')
    if '♦' in text:
        text = text.split('♦')[0]
    if '[' in text:
        text = text.split('[')[0]
    if 'formerly' in text:
        text = text.split(' (formerly)')[0]
    return text.replace('\n', '')

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    if html is None:
        return []
    rows = get_wikipedia_data(html)
    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'image': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else NO_IMAGE,
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='extracted_data', value=json_rows)
    return "OK"

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)

    transformed_data = []
    for entry in data:
        transformed_entry = {
            'rank': entry['rank'],
            'stadium': entry['stadium'],
            'capacity': int(entry['capacity']),
            'region': entry['region'],
            'country': entry['country'],
            'city': entry['city'],
            'image': entry['image'],
            'home_team': entry['home_team'],
        }
        transformed_data.append(transformed_entry)
    json_transformed_data = json.dumps(transformed_data)
    kwargs['ti'].xcom_push(key='transformed_data', value=json_transformed_data)
    return "OK"

def load_wikipedia_data(**kwargs):
    """Load transformed data into PostgreSQL"""
    data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_wikipedia_data')
    data = json.loads(data)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS stadiums (
        rank INT,
        stadium VARCHAR(255),
        capacity INT,
        region VARCHAR(255),
        country VARCHAR(255),
        city VARCHAR(255),
        image TEXT,
        home_team VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    insert_query = """
    INSERT INTO stadiums (rank, stadium, capacity, region, country, city, image, home_team)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    for entry in data:
        try:
            cursor.execute(insert_query, (
                entry['rank'],
                entry['stadium'],
                entry['capacity'],
                entry['region'],
                entry['country'],
                entry['city'],
                entry['image'],
                entry['home_team']
            ))
        except Exception as e:
            print(f"Error inserting data: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    return "OK"


