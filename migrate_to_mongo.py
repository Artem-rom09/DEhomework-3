import mysql.connector
from mysql.connector import Error
import pymongo
import pandas as pd
import os
from collections import defaultdict
from datetime import datetime

# --- КОНФІГУРАЦІЯ ---
MYSQL_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_DATABASE', 'my_ad_data'),
    'user': os.getenv('DB_USER', 'me'),
    'password': os.getenv('DB_PASSWORD', 'artem228')
}
MONGO_CONFIG = {
    'host': os.getenv('MONGO_HOST', 'localhost'),
    'port': int(os.getenv('MONGO_PORT', 27017)), 
    'db_name': os.getenv('MONGO_DB_NAME', 'ad_engagement_db'),
    'username': os.getenv('MONGO_USER', 'root'),
    'password': os.getenv('MONGO_PASSWORD', 'example')
}
MONGO_COLLECTION_NAME = 'user_interactions'


def connect_mysql():
    """Встановлює з'єднання з базою даних MySQL."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            print("Успішно підключено до MySQL.")
            return conn
    except Error as e:
        print(f"Помилка при підключенні до MySQL: {e}")
        return None

def connect_mongo():
    """Встановлює з'єднання з MongoDB."""
    try:
        # Створення URI для підключення з аутентифікацією
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = pymongo.MongoClient(uri)
        # Перевірка з'єднання
        client.admin.command('ping')
        print("Успішно підключено до MongoDB.")
        db = client[MONGO_CONFIG['db_name']]
        return db
    except pymongo.errors.OperationFailure as e:
        print(f"Помилка аутентифікації MongoDB: {e}. Перевірте ім'я користувача та пароль.")
        return None
    except pymongo.errors.ConnectionFailure as e:
        print(f"Помилка при підключенні до MongoDB: {e}")
        return None

def fetch_data_from_mysql(conn):
    """Витягує та об'єднує дані з усіх таблиць MySQL."""
    print("Витяг даних з MySQL...")
    query = """
    SELECT
        u.UserID, u.Age, u.Gender, u.Location AS UserLocation, u.SignupDate,
        GROUP_CONCAT(DISTINCT inte.InterestName SEPARATOR ',') AS Interests,
        imp.ImpressionID, imp.Device, imp.Location AS ImpressionLocation, imp.Timestamp AS ImpressionTimestamp, imp.AdCost,
        camp.CampaignID, camp.CampaignName, camp.TargetingCriteria AS CampaignCategory,
        adv.AdvertiserID, adv.AdvertiserName,
        c.ClickID, c.ClickTimestamp, c.AdRevenue
    FROM
        Users u
    LEFT JOIN UserInterests ui ON u.UserID = ui.UserID
    LEFT JOIN Interests inte ON ui.InterestID = inte.InterestID
    LEFT JOIN Impressions imp ON u.UserID = imp.UserID
    LEFT JOIN Campaigns camp ON imp.CampaignID = camp.CampaignID
    LEFT JOIN Advertisers adv ON camp.AdvertiserID = adv.AdvertiserID
    LEFT JOIN Clicks c ON imp.ImpressionID = c.ImpressionID
    WHERE
        imp.ImpressionID IS NOT NULL
    GROUP BY
        u.UserID, imp.ImpressionID, c.ClickID
    ORDER BY
        u.UserID, imp.Timestamp;
    """
    try:
        # Використання SQLAlchemy для уникнення попередження
        from sqlalchemy import create_engine
        engine = create_engine(f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}")
        df = pd.read_sql(query, engine)
        print(f"Отримано {len(df)} записів з MySQL.")
        return df
    except Exception as e:
        print(f"Помилка при виконанні SQL-запиту: {e}")
        return pd.DataFrame()

def transform_data_for_mongo(df):
    """Трансформує плаский DataFrame у вкладену структуру для MongoDB."""
    print("Трансформація даних для MongoDB...")
    users_data = defaultdict(lambda: {
        'user_details': {},
        'sessions': []
    })

    for user_id, user_group in df.groupby('UserID'):
        first_row = user_group.iloc[0]
        users_data[user_id]['_id'] = int(user_id)
        users_data[user_id]['user_details'] = {
            'age': int(first_row['Age']) if pd.notna(first_row['Age']) else None,
            'gender': first_row['Gender'],
            'location': first_row['UserLocation'],
            'signup_date': first_row['SignupDate'],
            'interests': first_row['Interests'].split(',') if pd.notna(first_row['Interests']) else []
        }

        impressions_by_session = defaultdict(list)
        for _, row in user_group.iterrows():
            impression_doc = {
                'impression_id': row['ImpressionID'],
                'timestamp': row['ImpressionTimestamp'],
                'campaign_details': {
                    'campaign_id': int(row['CampaignID']) if pd.notna(row['CampaignID']) else None,
                    'campaign_name': row['CampaignName'],
                    'advertiser_id': int(row['AdvertiserID']) if pd.notna(row['AdvertiserID']) else None,
                    'advertiser_name': row['AdvertiserName'],
                    'category': row['CampaignCategory']
                },
                'ad_cost': float(row['AdCost']) if pd.notna(row['AdCost']) else 0,
                'clicked': pd.notna(row['ClickID']),
                'click_details': {}
            }
            if impression_doc['clicked']:
                impression_doc['click_details'] = {
                    'click_id': int(row['ClickID']) if pd.notna(row['ClickID']) else None,
                    'click_timestamp': row['ClickTimestamp'],
                    'ad_revenue': float(row['AdRevenue']) if pd.notna(row['AdRevenue']) else 0
                }
            session_key = (row['Device'], row['ImpressionTimestamp'].strftime('%Y-%m-%d'))
            impressions_by_session[session_key].append(impression_doc)

        for (device, _), impressions in impressions_by_session.items():
            if not impressions: continue
            impressions.sort(key=lambda x: x['timestamp'])
            session_doc = {
                'session_id': f"session_{impressions[0]['timestamp'].timestamp()}",
                'start_time': impressions[0]['timestamp'],
                'end_time': impressions[-1]['timestamp'],
                'device': device,
                'impressions': impressions
            }
            users_data[user_id]['sessions'].append(session_doc)
            
    print(f"Сформовано документи для {len(users_data)} користувачів.")
    return list(users_data.values())

def load_data_to_mongo(db, data):
    """Завантажує дані в колекцію MongoDB."""
    if not data:
        print("Немає даних для завантаження.")
        return

    collection = db[MONGO_COLLECTION_NAME]
    print(f"Очищення колекції '{MONGO_COLLECTION_NAME}'...")
    collection.delete_many({})

    print(f"Завантаження {len(data)} документів у колекцію...")
    try:
        collection.insert_many(data, ordered=False)
        print("Дані успішно завантажено в MongoDB.")
    except pymongo.errors.BulkWriteError as bwe:
        print("Сталися помилки під час масового запису, але деякі документи могли бути вставлені.")
        print(f"Деталі: {bwe.details}")
    except Exception as e:
        print(f"Помилка під час завантаження даних у MongoDB: {e}")

def main():
    """Головна функція для запуску процесу міграції."""
    mysql_conn = connect_mysql()
    mongo_db = connect_mongo()

    if mysql_conn is not None and mongo_db is not None:
        data_df = fetch_data_from_mysql(mysql_conn)
        if not data_df.empty:
            transformed_data = transform_data_for_mongo(data_df)
            load_data_to_mongo(mongo_db, transformed_data)

        mysql_conn.close()
        print("З'єднання з MySQL закрито.")

if __name__ == '__main__':

    main()
