import pymongo
import json
from datetime import datetime, timedelta

# --- КОНФІГУРАЦІЯ ---
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'db_name': 'ad_engagement_db',
    'username': 'root',
    'password': 'example' 
}
MONGO_COLLECTION_NAME = 'user_interactions'

def connect_mongo():
    """Встановлює з'єднання з MongoDB."""
    try:
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = pymongo.MongoClient(uri)
        client.admin.command('ping')
        print("Успішно підключено до MongoDB.")
        db = client[MONGO_CONFIG['db_name']]
        return db[MONGO_COLLECTION_NAME]
    except pymongo.errors.OperationFailure as e:
        print(f"Помилка аутентифікації MongoDB: {e}. Перевірте ім'я користувача та пароль.")
        return None
    except pymongo.errors.ConnectionFailure as e:
        print(f"Помилка при підключенні до MongoDB: {e}")
        return None

def pretty_print_json(data):
    """Виводить JSON у читабельному форматі."""
    if isinstance(data, list):
        if not data:
            print("Даних не знайдено (порожній список).")
            return
        for item in data:
            print(json.dumps(item, indent=2, default=str))
    elif data:
        print(json.dumps(data, indent=2, default=str))
    else:
        print("Даних не знайдено (None).")

# --- ЗАВДАННЯ ---

def task_1_get_user_interaction_history(collection, user_id=713):
    """1. Отримати всю історію взаємодій для конкретного користувача."""
    print(f"\n--- Завдання 1: Історія взаємодій для UserID: {user_id} ---\n")
    try:
        user_history = collection.find_one({'_id': user_id})
        pretty_print_json(user_history)
        return user_history
    except Exception as e:
        print(f"Помилка: {e}")
        return None

def task_2_get_last_5_sessions(collection, user_id=713): 
    """2. Отримати останні 5 рекламних сесій користувача."""
    print(f"\n--- Завдання 2: Останні 5 сесій для UserID: {user_id} ---\n")
    pipeline = [
        {'$match': {'_id': user_id}},
        {'$unwind': '$sessions'},
        {'$sort': {'sessions.start_time': -1}},
        {'$limit': 5},
        {'$group': {
            '_id': '$_id',
            'last_5_sessions': {'$push': '$sessions'}
        }},
        {'$project': { 
            '_id': 0,
            'user_id': '$_id',
            'last_5_sessions': '$last_5_sessions'
        }}
    ]
    try:
        result = list(collection.aggregate(pipeline))
        pretty_print_json(result)
        return result
    except Exception as e:
        print(f"Помилка: {e}")
        return None

def task_3_time_windowed_performance(collection, advertiser_id=41): 
    """3. Кількість кліків за годину для кампаній вказаного рекламодавця за останні 24 години."""
    print(f"\n--- Завдання 3: Кліки за годину для AdvertiserID: {advertiser_id} (останні 24 години) ---\n")

    last_two_years = datetime.now() - timedelta(days=365*2) 
    pipeline = [
        {'$unwind': '$sessions'},
        {'$unwind': '$sessions.impressions'},
        {'$match': {
            'sessions.impressions.clicked': True,
            'sessions.impressions.campaign_details.advertiser_id': advertiser_id,
            'sessions.impressions.click_details.click_timestamp': {'$gte': last_two_years} 
        }},
        {'$group': {
            '_id': {
                'campaign_id': '$sessions.impressions.campaign_details.campaign_id',
                'hour': {'$dateToString': {'format': '%Y-%m-%d %H:00', 'date': '$sessions.impressions.click_details.click_timestamp', 'timezone': 'Europe/Kiev'}}
            },
            'click_count': {'$sum': 1}
        }},
        {'$sort': {'_id.hour': -1, '_id.campaign_id': 1}},
        {'$project': {
            'campaign_id': '$_id.campaign_id',
            'hour': '$_id.hour',
            'clicks': '$click_count',
            '_id': 0
        }}
    ]
    try:
        result = list(collection.aggregate(pipeline))
        pretty_print_json(result)
        return result
    except Exception as e:
        print(f"Помилка: {e}")
        return None

def task_4_detect_ad_fatigue(collection, min_impressions=5):
    """4. Знайти користувачів, які бачили одну й ту саму рекламу 5+ разів, але не клікнули."""
    print(f"\n--- Завдання 4: Виявлення втоми від реклами ({min_impressions}+ показів без кліків) ---\n")
    pipeline = [
        {'$unwind': '$sessions'},
        {'$unwind': '$sessions.impressions'},
        {'$group': {
            '_id': {
                'user_id': '$_id',
                'campaign_id': '$sessions.impressions.campaign_details.campaign_id'
            },
            'impression_count': {'$sum': 1},
            'click_count': {'$sum': {'$cond': ['$sessions.impressions.clicked', 1, 0]}}
        }},
        {'$match': {
            'impression_count': {'$gte': min_impressions},
            'click_count': 0
        }},
        {'$project': {
            'user_id': '$_id.user_id',
            'campaign_id': '$_id.campaign_id',
            'impressions': '$impression_count',
            '_id': 0
        }}
    ]
    try:
        result = list(collection.aggregate(pipeline))
        pretty_print_json(result)
        return result
    except Exception as e:
        print(f"Помилка: {e}")
        return None

def task_5_real_time_targeting_lookup(collection, user_id=713):
    """5. Отримати топ-3 найбільш залучених категорій реклами для користувача."""
    print(f"\n--- Завдання 5: Топ-3 категорій для UserID: {user_id} ---\n")
    pipeline = [
        {'$match': {'_id': user_id}},
        {'$unwind': '$sessions'},
        {'$unwind': '$sessions.impressions'},
        {'$match': {'sessions.impressions.clicked': True}},
        {'$group': {
            '_id': '$sessions.impressions.campaign_details.category',
            'total_clicks': {'$sum': 1}
        }},
        {'$sort': {'total_clicks': -1}},
        {'$limit': 3},
        {'$project': {
            'category': '$_id',
            'clicks': '$total_clicks',
            '_id': 0
        }}
    ]
    try:
        result = list(collection.aggregate(pipeline))
        pretty_print_json(result)
        return result
    except Exception as e:
        print(f"Помилка: {e}")
        return None

def main():
    """Головна функція для запуску запитів."""
    collection = connect_mongo()
    if collection is not None:
        # Виконання всіх завдань
        task_1_get_user_interaction_history(collection, user_id=713)
        task_2_get_last_5_sessions(collection, user_id=713)
        task_3_time_windowed_performance(collection, advertiser_id=41) 
        task_4_detect_ad_fatigue(collection) 
        task_5_real_time_targeting_lookup(collection, user_id=713)

if __name__ == '__main__':
    main()
