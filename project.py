import pandas as pd
from pandasql import sqldf

# загрузка данных
df = pd.read_csv("Coffee_Sales.csv", encoding="utf-8")

# функция для запросов
pysqldf = lambda q: sqldf(q, globals())

# сначала посмотрим что вообще есть
print("Первые строки:")
print(df.head())
print("\nИнфo o данных:")
print(df.info())

# базовая статистика

# общие цифры по продажам
q1 = """
SELECT 
    COUNT(*) as всего_транзакций,
    ROUND(SUM(money), 2) as общая_выручка,
    ROUND(AVG(money), 2) as средний_чек
FROM df
"""
print("\n--- Общая статистика ---")
print(pysqldf(q1))

# какие напитки популярные

# топ напитков
query = """
SELECT 
    coffee_name, 
    COUNT(*) AS продано,
    ROUND(AVG(money), 2) AS средняя_цена,
    ROUND(SUM(money), 2) AS выручка
FROM df
GROUP BY coffee_name
ORDER BY продано DESC
LIMIT 10
"""
print("\n--- Топ-10 напитков ---")
result = pysqldf(query)
print(result)

# способы оплаты

# смотрим наличку vs карта
query2 = """
SELECT 
    cash_type,
    COUNT(*) as количество,
    ROUND(AVG(money), 2) as средний_чек,
    ROUND(SUM(money), 2) as выручка
FROM df
GROUP BY cash_type
"""
print("\n--- Способы оплаты ---")
print(pysqldf(query2))

# анализ по времени

# добавим часы для анализа
df['hour'] = pd.to_datetime(df['datetime']).dt.hour

# по часам
query3 = """
SELECT 
    hour as час,
    COUNT(*) as заказов
FROM df
GROUP BY hour
ORDER BY hour
"""
print("\n--- Заказы по часам ---")
print(pysqldf(query3))

# время суток (утро день вечер)
query4 = """
SELECT 
    CASE 
        WHEN hour >= 6 AND hour < 12 THEN 'утро'
        WHEN hour >= 12 AND hour < 18 THEN 'день'
        WHEN hour >= 18 AND hour < 23 THEN 'вечер'
        ELSE 'ночь'
    END as время_суток,
    COUNT(*) as заказов,
    ROUND(AVG(money), 2) as средний_чек
FROM df
GROUP BY время_суток
"""
print("\n--- По времени суток ---")
print(pysqldf(query4))

# дни недели

# добавим дни недели
df['weekday'] = pd.to_datetime(df['date']).dt.day_name()
df['weekday_num'] = pd.to_datetime(df['date']).dt.dayofweek

# анализ по дням
query5 = """
SELECT 
    weekday as день,
    COUNT(*) as заказов,
    ROUND(SUM(money), 2) as выручка
FROM df
GROUP BY weekday, weekday_num
ORDER BY weekday_num
"""
print("\n--- Продажи по дням недели ---")
print(pysqldf(query5))

# клиенты

# кто чаще всего покупает
query6 = """
SELECT 
    card as карта,
    COUNT(*) as визитов,
    ROUND(SUM(money), 2) as потратил,
    ROUND(AVG(money), 2) as средний_чек
FROM df
WHERE card IS NOT NULL
GROUP BY card
ORDER BY визитов DESC
LIMIT 15
"""
print("\n--- Самые частые клиенты ---")
print(pysqldf(query6))

# лучшие дни

# топ дней по деньгам
query7 = """
SELECT 
    date as дата,
    COUNT(*) as заказов,
    ROUND(SUM(money), 2) as выручка
FROM df
GROUP BY date
ORDER BY выручка DESC
LIMIT 10
"""
print("\n--- Топ-10 дней по выручке ---")
print(pysqldf(query7))

# что пьют когда

# популярные напитки по времени суток
query8 = """
SELECT 
    CASE 
        WHEN hour >= 6 AND hour < 12 THEN 'утро'
        WHEN hour >= 12 AND hour < 18 THEN 'день'  
        ELSE 'вечер'
    END as время,
    coffee_name as напиток,
    COUNT(*) as продано
FROM df
WHERE hour >= 6 AND hour < 23
GROUP BY время, coffee_name
HAVING COUNT(*) > 15
ORDER BY время, продано DESC
"""
print("\n--- Что пьют в разное время ---")
print(pysqldf(query8))

# цены

# группы по цене
query9 = """
SELECT 
    CASE 
        WHEN money < 25 THEN 'дешевые'
        WHEN money >= 25 AND money < 35 THEN 'средние'
        ELSE 'дорогие'
    END as категория,
    COUNT(*) as количество
FROM df
GROUP BY категория
"""
print("\n--- Категории по цене ---")
print(pysqldf(query9))

# интересные находки

# средний чек по дням и способу оплаты
query10 = """
SELECT 
    weekday as день,
    cash_type as оплата,
    COUNT(*) as транзакций,
    ROUND(AVG(money), 2) as средний_чек
FROM df
GROUP BY weekday, cash_type, weekday_num
ORDER BY weekday_num, cash_type
"""
print("\n--- Чек по дням и оплате ---")
print(pysqldf(query10))

# какой час лучший для каждого напитка
query11 = """
SELECT 
    coffee_name,
    hour,
    COUNT(*) as продаж
FROM df
GROUP BY coffee_name, hour
ORDER BY coffee_name, продаж DESC
"""
temp = pysqldf(query11)
# берем только первый (лучший) час для каждого напитка
best_hours = temp.groupby('coffee_name').first().reset_index()
print("\n--- Лучший час для продажи каждого напитка ---")
print(best_hours)

# выводы

print("\nОСНОВНЫЕ ВЫВОДЫ ")

# самый популярный напиток
top = pysqldf("SELECT coffee_name, COUNT(*) as cnt FROM df GROUP BY coffee_name ORDER BY cnt DESC LIMIT 1")
print(f"Самый продаваемый: {top['coffee_name'].values[0]}")

# пиковый час
peak = pysqldf("SELECT hour, COUNT(*) as cnt FROM df GROUP BY hour ORDER BY cnt DESC LIMIT 1")
print(f"Пиковый час: {peak['hour'].values[0]}:00")

# средняя выручка в день
daily_avg = pysqldf("SELECT ROUND(AVG(daily), 2) as avg FROM (SELECT date, SUM(money) as daily FROM df GROUP BY date)")
print(f"Средняя выручка в день: ${daily_avg['avg'].values[0]}")

# процент карт
cards = pysqldf("SELECT ROUND(SUM(CASE WHEN cash_type='card' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) as pct FROM df")
print(f"Платят картой: {cards['pct'].values[0]}%")
