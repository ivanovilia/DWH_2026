from pathlib import Path
from html import escape

ROOT = Path(__file__).resolve().parent
parts = ['''<svg xmlns="http://www.w3.org/2000/svg" width="1800" height="1460" viewBox="0 0 1800 1460" role="img" aria-labelledby="title desc">
<title id="title">Итоговый проект: DWH онлайн-кинотеатра</title>
<desc id="desc">Генератор отправляет просмотры, поиск и изменения профилей в Kafka. Независимые потребители сохраняют полный архив в S3 и строят оперативные показатели в ClickHouse. Airflow по расписанию выполняет SCD2, исторический JOIN, расчёт и проверки витрин, публикует batch-результат. Общий отчёт использует непересекающиеся интервалы двух слоёв.</desc>
<defs>
  <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0 L9,5 L0,10 Z" fill="#50657f"/></marker>
</defs>
<style>
  text {font-family: Arial, Helvetica, sans-serif; fill:#172b45;}
  .edge {fill:none;stroke:#50657f;stroke-width:2.5;stroke-linejoin:round;}
</style>
<rect width="1800" height="1460" fill="#f7f9fc"/>
''']

def rect(x, y, w, h, fill='#ffffff', stroke='#cbd5e1', radius=16):
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')

def text(x, y, value, size=18, color='#172b45', weight=400):
    parts.append(f'<text x="{x}" y="{y}" font-size="{size}" style="fill:{color}" font-weight="{weight}">{escape(value)}</text>')

def lines(x, y, values, size=18, step=28, color='#405572'):
    for i, value in enumerate(values):
        text(x, y + step*i, value, size, color)

def edge(points):
    path = 'M' + ' L'.join(f'{x},{y}' for x, y in points)
    parts.append(f'<path class="edge" d="{path}"/>')
    x, y = points[-1]
    px, py = points[-2]
    length = ((x-px)**2 + (y-py)**2)**0.5
    dx, dy = (x-px)/length, (y-py)/length
    triangle = [(x, y), (x-12*dx+5*dy, y-12*dy-5*dx),
                (x-12*dx-5*dy, y-12*dy+5*dx)]
    coords = ' '.join(f'{a},{b}' for a, b in triangle)
    parts.append(f'<polygon points="{coords}" fill="#50657f"/>')

text(60, 64, 'Итоговый проект · DWH онлайн-кинотеатра', 38, weight=700)
text(60, 101, 'Архитектура проекта · оперативная аналитика и исторические отчёты', 20, '#52657e')

# Group backgrounds.
rect(60, 350, 500, 730, '#ecf5ff', '#b6d3f2', 22)
text(85, 389, 'ПОСТОЯННО · ЗАГРУЗКА В CLICKHOUSE', 19, '#225d98', 700)
rect(640, 555, 1100, 330, '#f1edff', '#d0c3ed', 22)
text(663, 593, 'ПО РАСПИСАНИЮ · AIRFLOW DAG', 20, '#624596', 700)

# Data routes. Draw before cards so paths cannot obscure labels.
edge([(480, 205), (590, 205)])
edge([(860, 280), (860, 315), (35, 315), (35, 480), (85, 480)])
text(350, 306, 'просмотры и поиск', 16, '#52657e')
edge([(1000, 280), (1000, 350)])
text(1020, 321, 'оба потока', 16, '#52657e')
edge([(1505, 280), (1505, 350)])
edge([(310, 540), (310, 580)])
edge([(310, 690), (310, 840)])
text(332, 757, 'запись результатов', 16, '#52657e')
edge([(640, 480), (610, 480), (610, 667), (660, 667)])
edge([(1000, 667), (1020, 667)])
edge([(1360, 667), (1380, 667)])
edge([(1550, 715), (1550, 750)])
edge([(1380, 797), (1360, 797)])
edge([(1020, 797), (1000, 797)])
edge([(830, 845), (830, 930)])
edge([(310, 1020), (310, 1135), (650, 1135), (650, 1170)])
edge([(1150, 1080), (1150, 1170)])
edge([(1575, 1080), (1575, 1170)])
text(1168, 1135, 'общие метрики', 16, '#52657e')

# Supplied inputs and infrastructure.
rect(60, 145, 420, 135, '#ffffff')
text(82, 176, 'Генератор · предоставляем', 22, weight=700)
lines(82, 210, ['Просмотры и поисковые запросы', 'Изменения профилей пользователей'], 18, 27)

rect(590, 145, 540, 135, '#ffffff')
text(614, 179, 'Kafka · разворачивают студенты', 23, weight=700)
lines(614, 216, ['Поток действий + поток изменений профилей', 'Повторы и опоздания задаются генератором'], 18, 27)

rect(1270, 145, 470, 135, '#ffffff')
text(1293, 179, 'Каталог · предоставляем данные', 21, weight=700)
lines(1293, 216, ['Контент, категории, длительность', 'Статический справочник'], 18, 27)

# Student ClickHouse ingestion objects.
rect(85, 430, 450, 110, '#ffffff', '#a6c6e9')
text(107, 468, 'ClickHouse · Kafka Engine', 23, '#225d98', 700)
lines(107, 500, ['Чтение топика напрямую из ClickHouse', 'Студенты настраивают таблицу и consumer group'], 17, 25)

rect(85, 580, 450, 110, '#ffffff', '#a6c6e9')
text(107, 618, 'ClickHouse · materialized view', 23, '#225d98', 700)
lines(107, 650, ['Перенос сообщений в целевые таблицы', 'Студенты реализуют преобразования'], 17, 25)

rect(85, 840, 450, 180, '#ffffff', '#a6c6e9')
text(107, 878, 'ClickHouse · speed-таблицы', 22, '#225d98', 700)
lines(107, 915, ['Просмотры и секунды по минуте и контенту', 'Студенты проектируют DDL и агрегации', 'и обеспечивают корректность при повторах'], 17, 28)

# Complete archive with a separate Kafka consumer group.
rect(640, 350, 1100, 160, '#ecf8f2', '#afd6c1')
text(665, 389, 'S3 · полный архив событий и каталог', 24, '#286646', 700)
lines(665, 426, ['Студенты архивируют оба потока отдельной consumer group, независимой от ClickHouse.',
                 'raw_events · raw_profile_changes · ref_catalogue',
                 'Поздние события сохраняются; пакетный расчёт пересчитывает затронутые периоды.'], 18, 29)

def task(x, y, number, title, details):
    rect(x, y, 340, 95, '#ffffff', '#d0c3ed', 12)
    text(x+15, y+29, f'{number}. {title}', 19, '#56387f', 700)
    lines(x+15, y+56, details, 16, 23)

task(660, 620, 1, 'Зафиксировать вход', ['Интервал + версия исходного среза', 'Стабильный вход при повторе'])
task(1020, 620, 2, 'Подготовить события', ['Проверка схемы и ошибок', 'Дедупликация событий'])
task(1380, 620, 3, 'Построить SCD2', ['История профилей в S3', 'Версии страны и тарифа'])
task(1380, 750, 4, 'JOIN и витрины', ['Факты + исторический профиль + каталог', 'Общие и исторические показатели'])
task(1020, 750, 5, 'Проверить расчёты', ['Spark RDD ↔ DataFrame', 'Срез из S3 + качество витрин'])
task(660, 750, 6, 'Опубликовать результат', ['Проверенный batch → ClickHouse', 'После успеха — обновить границу T'])

rect(640, 930, 1100, 150, '#ffffff', '#c4b2e0')
text(665, 969, 'ClickHouse · опубликованные batch-таблицы', 24, '#56387f', 700)
lines(665, 1007, ['1. Те же поминутные показатели, что в speed-слое, пересчитанные по архиву.',
                  '2. Исторические витрины: просмотры по стране и тарифу на момент события.'], 18, 30)

# Serving semantics and separate enriched reports.
rect(440, 1170, 900, 165, '#172b45', '#172b45', 20)
text(466, 1209, 'Общий отчёт · студенты реализуют согласование слоёв', 23, '#ffffff', 700)
text(466, 1250, 'batch: время события < T     +     speed: время события ≥ T', 23, '#ffffff', 700)
lines(466, 1285, ['Детализация: минута + контент; периоды не пересекаются.',
                 'T — граница закрытого интервала, продвигается после успешной публикации.'], 17, 27, '#d9e5f4')

rect(1410, 1170, 330, 165, '#ffffff', '#c4b2e0')
text(1430, 1209, 'Исторические отчёты', 21, '#56387f', 700)
lines(1430, 1250, ['Страна и тариф на дату просмотра', 'Топ-5 контента за день', 'Частоты слов в поиске'], 17, 28)

text(60, 1383, 'ДАЁМ: генератор и данные. СТУДЕНТЫ: инфраструктура, загрузка, DAG, SCD2, JOIN, DDL, метаполя и проверки.', 19, weight=700)
text(60, 1417, 'ClickHouse на схеме — один аналитический слой с разными таблицами. Шесть блоков DAG — этапы; внутри может быть несколько задач.', 17, '#52657e')
parts.append('</svg>')
(ROOT / 'architecture.svg').write_text('\n'.join(parts), encoding='utf-8')
print(ROOT / 'architecture.svg')
