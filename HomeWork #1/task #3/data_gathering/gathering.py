"""
ЗАДАНИЕ

Выбрать источник данных и собрать данные по некоторой предметной области.

Цель задания - отработать навык написания программ на Python.
В процессе выполнения задания затронем области:
- организация кода в виде проекта, импортирование модулей внутри проекта
- unit тестирование
- работа с файлами
- работа с протоколом http
- работа с pandas
- логирование

Требования к выполнению задания:

- собрать не менее 1000 объектов

- в каждом объекте должно быть не менее 5 атрибутов
(иначе просто будет не с чем работать.
исключение - вы абсолютно уверены что 4 атрибута в ваших данных
невероятно интересны)

- сохранить объекты в виде csv файла

- считать статистику по собранным объектам


Этапы:

1. Выбрать источник данных.

Это может быть любой сайт или любое API

Примеры:
- Пользователи vk.com (API)
- Посты любой популярной группы vk.com (API)
- Фильмы с Кинопоиска
(см. ссылку на статью ниже)
- Отзывы с Кинопоиска
- Статьи Википедии
(довольно сложная задача,
можно скачать дамп википедии и распарсить его,
можно найти упрощенные дампы)
- Статьи на habrahabr.ru
- Объекты на внутриигровом рынке на каком-нибудь сервере WOW (API)
(желательно англоязычном, иначе будет сложно разобраться)
- Матчи в DOTA (API)
- Сайт с кулинарными рецептами
- Ebay (API)
- Amazon (API)
...

Не ограничивайте свою фантазию. Это могут быть любые данные,
связанные с вашим хобби, работой, данные любой тематики.
Задание специально ставится в открытой форме.
У такого подхода две цели -
развить способность смотреть на задачу широко,
пополнить ваше портфолио (вы вполне можете в какой-то момент
развить этот проект в стартап, почему бы и нет,
а так же написать статью на хабр(!) или в личный блог.
Чем больше у вас таких активностей, тем ценнее ваша кандидатура на рынке)

2. Собрать данные из источника и сохранить себе в любом виде,
который потом сможете преобразовать

Можно сохранять страницы сайта в виде отдельных файлов.
Можно сразу доставать нужную информацию.
Главное - постараться не обращаться по http за одними и теми же данными много раз.
Суть в том, чтобы скачать данные себе, чтобы потом их можно было как угодно обработать.
В случае, если обработать захочется иначе - данные не надо собирать заново.
Нужно соблюдать "этикет", не пытаться заддосить сайт собирая данные в несколько потоков,
иногда может понадобиться дополнительная авторизация.

В случае с ограничениями api можно использовать time.sleep(seconds),
чтобы сделать задержку между запросами

3. Преобразовать данные из собранного вида в табличный вид.

Нужно достать из сырых данных ту самую информацию, которую считаете ценной
и сохранить в табличном формате - csv отлично для этого подходит

4. Посчитать статистики в данных
Требование - использовать pandas (мы ведь еще отрабатываем навык использования инструментария)
То, что считаете важным и хотели бы о данных узнать.

Критерий сдачи задания - собраны данные по не менее чем 1000 объектам (больше - лучше),
при запуске кода командой "python3 -m gathering stats" из собранных данных
считается и печатается в консоль некоторая статистика

Код можно менять любым удобным образом
Можно использовать и Python 2.7, и 3

Зачем нужны __init__.py файлы
https://stackoverflow.com/questions/448271/what-is-init-py-for

Про документирование в Python проекте
https://www.python.org/dev/peps/pep-0257/

Про оформление Python кода
https://www.python.org/dev/peps/pep-0008/


Примеры сбора данных:
https://habrahabr.ru/post/280238/

Для запуска тестов в корне проекта:
python3 -m unittest discover

Для запуска проекта из корня проекта:
python3 -m gathering gather
или
python3 -m gathering transform
или
python3 -m gathering stats


Для проверки стиля кода всех файлов проекта из корня проекта
pep8 .

"""

import logging
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import os
import sys

logger = logging.getLogger(__name__)

def gather_process():
    print("Скачиваем данные с русскоязычного сайта АПЛ (www.fapl.ru)." 
                +"Данные итоговых таблиц сезонов премьер лиги сохраним в папке Data")
    
    for i in range(0,12):
        url = r'http://fapl.ru/table/?season='+str(i+1)
        r = requests.get(url)
        r.encoding = 'cp1251'
        r_text = r.text
        # Создаем папку для хранения данных Data в корневом каталоге проекта
        if not os.path.exists("Data"):
                os.makedirs("Data")
                
        text_file = open("Data/Output" + str(i)+ ".txt", "w")
        text_file.write(r_text)
        text_file.close()

def convert_data_to_table_format():
    print("Парсим полученные html страницы и сохраняем данные в датафрейм fapl_table,"
                +"который сохраним корневом каталоге под именем fapl_table.csv")
    place = []
    team_name = []
    cnt_match = []
    win = []
    draw = []
    lose = []
    goal = []
    cons = []
    season = []
    
    for i in range(0,12):
        text_file = open("Data/Output" + str(i)+ ".txt", "r")
        r_text = text_file.read() 
        
        soup = BeautifulSoup(r_text, 'lxml')
        table = soup.find_all('table')[0]
        for row in table.find_all('tr'):
            c = 0
            for elem in row.find_all('td'):
                if c == 0:
                    place.append(elem.text) 
                    season.append(i+1)
                elif c == 1:
                    string_name = elem.text
                    team_name.append(string_name.strip())
                elif c == 2:
                    cnt_match.append(elem.text)
                elif c == 3:
                    win.append(elem.text)
                elif c == 4:
                    draw.append(elem.text)
                elif c == 5:
                    lose.append(elem.text)          
                elif c == 6:
                    goal.append(elem.text)
                elif c == 7:
                    cons.append(elem.text)
                c = c + 1
              
    diction = {'place' : place, 'team_name': team_name, 'cnt_match': cnt_match, 'win':win, 'draw':draw, \
       'lose':lose,'goal':goal, 'cons':cons,'season':season}  
    fapl_table = pd.DataFrame(diction)
    fapl_table.to_csv("fapl_table.csv", index=False)
    pass


def stats_of_data():
    print('Статистика АПЛ за все время существования с сезона - 1993 по 2017-2018 год')
    fapl_table = pd.read_csv("fapl_table.csv")
    print("Кол-во уникальных команд участвующих за всю историю АПЛ - {}".format(len(fapl_table['team_name'].unique())))
    agr_table = fapl_table.groupby(['team_name'], as_index=False).agg({'cnt_match': np.sum, 'win': np.sum, 'draw': np.sum, 'lose': np.sum, \
                     'goal': np.sum, 'cons': np.sum })
    df_table = pd.DataFrame(agr_table)
    print("****************************************************************")
    print("Топ-5 команд, которые провели больше всех матчей в АПЛ с сезона 2006-2007 года \n")
    print(df_table[['team_name','cnt_match']].nlargest(5, 'cnt_match'))
    print("****************************************************************")
    print("Топ-5 команд, имеющих больше всего побед в АПЛ с сезона 2006-2007 года \n")
    print(df_table[['team_name','win']].nlargest(5, 'win'))
    print("****************************************************************")
    print("Топ-5 команд, чаще всех играющих вничью в АПЛ с сезона 2006-2007 года \n")
    print(df_table[['team_name','draw']].nlargest(5, 'draw'))
    print("****************************************************************")
    print("Топ-5 команд, имеющие больше других поражений в АПЛ с сезона 2006-2007 года \n")
    print(df_table[['team_name','lose']].nlargest(5, 'lose'))
    print("****************************************************************")
    df_table['goal_per_match'] = df_table['goal'] / df_table['cnt_match']
    df_table['cons_per_match'] = df_table['cons'] / df_table['cnt_match']
    print("Топ-5 команд, больше других забивающие в среднем за матч в АПЛ с сезона 2006-2007 года \n")
    print(df_table[['team_name','goal_per_match']].nlargest(5, 'goal_per_match'))
    print("****************************************************************")
    print("Топ-5 команд, меньше других пропускающие в среднем за матч в АПЛ с сезона 2006-2007 года \n")
    print(df_table[['team_name','cons_per_match']].nsmallest(5, 'cons_per_match'))
    print("****************************************************************")
    print("{} команды забили в АПЛ больше 100 голов с сезона 2006-2007 года ".format(len(df_table[df_table['goal']>100])))
    print("{} команд выиграли в АПЛ больше 100 матчей с сезона 2006-2007 года ".format(len(df_table[df_table['win']>100])))
    print("****************************************************************")
    
if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    print("--------- Work Started ---------")

    if sys.argv[1] == 'gather':
        gather_process()

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        stats_of_data()

    print("--------- Work Ended ---------")
