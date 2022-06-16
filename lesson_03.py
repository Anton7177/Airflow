import numpy as np
import pandas as pd
import telegram
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'a-gureev-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 8),
    'schedule_interval': '00 12 * * *'
} 

CHAT_ID = -639919847
BOT_TOKEN = '5127663592:AAGxkRPIo1JMLk4K1vduSFR7018H-LGvBN0'

#  Later for secrecy  
# BOT_TOKEN = Variable.get('telegram_secret') 


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Awesome dude! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

#  Later for secrecy   
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass
    

@dag(default_args=default_args, catchup=False)
def a_gureev_18_lesson3():
    
    @task(retries=3)
    def get_data():
        
        video_game_sales = pd.read_csv('vgsales.csv')  # Read data
        video_game_sales = video_game_sales.drop_duplicates()
        video_game_sales['Year'] = video_game_sales['Year'].fillna(0)
        
        login = 'a-gureev-18'
        current_year = 1994 + hash(f'{login}') % 23
        df_games_sales = video_game_sales[video_game_sales['Year'] == current_year]  # Filter by year
        df_games_sales['Publisher'] = df_games_sales['Publisher'].replace(np.nan, 0)
        
        return df_games_sales
    
    
    @task(retries=4, retry_delay=timedelta(10))
    # Какая игра была самой продаваемой в этом году во всем мире?
    def top_game(df_games_sales):
        
        #  Top game on all platforms combined       
        top_game = df_games_sales[df_games_sales.Global_Sales == df_games_sales.Global_Sales.max()].iloc[0].Name

        return top_game
    
    @task()
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def top_eu_genres(df_games_sales):
        
        top_europe_genres = df_games_sales.groupby('Genre', as_index=False) \
                                  .agg({'EU_Sales': 'sum'})
        
        top_eu_genres = top_europe_genres[top_europe_genres['EU_Sales'] == top_europe_genres['EU_Sales'].max()].iloc[0].Genre
        
        return top_eu_genres
    
    
    @task()
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    def top_NA_platform(df_games_sales):
        
        top_NA_platforms_over_1m = df_games_sales.query('NA_Sales > 1') \
                                .groupby('Platform', as_index=False) \
                                .agg({'Genre': 'count'}) \
                                .rename(columns={'Genre': 'Qty_Games'})
    
        top_NA_platform = top_NA_platforms_over_1m[top_NA_platforms_over_1m['Qty_Games'] == top_NA_platforms_over_1m.Qty_Games.max()].iloc[0].Platform

        return top_NA_platform
    
    
    @task()
    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    def top_publisher_Jap_sales(df_games_sales):
        
        avg_Jap_sales = df_games_sales.groupby('Publisher', as_index=False) \
                                .agg({'JP_Sales': 'mean'}) \
                                .rename(columns={'JP_Sales': 'Avg_Sales'})

        top_publisher_Jap_sales = avg_Jap_sales[avg_Jap_sales['Avg_Sales'] == avg_Jap_sales['Avg_Sales'].max()].iloc[0].Publisher

        return top_publisher_Jap_sales
    
    
    
    @task()
    # Сколько игр продались лучше в Европе, чем в Японии?
    def europe_japan_sales_titles(df_games_sales):

        # Count titles with sales on all platforms
        europe_japan_sales_titles = df_games_sales.groupby('Name', as_index=False) \
            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})

        europe_japan_sales_titles['More_Sales'] = np.where(europe_japan_sales_titles['EU_Sales'] > europe_japan_sales_titles['JP_Sales'], 'Europe', 'Japan')
        europe_sales_more_than_jap = europe_japan_sales_titles.query('More_Sales == "Europe"')

        europe_japan_sales_titles = europe_sales_more_than_jap.shape[0]

        return europe_japan_sales_titles
    
    
    @task(on_success_callback=send_message)
    def print_data(top_game, top_eu_genres, top_NA_platform, top_publisher_Jap_sales, europe_japan_sales_titles):
        
        
        login = 'a-gureev-18'
        current_year = 1994 + hash(f'{login}') % 23
        
        
        print(f'''Data for                                              {current_year} 
                  The most popular video game:                                   {top_game} 
                  The most popular genre in Europe:                              {top_eu_genres}
                  Top platform in North America:                                 {top_NA_platform}
                  Top publisher in Japan:                                        {top_publisher_Jap_sales}
                  Number of games that are more popular in Europe than in Japan: {europe_japan_sales_titles}''')
        
    df_games_sales = get_data()

    top_game = top_game(df_games_sales)
    top_eu_genres = top_eu_genres(df_games_sales)
    top_NA_platform = top_NA_platform(df_games_sales)
    top_publisher_Jap_sales = top_publisher_Jap_sales(df_games_sales)
    europe_japan_sales_titles = europe_japan_sales_titles(df_games_sales)
    

    print_data(top_game, top_eu_genres, top_NA_platform, top_publisher_Jap_sales, europe_japan_sales_titles)

a_gureev_18_lesson3 = a_gureev_18_lesson3() 