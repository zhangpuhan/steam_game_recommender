import json, os, re
from datetime import datetime
import sqlalchemy
import pandas as pd


# set file path
path_app_info = './data/app_detail.txt' # your crawled app info txt file
path_steam_app_info = './data/steam_app_info.csv' # where you want to save the output csv file

#####################################
### extract selected app features ###
#####################################


if os.path.exists(path_steam_app_info):
    print(path_steam_app_info, 'already exists')
    df_steam_app = pd.read_csv(path_steam_app_info)
else:
    with open(path_app_info, 'r') as f:
        dic_steam_app = {'initial_price':{},'name':{},'score':{},'windows':{},'mac':{},'linux':{},'type':{},'release_date':{},'recommendation':{},'header_image':{}}
        lst_raw_string = f.readlines()
        for raw_string in lst_raw_string:
            app_data = list(json.loads(raw_string).values())[0]#.get('data')
            if app_data != {}:									# steam api doesn't have information for the requested app id. We can skip that.
                steam_id = app_data.get('steam_appid')
                initial_price = app_data.get('price_overview',{}).get('initial')
                if app_data.get('is_free') == True:
                    initial_price = 0							# set price to 0 if the game is free
                app_name = app_data.get('name')
                critic_score = app_data.get('metacritic', {}).get('score')
                app_type = app_data.get('type')
                for (platform, is_supported) in app_data.get('platforms',{}).items():
                    if is_supported == True:
                        dic_steam_app[platform].update({steam_id:1})
                if app_data.get('release_date',{}).get('coming_soon') == False:
                    release_date = app_data.get('release_date',{}).get('date')
                    if not release_date == '':
                        if re.search(',', release_date) == None:
                            release_date = datetime.strptime(release_date, '%b %Y')
                        else:
                            try:
                                release_date = datetime.strptime(release_date, '%b %d, %Y')
                            except:
                                release_date = datetime.strptime(release_date, '%d %b, %Y')

                recommendation = app_data.get('recommendations',{}).get('total')
                header_image = app_data.get('header_image')
                dic_steam_app['initial_price'].update({steam_id:initial_price})
                dic_steam_app['name'].update({steam_id:app_name})
                dic_steam_app['score'].update({steam_id:critic_score})
                dic_steam_app['type'].update({steam_id:app_type})
                dic_steam_app['release_date'].update({steam_id:release_date})
                dic_steam_app['recommendation'].update({steam_id:recommendation})
                dic_steam_app['header_image'].update({steam_id:header_image})


    df_steam_app = pd.DataFrame(dic_steam_app)
    df_steam_app.initial_price = df_steam_app.initial_price.map(lambda x: x/100.0)
    df_steam_app.index.name = 'steam_appid'
    df_steam_app['windows'] = df_steam_app.windows.fillna(0)
    df_steam_app['mac'] = df_steam_app.mac.fillna(0)
    df_steam_app['linux'] = df_steam_app.linux.fillna(0)
    df_steam_app = df_steam_app[['name', 'type', 'initial_price', 'release_date', 'score', 'recommendation', 'windows', 'mac', 'linux', 'header_image']]
    df_steam_app.reset_index(inplace=True)
    df_steam_app.to_csv(path_steam_app_info,encoding='utf8',index=False)




#####################
### save to MySQL ###
#####################





# replace MySQL and csv path
engine = sqlalchemy.create_engine('mysql+pymysql://root:<password>@localhost:3306/game_recommendation?charset=utf8mb4&local_infile=1')

engine.execute(''' 
    CREATE TABLE IF NOT EXISTS `tbl_steam_app`(
        `steam_appid` INT,
        `name` VARCHAR(500) CHARACTER SET utf8mb4,
        `type` VARCHAR(15),
        `initial_price` FLOAT,
        `release_date` VARCHAR(20),
        `score` INT,
        `recommendation` INT,
        `windows` BOOLEAN,
        `mac` BOOLEAN,
        `linux` BOOLEAN,
        `header_image` VARCHAR(100)
    );
    ''')


engine.execute('''
    LOAD DATA LOCAL INFILE '%s' INTO TABLE `tbl_steam_app` 
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@steam_appid, @name, @type, @initial_price, @release_date, @score, @recommendation, @windows, @mac, @linux, @header_image)
    SET
    steam_appid = nullif(@steam_appid, ''),
    name = nullif(@name, ''),
    type = nullif(@type, ''),
    initial_price = nullif(@initial_price,''),
    release_date = nullif(@release_date,''),
    score = nullif(@score,''), 
    recommendation = nullif(@recommendation, ''),
    windows = nullif(@windows, ''),
    mac = nullif(@mac, ''),
    linux = nullif(@linux, ''),
    header_image = nullif(@header_image, '');
    ''' % (path_steam_app_info))



