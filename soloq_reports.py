# coding: utf-8
from config.constants import MONGODB_CONN, API_KEY, LEAGUES_DATA_DICT, EXCEL_EXPORT_PATH, SOLOQ, WORK_DIR
from pymongo import MongoClient
import pandas as pd
from riotwatcher import RiotWatcher
from classes.entities import Player
from classes.enums import AccountTypes, Roles
from collections import defaultdict
import datetime
import subprocess
from converters.data2frames import get_db_generic_dataframe

slds = MongoClient(MONGODB_CONN).slds
reports = MongoClient(MONGODB_CONN).reports
exports = MongoClient(MONGODB_CONN).exports


# ### Update static data
print('Updating static data.')
try:
	command1 = 'python -W ignore {}slds.py -c db -r euw -l soloq -usd'.format(WORK_DIR)
	p1 = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, shell=True)
	print('Static data updated.')
except:
	print('Error updating static data.')

# ### Get picks to practice
cursor = slds.practice_picks.find().sort('from', -1).limit(1)
practice_picks = cursor[0]
print('Top picks: {}'.format(practice_picks['picks']['Werlyb']))
print('Jungle picks: {}'.format(practice_picks['picks']['Selfmade']))
print('Mid picks: {}'.format(practice_picks['picks']['Nemesis']))
print('ADC picks: {}'.format(practice_picks['picks']['Crownshot']))
print('Support picks: {}'.format(practice_picks['picks']['Falco']))
print('Date of the picks: {}'.format(practice_picks['from']))

# ### Get last patch number
versions = slds.static_data.find_one({'_id': 'versions'})
v_nums = versions['versions'][0].split('.')
patch = v_nums[0] + '.' + v_nums[1]
print('Patch {}.'.format(patch))

# ### Get last 15 days
now = datetime.datetime.now()
month_before = now - datetime.timedelta(days=5)
date = datetime.datetime.strptime(str(month_before.year) + '-' + str(month_before.month) + '-' + str(month_before.day), '%Y-%m-%d')

# ### Download new games and export data into XLSX
command2 = 'python -W ignore {dir}slds.py -c db -r euw -l soloq -d -e -ta MAD -bt {date} -o DB -pd'.format(date=datetime.datetime.strftime(date, '%d-%m-%Y'), dir=WORK_DIR)
subprocess.run(command2, stdout=None, stderr=subprocess.PIPE, check=True, shell=True)
df = get_db_generic_dataframe(exports.soloq)
df['date'] = df.gameCreation.apply(lambda x: x.split(' ')[0])
df.date = df.date.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

df['patch'] = df.gameVersion.apply(lambda x: x.split('.')[0] + '.' + x.split('.')[1])
mad_df = df.loc[df.team_abbv == 'MAD']
date_picks = datetime.datetime.strptime(practice_picks['from'], '%Y-%m-%d')

new_patch_df = mad_df.loc[mad_df.patch == patch]
new_picks_df = mad_df.loc[mad_df.date >= date_picks]

envs = [{'id': 'patch', 'df': new_patch_df}, {'id': 'picks', 'df': new_picks_df}]

for env in envs:    
    name = env['id']
    df = env['df']
    champs_practiced = df.groupby(['player_name', 'champ_name']).count()['currentAccountId'].to_frame().reset_index()
    champs_practiced.rename(columns={'currentAccountId': 'times_played'}, inplace=True)

    df2 = champs_practiced
    dict0 = practice_picks['picks']
    practice_champs = pd.DataFrame([(key, v, 1) for key, values in dict0.items() for v in values]).rename(columns={0: 'player_name', 1: 'champ_name', 2: 'should_train'})
    practiced_champs = df2.merge(practice_champs, on=['player_name', 'champ_name'], how='outer').sort_values(['player_name', 'champ_name']).fillna(0)
    practiced_champs[['times_played', 'should_train']] = practiced_champs[['times_played', 'should_train']].astype(int)

    dict1 = {}
    for name2 in champs_practiced.player_name.unique():
        dfx = champs_practiced.loc[(champs_practiced.player_name == name2)]
        dict1[name2] = {row[1][1]: row[1][2] for row in dfx.iterrows()}

    together = df.groupby(['gameId', 'queueId', 'player_name'], as_index=False).count()
    duplicated_ids = together.duplicated('gameId', keep=False)
    played_together = together.ix[duplicated_ids]

    dict2 = defaultdict(int)
    dict2['Selfmade_Werlyb'] = 0
    dict2['Nemesis_Selfmade'] = 0
    dict2['Crownshot_Falco'] = 0

    for gameid in played_together.gameId.unique():
        df = played_together.loc[played_together.gameId == gameid]
        names = list(df.sort_values('player_name').player_name)
        if len(names) == 2:
            team_name = names[0] + '_' + names[1]
        elif len(names) == 3:
            team_name = names[0] + '_' + names[1] + '_' + names[2]
        elif len(names) == 5:
            team_name = names[0] + '_' + names[1] + '_' + names[2] + '_' + names[3] + '_' + names[4]
        dict2[team_name] += 1

    reports.get_collection(name + '_couples').replace_one({'_id': 'couples'}, {'_id': 'couples', 'data': [{'couple': k, 'games': v} for k, v in dict2.items()]}, upsert=True)
    reports.get_collection(name + '_practiced').replace_one({'_id': 'practiced'}, {'_id': 'practiced', 'data': practiced_champs.to_dict(orient='records')}, upsert=True)

tableau_datasources_commands = ['tabcmd refreshextracts --project \"\" --datasource \"\"',
				                'tabcmd refreshextracts --project \"\" --datasource \"\"',
				                'tabcmd refreshextracts --project \"\" --datasource \"\"',
				                'tabcmd refreshextracts --project \"\" --datasource \"\"']

print('Updating Tableau data sources.')
for cmd in tableau_datasources_commands:
	subprocess.run(cmd, stdout=None, stderr=subprocess.PIPE, check=True, shell=True)
print('Tableau data sources updated succesfully.')

print('Exporting dashboard images.')
picks_command = 'tabcmd export "<workbook>/<dashboard>?:refresh=yes" --png -f "picks.png"'
patch_command = 'tabcmd export "<workbook>/<dashboard>?:refresh=yes" --png -f "patch.png"'
subprocess.run(picks_command, stdout=None, stderr=subprocess.PIPE, check=True, shell=True)
subprocess.run(patch_command, stdout=None, stderr=subprocess.PIPE, check=True, shell=True)
print('Images exported.')
