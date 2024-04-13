import os
import json
import psycopg

# Database connection parameters
conn_params = "dbname='project_database' user='postgres' host='localhost' password='1234'"
conn = psycopg.connect(conn_params)
cursor = conn.cursor()

# Directory paths to JSON files
dir_path = os.path.dirname(os.path.realpath(__file__))
dir_path = os.path.join(dir_path,"open-data/data/")
matches_dir = os.path.join(dir_path, 'matches')
events_dir = os.path.join(dir_path, 'events')
lineups_dir = os.path.join(dir_path, 'lineups')

def import_competitions_and_seasons():
    with open(os.path.join(dir_path, 'competitions.json'), 'r') as file:
        data = json.load(file)
        for item in data:

            cursor.execute(
                "INSERT INTO season (season_id, season_name) VALUES (%s, %s) "
                "ON CONFLICT (season_id) DO NOTHING;",
                (
                    item['season_id'],
                    item['season_name']
                )
            )
            cursor.execute(
                "INSERT INTO competition (competition_id, season_id, country_name, season_name, competition_name, "
                "competition_gender, competition_youth, competition_international) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (competition_id,season_id) DO NOTHING;",
                (
                    item['competition_id'],
                    item['season_id'],
                    item['country_name'],
                    item['season_name'],
                    item['competition_name'],
                    item['competition_gender'],
                    item['competition_youth'],
                    item['competition_international']
                )
            )
            

# Function to import matches
def import_matches(competitions_and_seasons):
    match_ids = []
    for competition_id, seasons in competitions_and_seasons.items():
        for season_id in seasons:
            matches_file_path = os.path.join(matches_dir, f'{competition_id}', f'{season_id}.json')
            if os.path.exists(matches_file_path):
                with open(matches_file_path, 'r') as file:
                    matches = json.load(file)
                    for match in matches:
                        match_ids.append(match['match_id'])
                        
                        cursor.execute(
                            "INSERT INTO country (country_id, country_name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (country_id) DO NOTHING;",
                            (
                                match["home_team"]['country']['id'],
                                match["home_team"]['country']['name'],
                            )
                        )
                        cursor.execute(
                            "INSERT INTO country (country_id, country_name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (country_id) DO NOTHING;",
                            (
                                match["away_team"]['country']['id'],
                                match["away_team"]['country']['name'],
                            )
                        )
                        try:
                            for i in range(len(match["away_team"]["managers"])):
                                cursor.execute(
                                    "INSERT INTO country (country_id, country_name) "
                                    "VALUES (%s, %s) "
                                    "ON CONFLICT (country_id) DO NOTHING;",
                                    (
                                        match['away_team']['managers'][i]['country']['id'],
                                        match['away_team']['managers'][i]['country']['name'],
                                    )
                                )
                        except Exception as E:
                            pass

                        try:
                            for i in range(len(match["home_team"]["managers"])):
                                cursor.execute(
                                    "INSERT INTO country (country_id, country_name) "
                                    "VALUES (%s, %s) "
                                    "ON CONFLICT (country_id) DO NOTHING;",
                                    (
                                        match['home_team']['managers'][i]['country']['id'],
                                        match['home_team']['managers'][i]['country']['name'],
                                    )

                                )
                        except Exception as E:
                            pass
                        
                        try:
                            cursor.execute(
                                "INSERT INTO country (country_id, country_name) "
                                "VALUES (%s, %s) "
                                "ON CONFLICT (country_id) DO NOTHING;",
                                (
                                    match['stadium']['country']['id'],
                                    match['stadium']['country']['name'],
                                )
                            )
                        except Exception as E:
                            pass

                        if 'referee' in match:
                            cursor.execute(
                                "INSERT INTO country (country_id, country_name) "
                                "VALUES (%s, %s) "
                                "ON CONFLICT (country_id) DO NOTHING;",
                                (
                                    match['referee']['country']['id'] if 'referee' in match else None,
                                    match['referee']['country']['name'],
                                )
                            )
                        try:
                            cursor.execute(
                                "INSERT INTO stadium (stadium_id, stadium_name, country_id) "
                                "VALUES (%s, %s, %s) "
                                "ON CONFLICT (stadium_id) DO NOTHING;",
                                (
                                    match['stadium']['id'],
                                    match['stadium']['name'],
                                    match['stadium']['country']['id']
                                )
                            )
                        except Exception as E:
                            pass

                        if 'referee' in match:
                            cursor.execute(
                                "INSERT INTO referee (referee_id, referee_name, country_id) "
                                "VALUES (%s, %s, %s) "
                                "ON CONFLICT (referee_id) DO NOTHING;",
                                (
                                    match['referee']['id'],
                                    match['referee']['name'],
                                    match['referee']['country']['id']
                                )
                            )
                        cursor.execute(
                            "INSERT INTO team (team_id, team_name, gender, team_group, country_id) "
                            "VALUES (%s, %s, %s, %s, %s) "
                            "ON CONFLICT (team_id) DO NOTHING;",
                            (
                                match["away_team"]["away_team_id"],
                                match["away_team"]['away_team_name'],
                                match["away_team"]['away_team_gender'],
                                match["away_team"]['away_team_group'],
                                match["away_team"]['country']['id']
                            )
                        )
                        cursor.execute(
                            "INSERT INTO team (team_id, team_name, gender, team_group, country_id) "
                            "VALUES (%s, %s, %s, %s, %s) "
                            "ON CONFLICT (team_id) DO NOTHING;",
                            (
                                match["home_team"]["home_team_id"],
                                match["home_team"]['home_team_name'],
                                match["home_team"]['home_team_gender'],
                                match["home_team"]['home_team_group'],
                                match["home_team"]['country']['id']
                            )
                        )
                        conn.commit()
                        try:
                            for i in range(len(match["home_team"]["managers"])):
                                cursor.execute(
                                    "INSERT INTO manager (manager_id, manager_name, nickname, date_of_birth, country_id, team_id) "
                                    "VALUES (%s, %s, %s, %s, %s, %s) "
                                    "ON CONFLICT (team_id) DO NOTHING;",
                                    (
                                        match["home_team"]["managers"][i]["id"],
                                        match["home_team"]["managers"][i]['name'],
                                        match["home_team"]["managers"][i]['nickname'],
                                        match["home_team"]["managers"][i]['dob'],
                                        match["home_team"]["managers"][i]['country']['id'],
                                        match["home_team"]["home_team_id"]
                                    )
                                )
                        except Exception as E:
                            pass
                        
                        try:
                            for i in range(len(match["away_team"]["managers"])):
                                cursor.execute(
                                    "INSERT INTO manager (manager_id, manager_name, nickname, date_of_birth, country_id, team_id) "
                                    "VALUES (%s, %s, %s, %s, %s, %s) "
                                    "ON CONFLICT (manager_id) DO NOTHING;",
                                    (
                                        match["away_team"]["managers"][i]["id"],
                                        match["away_team"]["managers"][i]['name'],
                                        match["away_team"]["managers"][i]['nickname'],
                                        match["away_team"]["managers"][i]['dob'],
                                        match["away_team"]["managers"][i]['country']['id'],
                                        match["away_team"]["away_team_id"]
                                    )
                                )
                        except Exception as E:
                            conn.rollback()

                        cursor.execute(
                            "INSERT INTO competition_stage (competition_stage_id, competition_stage_name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (competition_stage_id) DO NOTHING;",
                            (
                                match['competition_stage']['id'],
                                match['competition_stage']['name'],
                            )
                        )
                        cursor.execute(
                            "INSERT INTO matches (match_id, season_id, competition_id, match_date, kick_off, "
                            "home_team_id, away_team_id, stadium_id, referee_id, home_score, away_score, match_week, competition_stage_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                             "ON CONFLICT (match_id) DO NOTHING;",
                            (
                                match['match_id'], 
                                match['season']['season_id'], 
                                match['competition']['competition_id'],
                                match["match_date"], 
                                match["kick_off"], 
                                match["home_team"]["home_team_id"], 
                                match["away_team"]["away_team_id"],
                                match['stadium']['id']if 'stadium' in match else None, 
                                match["referee"]['id'] if 'referee' in match else None,  # Check for 'referee' key and its not None
                                match["home_score"], 
                                match["away_score"], 
                                match["match_week"],
                                match['competition_stage']['id']
                            )
                        )
    return match_ids

# Function to import events
def import_events(match_ids):
    for match in match_ids:
        events_file_path = os.path.join(events_dir, f'{match}.json')
        if os.path.exists(events_file_path):
            print(match)
            with open(events_file_path, 'r') as file:
                events = json.load(file)
                for event in events:
                    
                    cursor.execute(
                        "INSERT INTO event_type (event_type_id, event_type_name) "
                        "VALUES (%s, %s) "
                        "ON CONFLICT (event_type_id) DO NOTHING;",
                        (
                            event['type']['id'],
                            event['type']['name']
                        )
                    )
                    cursor.execute(
                        "INSERT INTO play_pattern (play_pattern_id, play_pattern_name) "
                        "VALUES (%s, %s) "
                        "ON CONFLICT (play_pattern_id) DO NOTHING;",
                        (
                            event['play_pattern']['id'],
                            event['play_pattern']['name']
                        )
                    )
                    
                    cursor.execute(
                        "INSERT INTO event (event_id, index, period, timestamp, minute, "
                        "second, possession, duration, type_id, possession_team_id, play_pattern_id, team_id, player_id, match_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ",
                                            (
                                                event['id'],
                                                event['index'],
                                                event['period'],
                                                event['timestamp'],
                                                event['minute'],
                                                event['second'],
                                                event['possession'],
                                                event['duration'] if 'duration' in event else None,
                                                event['type']['id'],
                                                event['possession_team']['id'],
                                                event['play_pattern']['id'],
                                                event['team']['id'],
                                                event['player']['id'] if 'player' in event else None,
                                                match
                                            )
                                        )
                    if 'location' in event:
                        cursor.execute(
                                    "INSERT INTO location (event_id, length, width) "
                                    "VALUES (%s, %s, %s) ",
                                    (
                                        event['id'],
                                        event['location'][0],
                                        event['location'][1]
                                    )
                                )
                    if 'shot' in event:
                        cursor.execute(
                            "INSERT INTO shot_type (shot_type_id, name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (shot_type_id) DO NOTHING;",
                            (
                                event['shot']['type']['id'],
                                event['shot']['type']['name']
                            )
                        )
                        try:
                            height = event['shot']['end_location'][2]
                        except (KeyError, IndexError):
                            height = None
                        cursor.execute(
                            "INSERT INTO shot_location (event_id, length, width, height) "
                            "VALUES (%s, %s, %s, %s) ",
                            (
                                event['id'],
                                event['shot']['end_location'][0],
                                event['shot']['end_location'][1],
                                height
                            )
                        )
                        cursor.execute(
                            "INSERT INTO shot_outcome (shot_outcome_id, name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (shot_outcome_id) DO NOTHING;",
                            (
                                event['shot']['outcome']['id'],
                                event['shot']['outcome']['name']
                            )
                        )
                        cursor.execute(
                            "INSERT INTO shot_technique (shot_technique_id, name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (shot_technique_id) DO NOTHING;",
                            (
                                event['shot']['technique']['id'],
                                event['shot']['technique']['name']
                            )
                        )
                        cursor.execute(
                            "INSERT INTO shot (event_id, xg, first_time, one_on_one, key_pass_event_id, shot_type_id, shot_outcome_id, shot_technique_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ",
                            (
                                event['id'],
                                event['shot']['statsbomb_xg'],
                                event['shot']['first_time'] if 'first_time' in event['shot'] else None,
                                event['shot']['one_on_one'] if 'one_on_one' in event['shot'] else None,
                                event['shot']['key_pass_id'] if 'key_pass_id' in event['shot'] else None,
                                event['shot']['type']['id'],
                                event['shot']['outcome']['id'],
                                event['shot']['technique']['id']
                            )
                        )
                    if 'pass' in event:
                        cursor.execute(
                                "INSERT INTO pass_end_location (event_id, length, width) "
                                "VALUES (%s, %s, %s) ",
                                (
                                    event['id'],
                                    event['pass']['end_location'][0],
                                    event['pass']['end_location'][1]
                                )
                            )
                        if 'outcome' in event['pass']:
                            cursor.execute(
                                "INSERT INTO pass_outcome (outcome_id, name) "
                                "VALUES (%s, %s) "
                                "ON CONFLICT (outcome_id) DO NOTHING;",
                                (
                                    event['pass']['outcome']['id'],
                                    event['pass']['outcome']['name']
                                )
                            )
                        if 'technique' in event['pass']:
                            cursor.execute(
                                "INSERT INTO pass_technique (pass_technique_id, name) "
                                "VALUES (%s, %s) "
                                "ON CONFLICT (pass_technique_id) DO NOTHING;",
                                (
                                    event['pass']['technique']['id'],
                                    event['pass']['technique']['name']
                                )
                            )
                        if 'type' in event['pass']:
                            cursor.execute(
                                "INSERT INTO pass_type (pass_type_id, name) "
                                "VALUES (%s, %s) "
                                "ON CONFLICT (pass_type_id) DO NOTHING;",
                                (
                                    event['pass']['type']['id'],
                                    event['pass']['type']['name']
                                )
                            )
                        cursor.execute(
                            "INSERT INTO height (height_id, name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (height_id) DO NOTHING;",
                            (
                                event['pass']['height']['id'],
                                event['pass']['height']['name']
                            )
                        )
                        cursor.execute(
                            "INSERT INTO pass(event_id, length, angle, recipient_id, through_ball, outcome_id, height_id, type_id, technique_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ",
                            (
                                event['id'],
                                event['pass']['length'],
                                event['pass']['angle'],
                                event['pass']['recipient']['id'] if 'recipient' in event['pass'] else None,
                                event['pass']['through_ball'] if 'through_ball' in event['pass'] else None,
                                event['pass']['outcome']['id'] if 'outcome' in event['pass'] else None,
                                event['pass']['height']['id'],
                                event['pass']['type']['id'] if 'type' in event['pass'] else None,
                                event['pass']['technique']['id'] if 'technique' in event['pass'] else None
                            )
                        )
                    if 'dribble' in event:
                        cursor.execute(
                            "INSERT INTO dribble_outcome (dribble_outcome_id, name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (dribble_outcome_id) DO NOTHING;",
                            (
                                event['dribble']['outcome']['id'],
                                event['dribble']['outcome']['name'],
                            )
                        ) 
                        cursor.execute(
                            "INSERT INTO dribble (event_id, nutmeg, overrun, dribble_outcome_id) "
                            "VALUES (%s, %s, %s, %s) ",
                            (
                                event['id'],
                                event['dribble']['nutmeg'] if 'nutmeg' in event['dribble'] else None,
                                event['dribble']['overrun'] if 'overrun' in event['dribble'] else None,
                                event['dribble']['outcome']['id']
                            )
                        )
                    if 'duel' in event:
                        cursor.execute(
                            "INSERT INTO duel_type (duel_type_id, name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (duel_type_id) DO NOTHING;",
                            (
                                event['duel']['type']['id'],
                                event['duel']['type']['name'],
                            )
                        )

                        if 'outcome' in event['duel']:
                            cursor.execute(
                                "INSERT INTO duel_outcome (duel_outcome_id, name) "
                                "VALUES (%s, %s) "
                                "ON CONFLICT (duel_outcome_id) DO NOTHING;",
                                (
                                    event['duel']['outcome']['id'],
                                    event['duel']['outcome']['name'],
                                )
                            )
                        cursor.execute(
                            "INSERT INTO duel (event_id, outcome_id, type_id) "
                            "VALUES (%s, %s, %s) ",
                            (
                                event['id'],
                                event['duel']['outcome']['id'] if 'outcome' in event['duel'] else None,
                                event['duel']['type']['id']
                            )
                        )

                       
                for event in events:               
                    if 'related_events' in event:
                        for i in range(len(event['related_events'])):
                            cursor.execute(
                                "INSERT INTO related_events (event_id, related_event_id) "
                                "VALUES (%s, %s) ",
                                (
                                    event['id'],
                                    event['related_events'][i]
                                )
                            ) 
                               
# Function to import lineups
def import_lineups(match_ids):
    for match in match_ids:
        lineups_file_path = os.path.join(lineups_dir, f'{match}.json')
        if os.path.exists(lineups_file_path):
            with open(lineups_file_path, 'r') as file:
                lineups = json.load(file)
                for lineup in lineups:
                    for i in range(len(lineup['lineup'])):
                        cursor.execute(
                            "INSERT INTO country (country_id, country_name) "
                            "VALUES (%s, %s) "
                            "ON CONFLICT (country_id) DO NOTHING;",
                            (
                                lineup['lineup'][i]['country']['id'],
                                lineup['lineup'][i]['country']['name']
                            )
                        )
                        cursor.execute(
                            "INSERT INTO player (player_id, player_name, player_nickname, jersey_number, country_id) "
                            "VALUES (%s, %s, %s, %s, %s) "
                            "ON CONFLICT (player_id) DO NOTHING;",
                            (
                                lineup['lineup'][i]['player_id'],
                                lineup['lineup'][i]['player_name'],
                                lineup['lineup'][i]['player_nickname'] if 'player_nickname' in lineup['lineup'][i] else None,
                                lineup['lineup'][i]['jersey_number'] if 'jersey_number' in lineup['lineup'][i] else None,
                                lineup['lineup'][i]['country']['id']
                            )
                        )
                        for j in range(len(lineup['lineup'][i]['cards'])):
                            cursor.execute(
                                            "INSERT INTO card (player_id, time, card_type, reason, period) "
                                            "VALUES (%s, %s, %s, %s, %s) ",
                                            (
                                                lineup['lineup'][i]['player_id'],
                                                lineup['lineup'][i]['cards'][j]['time'],
                                                lineup['lineup'][i]['cards'][j]['card_type'],
                                                lineup['lineup'][i]['cards'][j]['reason'],
                                                lineup['lineup'][i]['cards'][j]['period']
                                            )
                                        )
                        for j in range(len(lineup['lineup'][i]['positions'])):                
                            cursor.execute(
                                "INSERT INTO positions (position_id, position_name, time_from, time_to, period_from, period_to, start_reason, end_reason, player_id) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                "ON CONFLICT (position_id,time_from,player_id) DO NOTHING;",
                                (
                                    lineup['lineup'][i]['positions'][j]['position_id'],
                                    lineup['lineup'][i]['positions'][j]['position'],
                                    lineup['lineup'][i]['positions'][j]['from'],
                                    lineup['lineup'][i]['positions'][j]['to'],
                                    lineup['lineup'][i]['positions'][j]['from_period'],
                                    lineup['lineup'][i]['positions'][j]['to_period'],
                                    lineup['lineup'][i]['positions'][j]['start_reason'],
                                    lineup['lineup'][i]['positions'][j]['end_reason'],
                                    lineup['lineup'][i]['player_id']
                                )
                        )

competitions_and_seasons = {'11': ['90', '42', '4'], '2': ['44']}

import_competitions_and_seasons()
match_ids = import_matches(competitions_and_seasons)
import_lineups(match_ids)
import_events(match_ids) 

conn.commit()
cursor.close()
conn.close()