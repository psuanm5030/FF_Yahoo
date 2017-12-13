"""
Purpose: Wrapper for Yahoo API.
Notes: This script retrieves data, parses / flattens it, and cleans it.  Most methods can be called independently with a league key.  League keys can be retrieved by calling the method 'get_leagues'.
"""

import Auth_Handler
import requests
import json
import xmltodict
import objectpath as op
import pandas as pd
import pprint as pp
from datetime import datetime
import collections
import db
import copy

# Overall
# todo Need to send the right types for fields
# todo remove Pandas
# todo limit refreshes
# todo could make calls more efficient - able to call more than one league at once.
# todo should i be using u'http://fantasysports.yahooapis.com/fantasy/v2/base.rng' xmlns
# Scoreboard
# todo need to clean up NaN and nan here / everywhere
# Managers
# todo Make guids the primary method of dealing
# Player Stats
# todo may want player stats each week in their own table and relate
# todo Weekly FreeAgent stats??
# Points
# todo Points conversions based on stat values
# todo create total_points
# Overall
# todo can I get overall standings?

class Yahoo_API(object):

    base_url = 'https://fantasysports.yahooapis.com/fantasy/v2/'

    def __init__(self,handler):
        self.token = handler.yahoo_auth.access_token
        self.league_raw = self.get_league_details()
        self.league_keys_list = self.league_raw.keys()

        self.league_stat_map = {}

        self.teams_count = None
        self.current_season = None
        self.current_week = None
        self.league_stats_flattened = None

        # Database
        self.db_league_details = None
        self.db_league_all_stats = None # 'value' are strings
        self.db_league_base_stats = None # 'value' are strings
        self.db_league_only_stats = None # 'value' are floats
        self.db_draft_with_players = None
        self.db_team_details = None
        self.db_scoreboard_one = None
        self.db_scoreboard_two = None
        self.db_league_transactions = None
        self.db_player_weekly_stats = None
        self.db_league_standings = None
        self.db_season_stats_all_players = None

    ## Helper Methods
    def send_query(self,url):
        """
        Makes request to API.
        :param url: e.g., 'https://fantasysports.yahooapis.com/fantasy/v2/league/359.l.67045'
        :return: dataframe
        """
        headers = {"Authorization": "bearer " + self.token,
                   "format":"json"}
        # url = 'https://fantasysports.yahooapis.com/fantasy/v2/league/359.l.67045'
        r = requests.get(url, headers=headers)
        details = xmltodict.parse(r.content) # in dictionary formats
        d1 = json.dumps(details)
        d2 = json.loads(d1) # in json format - to be used with object path
        return d2

    ## Helper Methods
    def format_league_details(self):
        new_data = []
        for lg in self.league_raw:
            new_data.append(self.clean_dict_keys({
                'league_key':lg['league_key'],
                'season':lg['season'],
                'name':lg['name'],
                'num_teams':lg['num_teams']}))
        return new_data

    def dict_for_db(self, data, **kwargs):
        """
        Takes dict and creates list of dicts with key inlcuded
        :return: list of dicts
        """
        key_name = kwargs.get('key_name', None)

        if key_name: # Return a listing of dicts with added keys with key name (per argument
            new_data = []
            for k,v in data.iteritems():
                v.update({key_name: k})
                new_data.append(v)
            return new_data
        else: # Return a listing of dicts
            new_data = []
            for k,v in data.iteritems():
                new_data.append(v)
            return new_data

    def dict_type_conversion(self,l,to_int=[],to_string=[],to_float=[]):
        """
        Takes a listing of dicts and performs conversions as requested.
        :param d: listing of dicts
        :param to_int: fields to convert to integer
        :param to_string: fields to convert to string
        :param to_float: fields to convert to float
        :return: listing of dicts
        """
        n1 = []
        for line in l:
            if len(to_int) > 0:
                line = {k:int(v) if k in to_int else v for k,v in line.items()}
            if len(to_string) > 0:
                line = {k: str(v) if k in to_string else v for k, v in line.items()}
            if len(to_float) > 0:
                line = {k: float(v) if k in to_float else v for k, v in line.items()}
            n1.append(line)
        return n1

    def clean_dict_keys(self, d):
        """
        Clean up the keys and prepare for DB.
            - Upper
            - Remove special chars
            - Unicode to string
        :return: dict
        :param d: Expects a dictionary
        :return:
        """
        new_d = {}
        for k,v in d.iteritems():
            k2 = k.upper()
            k2 = k2.replace("-", "_")
            k2 = k2.replace(" ", "_")
            k2 = k2.replace("@", "_")
            k2 = k2.replace("+", "")
            k2 = k2.encode("utf-8") # remove unicode reprsentations
            if k2.startswith('2_'):
                k2 = 'TWO_' + k2[2:]
            k2 = k2.replace('RECEPTION_','RECEIVING_')
            new_d.update({k2: v})
        return new_d

    def clean_dict_values(self, d):
        """
        Clean up the keys and prepare for DB.
            - Upper
            - Remove special chars
            - Unicode to string
            - Special instances with stat key names
        :return: dict
        :param d: Expects a dictionary
        :return:
        """
        new_d = {}
        for k,v in d.iteritems():
            v2 = v.upper()
            v2 = v2.replace("-", "_")
            v2 = v2.replace(" ", "_")
            v2 = v2.replace("+", "")
            v2 = v2.encode("utf-8") # remove unicode reprsentations
            v2 = v2.replace('RECEPTION_', 'RECEIVING_')
            if v2.startswith('2_'):
                v2 = 'TWO_' + v2[2:]
            new_d.update({k: v2})
        return new_d

    def clean_specific_key_values(self, d, key):
        """
        Clean up the values for a SPECIFIC KEY.
            - Upper
            - Remove special chars
            - Unicode to string
            - Special instances with stat key names
        :return: dict
        :param d: Expects a dictionary
        :return:
        """
        new_d = {}
        for k,v in d.iteritems():
            if k == key:
                v2 = v.upper()
                v2 = v2.replace("-", "_")
                v2 = v2.replace(" ", "_")
                v2 = v2.replace("+", "")
                v2 = v2.encode("utf-8") # remove unicode reprsentations
                if v2.startswith('2_'):
                    v2 = 'TWO_' + v2[2:]
                v2 = v2.replace('RECEPTION_', 'RECEIVING_')
                new_d.update({k: v2})
            else:
                new_d.update({k: v})
        return new_d

    def _make_stat_attrs_map(self, stat_dict):
        """

        :param stat_dict: Dictionary at this location:
            [u'fantasy_content'][u'league'][u'settings'][u'stat_categories'][u'stats'][u'stat']
        :return:
        """
        # Modifiers typically has less - if there is a state but no points are associated
        stat_dict_categories = stat_dict[u'stat_categories'][u'stats'][u'stat']
        stat_dict_modifiers = stat_dict[u'stat_modifiers'][u'stats'][u'stat']

        # Create the ID - Name map
        stat_attrs_map = {}
        for item in stat_dict_categories:
            item_settings = {}
            item_settings[item['stat_id']] = {'name' :item['name'].replace(' ' ,'_'),
                                              'position_type' :item['position_type']}
            stat_attrs_map.update(item_settings)

        # Add in the Stat Value
        for item in stat_dict_modifiers:
            stat_attrs_map[item['stat_id']]['value'] = item['value']

        return stat_attrs_map

    def admin_drop_suffixed_columns(self,df):
        keep_cols = [c for c in df.columns if '_del' not in c]
        df = df[keep_cols]
        return df

    def admin_rename_fields(self, df, rename_dict):
        df.rename(columns=rename_dict, inplace=True)  # Change column names

    def flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _parse_player_transaction(self ,d):
        """
        Parse the details for a player from transactions.
        :param d: dictionary of detail
        :return: flat dictionary
        """
        """
        {u'display_position': u'RB',
              u'editorial_team_abbr': u'LAR',
              u'name': {u'ascii_first': u'Lance',
                        u'ascii_last': u'Dunbar',
                        u'first': u'Lance',
                        u'full': u'Lance Dunbar',
                        u'last': u'Dunbar'},
              u'player_id': u'26064',
              u'player_key': u'359.p.26064',
              u'position_type': u'O',
              u'transaction_data': {u'destination_team_key': u'359.l.67045.t.3',
                                    u'destination_team_name': u"Beer's Ballers",
                                    u'destination_type': u'team',
                                    u'source_type': u'freeagents',
                                    u'type': u'add'}}
        """
        return self.flatten(d)

    def _retrieve_player_details(self, player_key, league_key):
        """
        Returns player information.
        :param player_key: e.g., '359.p.24171' # 2016 Antonio Brown
        **TYPE Status: ADDRESSED
        :return: dictionary with player_key as ID and value map
        """

        url = self.base_url + "league/{0}/players;player_keys={1}/stats".format(league_key, player_key)
        r = self.send_query(url)
        tree = op.Tree(r)
        dets = tree.execute('$..player[0]')
        try:
            pts = float(dets['player_points']['total'])
        except:
            pts = 0.0
        player_details = {dets['player_key']:
            self.clean_dict_keys({
                'bye_week': dets['bye_weeks']['week'],
                'name': dets['name']['full'],
                'fname': dets['name']['first'],
                'lname': dets['name']['last'],
                'player_id': dets['player_id'],
                'position_type': dets['position_type'],
                'uniform_number': dets['uniform_number'],
                'display_position': dets['display_position'],
                'team_abbr': dets['editorial_team_abbr'],
                'team': dets['editorial_team_full_name'],
                'team_key': dets['editorial_team_key'],
                'image_url': dets['image_url'],
                'season': dets['player_points']['season'],
                'player_points': pts,
                'league_key': league_key
            })
        }

        # Manage Stats Details
        """
        {u'2-Point_Conversions': 0.0,
         u'Completions': 0.0,
         u'Fumbles_Lost': 0.0,
         u'Interceptions': 0.0,
         u'Offensive_Fumble_Return_TD': 0.0,
         u'Passing_Touchdowns': 0.0,
         u'Passing_Yards': 0.0,
         u'Receiving_Touchdowns': 12.0,
         u'Receiving_Yards': 1284.0,
         u'Receptions': 106.0,
         u'Return_Touchdowns': 0.0,
         u'Return_Yards': 163.0,
         u'Rushing_Attempts': 3.0,
         u'Rushing_Touchdowns': 0.0,
         u'Rushing_Yards': 9.0,
         u'Targets': 154.0}
        """
        stats_dict = {}
        stats = tree.execute('$..player[0]..stat')
        test = [stats_dict.update({self.league_stat_map[x['stat_id']]['name']: float(x['value'])}) for x in stats]

        # Update Player dict with the stats functions
        player_details[dets['player_key']].update(self.clean_dict_keys(stats_dict))

        print 'Retrieved {0} from {1}.'.format(player_details[player_key]['NAME']
                                               , player_details[player_key]['SEASON'])

        """ Final looks like this: 
        {u'273.p.7760': {'BYE_WEEK': u'7',
                         'COMPLETIONS': 255.0,
                         'DISPLAY_POSITION': u'QB',
                         'FNAME': u'Jay',
                         'FUMBLES_LOST': 4.0,
                         'IMAGE_URL': u'https://s.yimg.com/iu/api/res/1.2/HOa2mZFG_jklfdGhkcJ.qA--~B/YXBwaWQ9c2hhcmVkO2NoPTIzMzY7Y3I9MTtjdz0xNzkwO2R4PTg1NztkeT0wO2ZpPXVsY3JvcDtoPTYwO3E9MTAwO3c9NDY-/https://s.yimg.com/xe/i/us/sp/v/nfl_cutout/players_l/20161007/7760.png',
                         'INTERCEPTIONS': 14.0,
                         'LEAGUE_KEY': u'273.l.314496',
                         'LNAME': u'Cutler',
                         'NAME': u'Jay Cutler',
                         'OFFENSIVE_FUMBLE_RETURN_TD': 0.0,
                         'PASSING_TOUCHDOWNS': 19.0,
                         'PASSING_YARDS': 3033.0,
                         'PLAYER_ID': u'7760',
                         'PLAYER_POINTS': 235.62,
                         'POSITION_TYPE': u'O',
                         'RECEPTIONS': 0.0,
                         'RECEPTION_TOUCHDOWNS': 0.0,
                         'RECEPTION_YARDS': 0.0,
                         'RETURN_TOUCHDOWNS': 0.0,
                         'RETURN_YARDS': 0.0,
                         'RUSHING_TOUCHDOWNS': 0.0,
                         'RUSHING_YARDS': 233.0,
                         'SEASON': u'2012',
                         'TEAM': u'Miami Dolphins',
                         'TEAM_ABBR': u'Mia',
                         'TEAM_KEY': u'nfl.t.15',
                         'TWO_POINT_CONVERSIONS': 0.0,
                         'UNIFORM_NUMBER': u'6'}}
        """

        return player_details  # dict by player_id

    def _map_stat(self, frames):
        """
        This maps the stat_key to the actual name of the stat.  '50' becomes 'RECEIPTIONS' for example.
        :param frames: Takes in listing of dicts with stat values
        :return: listing of dicts
        """
        # Map Stat Keys / IDs to formal names

        # Get the mapping
        mapping = {}
        for x in self.db_league_only_stats:
            mapping.update(self.clean_dict_values(self.clean_dict_keys({x['STAT_KEY']: x['NAME']})))

        # Run through the listing of dicts and replace each
        frames_new = []
        for f in frames:
            new_d = {}
            for k, v in f.iteritems():
                try:
                    new_d.update({mapping[k]: v})
                except:
                    new_d.update({k: v})
            frames_new.append(new_d)
        return frames_new

    # BUILD Items
    def get_league_details(self):
        """
        Get all the leagues for the user.
        :returns: dict with keys of LEAGUE_KEY
        """
        url = self.base_url + str.format('users;use_login=1/games/leagues')
        r = self.send_query(url)
        tree = op.Tree(r)

        games = [x for x in tree.execute('$..league')]
        new_games = []
        for g in games:
            if g['game_code'] != 'nfl':
                continue
            else:
                new_games.append(g)

        # DB Fmt
        n1 = self.dict_type_conversion(new_games,to_int=['num_teams'])
        n2 = []
        for d in n1:
            n2.append(self.clean_dict_keys(d))
        self.db_league_details = n2

        # Returnable - Dict with keys
        n3 = copy.deepcopy(n2)
        rtn = {x.pop('LEAGUE_KEY'):x for x in list(n3)}
        return rtn

    def get_league_settings_stats(self, league_key):
        """
        Gets the settings for the league.
        **TYPE Status: Unaddressed
        :returns:
        """
        # Setup "Current" details for the instance
        url = self.base_url + 'league/{}/settings'.format(league_key)
        r = self.send_query(url)
        tree = op.Tree(r)
        dets = [x for x in tree.execute('$..settings')][0]

        # Build the Base settings
        master = {}
        for k, v in dets.iteritems():
            if isinstance(v, dict):
                continue
            else:
                master[k] = v

        # Build the STAT settings
        stats = self._make_stat_attrs_map(dets)
        master.update({'stats':stats}) # adds a key for 'stats' with all the stat IDs and their values

        # Combine into large dictionary
        stats_master = {league_key:master}
        """ Format
        {'371.l.80648': {u'cant_cut_list': u'none',
                          u'draft_pick_time': u'105',
                          u'draft_time': u'1503788602',
                          u'draft_type': u'self',
                          u'has_multiweek_championship': u'0',
                          u'has_playoff_consolation_games': u'1',
                          u'is_auction_draft': u'0',
                          u'max_teams': u'12',
                          u'num_playoff_consolation_teams': u'6',
                          u'num_playoff_teams': u'6',
                          u'persistent_url': u'https://football.fantasysports.yahoo.com/league/cmboys',
                          u'pickem_enabled': u'1',
                          u'player_pool': u'ALL',
                          u'playoff_start_week': u'14',
                          u'post_draft_players': u'W',
                          u'scoring_type': u'head',
                          'stats': {u'10': {'name': u'Rushing_Touchdowns',
                            'position_type': u'O',
                            'value': u'6'},
                           u'11': {'name': u'Receptions', 'position_type': u'O', 'value': u'1'},
                           u'12': {'name': u'Receiving_Yards',
                            'position_type': u'O',
                            'value': u'0.05'},
                           u'13': {'name': u'Receiving_Touchdowns',
                            'position_type': u'O',
                            'value': u'6'},
                           u'14': {'name': u'Return_Yards', 'position_type': u'O', 'value': u'0.05'},
                           u'15': {'name': u'Return_Touchdowns', 'position_type': u'O', 'value': u'6'},
                           u'16': {'name': u'2-Point_Conversions',
                            'position_type': u'O',
                            'value': u'2'},
                           u'18': {'name': u'Fumbles_Lost', 'position_type': u'O', 'value': u'-2'},
                           u'19': {'name': u'Field_Goals_0-19_Yards',
                            'position_type': u'K',
                            'value': u'3'},
                           u'2': {'name': u'Completions', 'position_type': u'O', 'value': u'.5'},
                           u'20': {'name': u'Field_Goals_20-29_Yards',
                            'position_type': u'K',
                            'value': u'3'},
                           u'21': {'name': u'Field_Goals_30-39_Yards',
                            'position_type': u'K',
                            'value': u'3'},
                           u'22': {'name': u'Field_Goals_40-49_Yards',
                            'position_type': u'K',
                            'value': u'4'},
                           u'23': {'name': u'Field_Goals_50+_Yards',
                            'position_type': u'K',
                            'value': u'5'},
                           u'24': {'name': u'Field_Goals_Missed_0-19_Yards',
                            'position_type': u'K',
                            'value': u'-1'},
                           u'25': {'name': u'Field_Goals_Missed_20-29_Yards',
                            'position_type': u'K',
                            'value': u'-1'},
                           u'26': {'name': u'Field_Goals_Missed_30-39_Yards',
                            'position_type': u'K',
                            'value': u'-1'},
                           u'27': {'name': u'Field_Goals_Missed_40-49_Yards',
                            'position_type': u'K',
                            'value': u'-1'},
                           u'28': {'name': u'Field_Goals_Missed_50+_Yards',
                            'position_type': u'K',
                            'value': u'-1'},
                           u'29': {'name': u'Point_After_Attempt_Made',
                            'position_type': u'K',
                            'value': u'1'},
                           u'30': {'name': u'Point_After_Attempt_Missed',
                            'position_type': u'K',
                            'value': u'-2'},
                           u'31': {'name': u'Points_Allowed', 'position_type': u'DT'},
                           u'32': {'name': u'Sack', 'position_type': u'DT', 'value': u'1'},
                           u'33': {'name': u'Interception', 'position_type': u'DT', 'value': u'2'},
                           u'34': {'name': u'Fumble_Recovery', 'position_type': u'DT', 'value': u'2'},
                           u'35': {'name': u'Touchdown', 'position_type': u'DT', 'value': u'6'},
                           u'36': {'name': u'Safety', 'position_type': u'DT', 'value': u'2'},
                           u'37': {'name': u'Block_Kick', 'position_type': u'DT', 'value': u'2'},
                           u'4': {'name': u'Passing_Yards', 'position_type': u'O', 'value': u'0.02'},
                           u'5': {'name': u'Passing_Touchdowns', 'position_type': u'O', 'value': u'6'},
                           u'50': {'name': u'Points_Allowed_0_points',
                            'position_type': u'DT',
                            'value': u'15'},
                           u'51': {'name': u'Points_Allowed_1-6_points',
                            'position_type': u'DT',
                            'value': u'10'},
                           u'52': {'name': u'Points_Allowed_7-13_points',
                            'position_type': u'DT',
                            'value': u'7'},
                           u'53': {'name': u'Points_Allowed_14-20_points',
                            'position_type': u'DT',
                            'value': u'5'},
                           u'54': {'name': u'Points_Allowed_21-27_points',
                            'position_type': u'DT',
                            'value': u'3'},
                           u'55': {'name': u'Points_Allowed_28-34_points',
                            'position_type': u'DT',
                            'value': u'-3'},
                           u'56': {'name': u'Points_Allowed_35+_points',
                            'position_type': u'DT',
                            'value': u'-6'},
                           u'57': {'name': u'Offensive_Fumble_Return_TD',
                            'position_type': u'O',
                            'value': u'6'},
                           u'6': {'name': u'Interceptions', 'position_type': u'O', 'value': u'-2'},
                           u'78': {'name': u'Targets', 'position_type': u'O'},
                           u'8': {'name': u'Rushing_Attempts', 'position_type': u'O'},
                           u'82': {'name': u'Extra_Point_Returned',
                            'position_type': u'DT',
                            'value': u'2'},
                           u'9': {'name': u'Rushing_Yards', 'position_type': u'O', 'value': u'0.1'}},
                          u'trade_end_date': u'2017-11-11',
                          u'trade_ratify_type': u'commish',
                          u'trade_reject_time': u'1',
                          u'uses_faab': u'0',
                          u'uses_fractional_points': u'1',
                          u'uses_lock_eliminated_teams': u'1',
                          u'uses_negative_points': u'1',
                          u'uses_playoff': u'1',
                          u'uses_playoff_reseeding': u'0',
                          u'waiver_rule': u'gametime',
                          u'waiver_time': u'1',
                          u'waiver_type': u'R'}}
        """

        # Update the class variables
        self.league_stat_map = stats_master[league_key]['stats']

        # Setup the Base Stats for Import into DB
        base_stats = stats_master[league_key]
        base_stats.pop('stats', None)
        base_stats_fmt = []
        for k,v in base_stats.iteritems():
            det = {'league_key':league_key,
                   'stat_key':'',
                   'position_type':'base',
                   'name':k,
                   'value':v
                   }
            base_stats_fmt.append(self.clean_dict_keys(det))

        # PREPARE Listing of dicts for DB
        det_stats_fmt = self.dict_for_db(self.league_stat_map,key_name='stat_key')
        det_stats_fmt_ = []
        for item in det_stats_fmt:
            item.update({'league_key':league_key})
            det_stats_fmt_.append(self.clean_specific_key_values(self.clean_dict_keys(item),'NAME'))
        all_stats_w_league = det_stats_fmt_ + base_stats_fmt
        self.db_league_all_stats = all_stats_w_league
        self.db_league_base_stats = base_stats_fmt
        self.db_league_only_stats = self.dict_type_conversion(det_stats_fmt_ ,to_float=['VALUE'])

        # Setup some variables used later
        current_league = self.league_raw[league_key]
        self.team_count = current_league['NUM_TEAMS']
        self.current_season = current_league['SEASON']
        self.current_week = int(r['fantasy_content']['league']['current_week'])

        return all_stats_w_league

    def get_draft_by_leaguekey(self, league_key):
        """
        Get listing of picks for the leagues draft that season.  Matchup with player stat totals in that year.
        **TYPE Status: Unaddressed
        :return: dataframe
        """
        # Ensure that the settings are prepared.
        self.get_league_settings_stats(league_key)

        url = self.base_url + "league/{}/draftresults".format(league_key)
        r = self.send_query(url)
        tree = op.Tree(r)
        picks = [x for x in tree.execute('$..draft_result')]
        try:
            picks_d = {p.pop('player_key'): p for p in picks}
        except:
            # there is no player_key in 2003
            db_fmt = []
            for d in picks:
                new_v = {}
                new_v.update({'league_key':league_key})
                new_ = self.clean_dict_keys(new_v)
                db_fmt.append(new_)
            self.db_draft_with_players = db_fmt
            return db_fmt

        ###### PULL PLAYERS STATS ######
        # Update picks_d with current stats for players
        cnt = 0
        for player in picks_d.keys():
            cnt+=1
            ply = self._retrieve_player_details(player ,league_key)
            picks_d[player].update(ply[player])
            if cnt > 5:
                break

        # PREPARE Listing of dicts for DB
        db_fmt = []
        for k,v in picks_d.iteritems():
            new_v = {}
            for k2,v2 in v.iteritems():
                new_v.update({k2.upper():v2})
            new_ = self.clean_dict_keys(new_v)
            db_fmt.append(new_)
        db_fmt = self.dict_type_conversion(db_fmt,
                                           to_int=[],
                                           to_float=[])
        self.db_draft_with_players = db_fmt
        return picks_d

    def get_teams_detail(self, league_key):
        """
        Returns all relevant team details in dictionary format (for number of players).
        **TYPE Status: ADDRESSED
        :param league_key:
        :return: dict of details organized by team_key (e.g., 359.l.67045.t.6)
        """
        # Ensure that the settings are prepared.
        self.get_league_settings_stats(league_key)

        # Cycle through to request detail in each team
        team_details = {}
        for team in range(1, int(self.team_count) +1):
            url = self.base_url + "team/{}.t.{}".format(league_key,team)
            r = self.send_query(url)
            try: # handle rare issue where there is a team
                test = r['fantasy_content']
            except:
                print 'Skipping team {}.t.{} becuase API returned error.'.format(league_key,team)
                continue

            tree = op.Tree(r)
            picks = collections.defaultdict(lambda:None,[x for x in tree.execute('$..team')][0])
            # Deal with Multiple managers - take only the first.
            try:
                manager = collections.defaultdict(lambda:None,picks['managers']['manager'])
            except:
                manager = collections.defaultdict(lambda: None, picks['managers']['manager'][0])
            keep = {
                picks['team_key'] :{
                    'season':self.current_season,
                    'name':picks['name'],
                    'draft_grade':picks['draft_grade'],
                    'draft_recap_url':picks['draft_recap_url'],
                    'number_of_moves':picks['number_of_moves'],
                    'number_of_trades':picks['number_of_trades'],
                    'team_id':picks['team_id'],
                    'team_key':picks['team_key'],
                    'url' :picks['url'],
                    'waiver_priority':picks['waiver_priority'],
                    'logo_url':picks['team_logos']['team_logo']['url'],
                    'manager_guid':manager['guid'],
                    'manager_image_url':manager['image_url'],
                    'manager_nickname':manager['nickname']
                }
            }

            try:  # See if this detail is available - and update the match_details dictionary
                keep[picks['team_key']]['manager_email'] = manager['email']
            except:
                pass

            team_details.update(keep)

        # PREPARE Listing of dicts for DB
        t = self.dict_for_db(team_details,key_name='team_id')
        t_ = []
        for item in t:
            item.update({'league_key':league_key})
            item.pop('waiver_priority') # remove as its problematic
            t_.append(self.clean_dict_keys(item))
        n1 = self.dict_type_conversion(t_,
                                       to_int=['NUMBER_OF_MOVES','NUMBER_OF_TRADES','WAIVER_PRIORITY'],
                                       to_string=['SEASON','WAIVER_PRIORITY'])
        self.db_team_details = n1

        return team_details

    def get_scoreboard(self, league_key ,return_type='one'):
        """
        Returns all relevant team details in dictionary format (for number of players).
        **TYPE Status: ADDRESSED
        :param league_key:
        :return: dict of matchups (matchup a
        """
        # Ensure that the settings are prepared.
        self.get_league_settings_stats(league_key)

        # Cycle through to request detail in each team
        season_matchups = {}
        for week in range(1, self.current_week +1):
            url = self.base_url + "league/{}/scoreboard;week={}".format(league_key ,week)
            r = self.send_query(url)
            tree = op.Tree(r)
            matchup = [x for x in tree.execute('$..matchup')]
            week_matchups = {}
            for match ,i in zip(matchup ,range(1 ,len(matchup ) +1)):
                # Check if the matchup is in process
                # if datetime.today() < datetime.strptime(match['week_end'], "%Y-%m-%d"):
                if int(match['week']) == self.current_week:
                    continue

                # Deal with Multiple managers - take only the first.
                for ind, t_test in enumerate(match['teams']['team']):
                    if isinstance(t_test['managers']['manager'],list):
                        match['teams']['team'][ind]['managers']['manager'] = match['teams']['team'][ind]['managers']['manager'][0]

                # Organize the Teams level detail
                teams = match['teams']['team']
                if match['is_tied'] == '1':
                    t1_type = 'T1'
                    t2_type = 'T2'
                    winner_team_key = 'TIE'
                elif teams[0]['team_key'] == match['winner_team_key']:
                    t1_type = 'T1'
                    t2_type = 'T2'
                    winner_team_key = match['winner_team_key']
                else:
                    t1_type = 'T2'
                    t2_type = 'T1'
                    winner_team_key = match['winner_team_key']
                team_1 = self.flatten(teams[0] ,t1_type)
                team_2 = self.flatten(teams[1] ,t2_type)
                team_1.update(team_2)
                # organize the High-Level Matchup detail
                match_details = {
                    'matchup_week' :match['week'],
                    'matchup_week_end' :match['week_end'],
                    'matchup_week_start' :match['week_start'],
                    'matchup_winner_team_key':winner_team_key,
                    'matchup_consolation' :match['is_consolation'],
                    'matchup_is_playoffs' :match['is_playoffs'],
                    'matchup_is_tied' :match['is_tied']
                }
                try: # See if this detail is available - and update the match_details dictionary
                    new_match_details = {
                        'matchup_recap_title': match['matchup_recap_title'],
                        'matchup_recap_url': match['matchup_recap_url']
                    }
                    match_details.update(new_match_details)
                except:
                    pass

                team_1.update(match_details) # add this detail in
                # Add to the week compendium
                week_matchups.update({match['week_start' ] +' - ' +str(i) :team_1}) # Add in the matchup key
            # Add to the season compendium
            season_matchups.update(week_matchups)

        # Dictionary Verison - One and Two Liners
        new_one = season_matchups
        new_two = {}
        for k,v in season_matchups.iteritems():
            new_w = {}
            new_l = {}
            for k2,v2 in v.iteritems():
                if k2.startswith('T2_'):
                    new_l.update({k2[3:]:v2})
                elif k2.startswith('T1_'):
                    new_w.update({k2[3:]:v2})
                else:
                    new_l.update({k2: v2})
                    new_w.update({k2: v2})
            new_two[k + ' - T2'] = new_l
            new_two[k + ' - T1'] = new_w

        # PREPARE Listing of dicts for DB
        # One_Liner
        t1_ = []
        new_one = self.dict_for_db(new_one,key_name='matchup_key')
        for item in new_one:
            item.update({'league_key':league_key})
            t1_.append(self.clean_dict_keys(item))
        t1_1 = self.dict_type_conversion(t1_,
                                         to_int=['T2_NUMBER_OF_MOVES','T2_NUMBER_OF_TRADES','T1_NUMBER_OF_MOVES','T1_NUMBER_OF_TRADES'],
                                         to_float=['T2_TEAM_PROJECTED_POINTS_TOTAL','T2_TEAM_POINTS_TOTAL','T1_TEAM_POINTS_TOTAL','T1_TEAM_PROJECTED_POINTS_TOTAL','T2_WIN_PROBABILITY','T1_WIN_PROBABILITY'])
        self.db_scoreboard_one = t1_1

        # Two_Liner
        t2_ = []
        new_two = self.dict_for_db(new_two, key_name='matchup_key')
        for item in new_two:
            item.update({'league_key':league_key})
            t2_.append(self.clean_dict_keys(item))
        t2_1 = self.dict_type_conversion(t2_,
                                         to_int=['NUMBER_OF_MOVES','NUMBER_OF_TRADES','NUMBER_OF_MOVES','NUMBER_OF_TRADES'],
                                         to_float=['TEAM_PROJECTED_POINTS_TOTAL','TEAM_POINTS_TOTAL','TEAM_POINTS_TOTAL','TEAM_PROJECTED_POINTS_TOTAL','WIN_PROBABILITY','WIN_PROBABILITY'])
        self.db_scoreboard_two = t2_1

        # Return data based upon choice
        if str.upper(return_type )=='TWO':
            print 'Completed Scoreboard - returning TWO-line matchup dict.'
            return t2_1
        else:
            print 'Completed Scoreboard - returning ONE-line matchup dict.'
            return t1_1

    def get_league_standings(self, league_key):
        """
        Gets the standings for the league.
        **TYPE Status: ADDRESSED
        :returns: Dict with keys as team id
        """
        # Setup "Current" details for the instance
        url = self.base_url + 'league/{}/standings'.format(league_key)
        r = self.send_query(url)
        tree = op.Tree(r)
        dets = [x for x in tree.execute('$..team')]

        dets2 = []
        for d in dets:
            try:
                d.pop('waiver_priority')
            except:
                pass
            # Check for 2 managers
            if isinstance(d['managers']['manager'],list):
                d['managers']['manager'] = d['managers']['manager'][0] # take first manager only
            d.update({'LEAGUE_KEY':league_key})
            dets2.append(self.clean_dict_keys(self.flatten(d)))
        dets2 = self.dict_type_conversion(dets2,to_int=['TEAM_STANDINGS_RANK','TEAM_STANDINGS_PLAYOFF_SEED','NUMBER_OF_MOVES','NUMBER_OF_TRADES','TEAM_STANDINGS_OUTCOME_TOTALS_WINS','TEAM_STANDINGS_OUTCOME_TOTALS_LOSSES','TEAM_STANDINGS_OUTCOME_TOTALS_TIES'], to_float=['TEAM_POINTS_TOTAL','TEAM_STANDINGS_OUTCOME_TOTALS_PERCENTAGE','TEAM_STANDINGS_POINTS_AGAINST','TEAM_STANDINGS_POINTS_FOR','TEAM_STANDINGS_STREAK_VALUE'])
        self.db_league_standings = dets2
        dets_w_keys = {x.pop('TEAM_KEY'):x for x in dets2}
        return dets_w_keys

    def get_league_transactions(self, league_key, trans_type='adds'):
        """
        Gets the transactions for the league.
        **TYPE Status: Unaddressed
        :returns:
        """
        # todo add player stats
        # Setup "Current" details for the instance
        url = self.base_url + 'league/{}/transactions'.format(league_key)
        r = self.send_query(url)
        tree = op.Tree(r)
        dets = [x for x in tree.execute('$..transaction')]

        frames = []
        # Run through each transaction
        for trans in dets:
            if trans['type'] == 'commish': continue # ignore commish only moves - no clue what this is.

            # Gather meta data
            meta = {
                'type': trans['type'],
                'status': trans['status'],
                'time': trans['timestamp'],
                'id': trans['transaction_id'],
                'key': trans['transaction_key'],
                'players_moved': int(trans['players']['@count'])
            }

            meta_frame = pd.DataFrame.from_dict(meta, orient='index').T

            if trans['players']['@count'] == '1':  # Deal with 1 player
                player_flat = self._parse_player_transaction(trans['players']['player'] )
                player_frame = pd.DataFrame.from_dict(player_flat, orient='index').T
                both_frames = pd.concat([meta_frame, player_frame], axis=1).set_index(['id'])
                frames.append(both_frames)
            else:  # for multiple players
                for player in trans['players']['player']:
                    player_flat = self._parse_player_transaction(player)
                    player_frame = pd.DataFrame.from_dict(player_flat, orient='index').T
                    both_frames = pd.concat([meta_frame, player_frame], axis=1).set_index(['id'])
                    frames.append(both_frames)

        df = pd.concat(frames ,axis=0)
        df.index.names = ['trans_id']
        df = df.reset_index()

        # Convert all Timestamps
        try:
            df['time'] = df['time'].apply(lambda x: datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S'))
        except:
            pass

        # PREPARE Listing of dicts for DB
        t1 = self.dict_for_db(df.T.to_dict())
        t2 = []
        for item in t1:
            item.update({'league_key':league_key})
            t2.append(self.clean_dict_keys(item))
        self.db_league_transactions = t2

        # todo address types.
        return df

    def get_roster_stats_week(self, league_key):
        """
        Time: Estimated to take several mins
        Get roster for a particular week with asscoiated stats.
        **TYPE Status: Unaddressed
        :param league_key:
        :param week:
        :return:
        """
        # First fun Team_Details
        leagues = self.get_league_settings_stats(league_key)
        teams1 = self.get_teams_detail(league_key)
        tdy = str(datetime.today())[:10]

        # Run through each team
        frames=[]
        for week in range(1,self.current_week+1): # Weeks
            print 'Retrieving week {} stats'.format(week)
            for team in teams1.keys(): # Teams
                url_team_players = self.base_url + "teams;team_keys={}/roster;week={}/players/stats".format(team ,int(week))
                r = self.send_query(url_team_players)
                tree = op.Tree(r)
                roster = [x for x in tree.execute('$..player')]
                teams_det = teams1[team]
                teams_det.update({'week':week})

                for player in roster: # Players
                    player_det = self.flatten(player)
                    stats = player_det.pop('player_stats_stats_stat')
                    # Abstract the Stats
                    stats_det = {}
                    for stat in stats:
                        stat2 = {stat[u'stat_id']:float(stat[u'value'])}
                        stats_det.update(stat2)
                    player_det.update(stats_det)
                    # player_det.update(teams_det) # why is this here?
                    frames.append(player_det)
            break # Break after 1 week

        # Map Stat Keys / IDs to formal names
        frames_new = self._map_stat(frames_new)

        # PREPARE Listing of dicts for DB
        t = []
        for item in frames_new:
            t.append(self.clean_dict_keys(item))
        self.dict_type_conversion(t,
                                  to_int=['NUMBER_OF_MOVES','NUMBER_OF_TRADES'],
                                  to_float=['PLAYER_POINTS_TOTAL'],
                                  to_string=['WEEK'])
        self.db_player_weekly_stats = t

        return

    def get_players_all(self, league_key):
        """
        Get all players
        :param league_key:
        :return:
        """
        self.get_league_settings_stats(league_key)
        request_times = 90 # this is roughly 2000 players (25 per call)
        print 'Starting All Player Season Stats for {}'.format(league_key)

        # Run
        roster_flat = []
        for i in range(0,request_times):
            start = (i * 25) + 1
            # stop = ((i+1)*25) + 1
            count = 25

            print 'Working through request {} of {}'.format(i,request_times)
            url = self.base_url + "leagues;league_keys={}/players;out=stats,percent_owned,ownership,draft_analysis;start={};count={}".format(league_key,start,count)
            r = self.send_query(url)
            # Check for errors (found some issues where a "player was not found"
            # This next loop runs until there is no error.
            if 'error' in r:
                print 'Error: {}. Incrementing by 1 now instead of 25.'
                start += 1 # + 1 because last start didnt work
                count -= 2
                end = start + count
                for ii in range(start,end):
                    url = self.base_url + "leagues;league_keys={}/players;out=stats,percent_owned,ownership,draft_analysis;start={};count={}".format(
                        league_key,ii,count)
                    r = self.send_query(url)
                    if 'error' in r:
                        start += 1
                        count -= 1
                        continue
                    else:
                        print 'Error past at player: {}.'.format(str(ii))
                        break
            elif r['fantasy_content']['leagues']['league']['players'] == None:
                print 'No more players being surfaced.  Breaking out of requests.  Sent {} requests.'.format(str(i))
                break

            tree = op.Tree(r)
            roster = [x for x in tree.execute('$..player')]
            for ply in roster:
                ply_new = self.flatten(ply)
                # Take out the stats to flatten
                stats = ply_new.pop('player_stats_stats_stat')
                stats_det = {}
                for stat in stats:
                    stat2 = {stat[u'stat_id']: float(stat[u'value'])}
                    stats_det.update(stat2)
                # Put the Stats back in
                checks = ['draft_analysis_average_cost','draft_analysis_average_pick','draft_analysis_average_round','draft_analysis_percent_drafted']
                ply_new = {k:(v.replace('-','0.0') if k in checks else v) for k,v in ply_new.items()}
                ply_new.update(stats_det)
                ply_new.update({'league_key': league_key}) # add League Key
                roster_flat.append(ply_new)

        # Map the stats
        roster_clean = self._map_stat(roster_flat)
        roster_clean = [self.clean_dict_keys(x) for x in roster_clean]
        roster_clean = self.dict_type_conversion(roster_clean,
                                                 to_int=['PERCENT_OWNED_DELTA','PERCENT_OWNED_VALUE','PERCENT_OWNED_WEEK','OWNERSHIP_TEAMS_TEAM_NUMBER_OF_MOVES','OWNERSHIP_TEAMS_TEAM_NUMBER_OF_TRADES'],
                                                 to_float=['PLAYER_POINTS_TOTAL','DRAFT_ANALYSIS_AVERAGE_COST','DRAFT_ANALYSIS_AVERAGE_PICK','DRAFT_ANALYSIS_AVERAGE_ROUND','DRAFT_ANALYSIS_PERCENT_DRAFTED'])
        self.db_season_stats_all_players = roster_clean
        return roster_clean


    # API Methods - Not Completed
    def get_roster_today(self, league_key):
        """
        Gets the roster of players for each fantasy team.
        :param league_key:
        :return:
        """
        # todo should i have a sub-funtion that gets the points
        # todo should i get stats by week?
        # First fun Team_Details
        leagues = self.get_league_settings_stats(league_key)
        teams = self.get_teams_detail(league_key)
        tdy = str(datetime.today())[:10]

        # Run through each team
        frames = []
        for team in teams.keys():
            # url = self.base_url + "teams;team_keys={}/roster;date={}".format(team,tdy)
            url = self.base_url + "teams;team_keys={}/roster".format(team)
            # 'http://fantasysports.yahooapis.com/fantasy/v2/teams;team_keys=238.l.627060.t.8/roster;date=2010-05-14'
            r = self.send_query(url)
            tree = op.Tree(r)
            roster = [x for x in tree.execute('$..player')]
            meta_frame = pd.DataFrame.from_dict(teams[team], orient='index').T

            for player in roster:
                player_flat = self.flatten(player)
                player_frame = pd.DataFrame.from_dict(player_flat, orient='index').T
                both_frames = pd.concat([meta_frame, player_frame], axis=1).set_index(['team_key'])
                frames.append(both_frames)

        # Form into a dataframe
        df = pd.concat(frames, axis=0)
        df.index.names = ['team_key']
        df = df.reset_index()

        # Keep only necessary coluns
        keep_cols = ['team_key',
                     'manager_guid',
                     'manager_nickname',
                     'name',
                     'name_full',
                     'player_key',
                     'display_position',
                     'bye_weeks_week',
                     'selected_position_week',
                     'selected_position_position',
                     'waiver_priority']
        df_cln = df[keep_cols]
        rename_cols = ['Mgr_Team_Key',
                       'Mgr_GUID',
                       'Mgr_Nickname',
                       'Mgr_Name',
                       'Player_FullName',
                       'Player_Key',
                       'Player_Position',
                       'Player_Bye',
                       'Week',
                       'Roster_Position',
                       'Mgr_Waiver_Priority']
        df_cln.columns = rename_cols

        return df_cln

    def get_200_free_agents(self, league_key ,position=None, type=''):
        """
        Get roster for a particular week with asscoiated stats.  Listing is sorted by "overall rank" which will show the best for the week.
        :param league_key:
        :param position:
        :param type: A (all available players) FA (free agents only, W (waivers only), T (all taken players), K (keepers only)
        :return:
        """
        # First fun Team_Details
        leagues = self.get_league_settings_stats(league_key)

        # Get 8 groups of top 25s
        master_players = []
        for i in range(0 ,8):
            if position:
                url = self.base_url + "league/{}/players;position={};status={};start={};count=25;sort=AR".format \
                    (league_key ,position ,type ,((i * 25) + 1))
            else:
                url = self.base_url + "league/{}/players;status={};start={};count=25;sort=AR".format(league_key ,type
                                                                                                      ,(( i *25 ) +1))
            r = self.send_query(url)
            tree = op.Tree(r)
            player_lst = [x for x in tree.execute('$..player')]
            for p in player_lst:
                master_players.append(p)

        # todo this next bunch of code is used in many places.  You can make a function
        # Parse the results
        frames = []
        for p in master_players:
            player_flat = self.flatten(p)
            player_frame = pd.DataFrame.from_dict(player_flat, orient='index').T
            frames.append(player_frame)
            print 'ds'

        # Create the DF
        df = pd.concat(frames, axis=0)
        df = df.set_index('player_key')
        # df.index.names = ['player_key']
        df = df.reset_index()

        # Keep only necessary coluns
        keep_cols = ['player_key',
                     'name_full',
                     'status',
                     'status_full',
                     'has_player_notes',
                     'display_position',
                     'editorial_team_abbr',
                     'editorial_team_full_name']
        df_cln = df[keep_cols]
        rename_cols = ['Player_Key',
                       'Player_FullName',
                       'Player_Status',
                       'Player_Status_Full',
                       'Player_Has_notes',
                       'Player_Position',
                       'Team_Abbr',
                       'Team']
        df_cln.columns = rename_cols

        return df_cln



if __name__ == '__main__':
    h = Auth_Handler.Auth()
    api = Yahoo_API(h)
    # d = db.db_Storage('aws_master')
    # d.delete_all_tables()

    # Testing
    league_key = '348.l.294871'
    # url = 'https://fantasysports.yahooapis.com/fantasy/v2/' + 'league/{}/standings'.format(league_key)
    # r_old = api.send_query(url)
    # r_new = api.send_query_TEST(url)
    # print 'd'
    # Andy TEsitng
    # api.get_roster_stats_week(league_key)
    api.get_league_details(league_key)
    api.get_players_all(league_key)

    # DB: Store League / Stat Details
    # api.get_league_settings_stats(api.league_keys_list[27])
    # d.create_table(api.db_leagues, table_name='league_details')
    # d.create_table(api.db_league_all_stats, table_name='league_all_stat_values')
    # d.create_table(api.db_league_only_stats, table_name='league_only_stat_values')

    # DB: Store Team Details
    # api.get_teams_detail(api.league_keys_list[27])
    # d.create_table(api.db_team_details, table_name='team_details')

    # DB: Draft with Player Stats
    # api.get_draft_by_leaguekey(api.league_keys_list[27])
    # d.create_table(api.db_draft_with_players, table_name='draft_with_players')

    # SCOREBOARD
    # api.get_scoreboard('359.l.67045')
    # d.create_table(api.db_scoreboard_one, table_name='scoreboard_one')
    # d.create_table(api.db_scoreboard_two, table_name='scoreboard_two')

    # DB: League Standings
    # api.get_league_standings('348.l.294871')
    # d.create_table(api.db_league_standings, table_name='league_standings')

    # DB: League Transactions
    # api.get_league_transactions(api.league_keys_list[27])
    # d.create_table(api.db_league_transactions, table_name='league_transactions')

    # DB: Weekly Player Stats
    # api.get_roster_stats_week(api.league_keys_list[27])
    # d.create_table(api.db_player_weekly_stats, table_name='player_weekly_stats')


    print 'done'


"""
Tables to Create: 
- League Details: Shows all league keys and associated information
- League Stats: Shows all the league stat keys
- Draft with Player Stats: Shows the draft and the player stasts
    - ** Will need to think about how to update this in season.
"""