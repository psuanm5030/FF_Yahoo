"""
Purpose: Manage database models and connections for creating / deleting / updating tables.
"""

from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date, Sequence
from sqlalchemy.orm import sessionmaker
import yaml

Base = declarative_base()

class db_Storage(object):

    # todo Primary keys.  Need to have legit primary keys for every row to make updating better without having to do it all
    """
    PRIMARY KEYS
    league_details - LEAGUE_KEY
    league_all_stat_values - ID
    league_only_stat_values - LEAGUE_KEY + ' - ' + STAT_KEY
    draft_with_players - LEAGUE_KEY + ' - ' + PICK
    team_details - TEAM_KEY
    scoreboard_one - MATCHUP_ID + LEAGUE_KEY
    scoreboard_two - ??????
    league_transactions - LEAGUE_KEY + ' - ' + TRANS_ID + ' - ' + TRANSACTION_DATA_TYPE
    player_weekly_stats - LEAGUE_KEY + ' - ' + PLAYER_KEY / PLAYER_ID
    league_standings - LEAGUE_KEY + ' - ' + TEAM_ID
    season_stats_all_players - LEAGUE_KEY + ' - ' + PLAYER_KEY / PLAYER_ID
    """

    def __init__(self, which):
        self.db_creds = self.get_creds(which)
        self.engine = self.db_open()
        self.table_check = {
            # 'test': test,
            'league_details':league_details,
            'league_all_stat_values':league_all_stat_values,
            'league_only_stat_values':league_only_stat_values,
            'draft_with_players':draft_with_players,
            'team_details':team_details,
            'scoreboard_one':scoreboard_one,
            'scoreboard_two':scoreboard_two,
            'league_transactions':league_transactions,
            'player_weekly_stats':player_weekly_stats,
            'league_standings':league_standings,
            'season_stats_all_players':season_stats_all_players
        }

    def get_creds(self, which):
        stream = file('credentials_master.yml', 'r')
        creds = yaml.load(stream)[which]
        return creds

    def table_choice(self,table):
        if table not in self.table_check:
            raise ValueError('Bad table: {}'.format(self.table))
        return self.table_check[table]

    def db_open(self):
        return create_engine(URL(**self.db_creds), isolation_level="AUTOCOMMIT")

    def create_table(self, data, table_name):
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        s = Session()
        t = self.table_choice(table_name)

        # TESTING ONLY
        for index, value in enumerate(data,1):
            record = t(**value)
            s.add(record)  # Add all the records
            s.commit()
        print 'Done Committing Records!  Manual'
        s.close()

        # try:
        #     for index, value in enumerate(data,1):
        #         record = t(**value)
        #         s.add(record)  # Add all the records
        #     print 'Committing Records for {}'.format(t.__tablename__)
        #     s.commit()
        # except:
        #     print '***** Rolling back ***** Table: {}'.format(t.__tablename__)
        #     s.rollback()
        # finally:
        #     print 'Closing connection.'
        #     s.close()

    def delete_table(self, table_name):
        try:
            t = self.table_choice(table_name)
            t.__table__.drop(self.engine)
            print 'Deleted table: {}'.format(t.__tablename__)
        except:
            print 'Table is not created or could not be deleted.'
        # meta = MetaData(self.engine, reflect=True)
        # t = meta.tables[self.table]
        # self.engine.execute(t.delete().where(t.c.Primary_Key != ''))

    def delete_all_tables(self):
        for t_name, table in self.table_check.iteritems():
            try:
                table.__table__.drop(self.engine)
                print 'Deleted table: {}'.format(t_name)
            except:
                print 'Table is not created or could not be deleted.'
        # meta = MetaData(self.engine, reflect=True)
        # t = meta.tables[self.table]
        # self.engine.execute(t.delete().where(t.c.Primary_Key != ''))

# DB Models
class test(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'test'

    # Define Columns
    Primary_Key = Column(String, primary_key=True)
    name = Column(String)
    age = Column(Integer)

    def __repr__(self):
        return "<Test(id='%s', name='%s', age='%s')>" % (self.Primary_Key,self.name, self.age)

class league_details(Base):
    __tablename__ = 'league_details'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    ALLOW_ADD_TO_DL_EXTRA_POS = Column(String)
    CURRENT_WEEK = Column(String)
    DRAFT_STATUS = Column(String)
    EDIT_KEY = Column(String)
    END_DATE = Column(String)
    END_WEEK = Column(String)
    GAME_CODE = Column(String)
    IS_CASH_LEAGUE = Column(String)
    IS_FINISHED = Column(String)
    IS_PRO_LEAGUE = Column(String)
    LEAGUE_ID = Column(String)
    LEAGUE_KEY = Column(String)
    LEAGUE_TYPE = Column(String)
    LEAGUE_UPDATE_TIMESTAMP = Column(String)
    NAME = Column(String)
    NUM_TEAMS = Column(Integer)
    PASSWORD = Column(String)
    RENEW = Column(String)
    RENEWED = Column(String)
    SCORING_TYPE = Column(String)
    SEASON = Column(String)
    SHORT_INVITATION_URL = Column(String)
    START_DATE = Column(String)
    START_WEEK = Column(String)
    URL = Column(String)
    WEEKLY_DEADLINE = Column(String)


    def __repr__(self):
        return "<League Details (Season='%s', Name='%s', Teams='%s')>" % (self.season, self.name,self.num_teams)

class league_all_stat_values(Base):
    __tablename__ = 'league_all_stat_values'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    LEAGUE_KEY = Column(String)
    STAT_KEY = Column(String)
    NAME = Column(String)
    VALUE = Column(String)
    POSITION_TYPE = Column(String)

    def __repr__(self):
        return "<Status(stat_key='%s', name='%s', value='%s', league='%s')>" % (self.stat_key, self.name,self.value,self.league_key)

class league_only_stat_values(Base):
    __tablename__ = 'league_only_stat_values'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    LEAGUE_KEY = Column(String)
    STAT_KEY = Column(String)
    NAME = Column(String)
    VALUE = Column(Float)
    POSITION_TYPE = Column(String)

    def __repr__(self):
        return "<Status(stat_key='%s', name='%s', value='%f', league='%s')>" % (self.stat_key, self.name,self.value,self.league_key)

class draft_with_players(Base):
    __tablename__ = 'draft_with_players'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    BLOCK_KICK = Column(Float)
    BYE_WEEK = Column(String)
    COMPLETIONS = Column(Float)
    DISPLAY_POSITION = Column(String)
    EXTRA_POINT_RETURNED = Column(Float)
    FIELD_GOALS_0_19_YARDS = Column(Float)
    FIELD_GOALS_20_29_YARDS = Column(Float)
    FIELD_GOALS_30_39_YARDS = Column(Float)
    FIELD_GOALS_40_49_YARDS = Column(Float)
    FIELD_GOALS_50_YARDS = Column(Float)
    FIELD_GOALS_MISSED_0_19_YARDS = Column(Float)
    FIELD_GOALS_MISSED_20_29_YARDS = Column(Float)
    FIELD_GOALS_MISSED_30_39_YARDS = Column(Float)
    FIELD_GOALS_MISSED_40_49_YARDS = Column(Float)
    FIELD_GOALS_MISSED_50_YARDS = Column(Float)
    FNAME = Column(String)
    FUMBLE_RECOVERY = Column(Float)
    FUMBLES_LOST = Column(Float)
    IMAGE_URL = Column(String)
    INTERCEPTION = Column(Float)
    INTERCEPTIONS = Column(Float)
    LEAGUE_KEY = Column(String)
    LNAME = Column(String)
    NAME = Column(String)
    OFFENSIVE_FUMBLE_RETURN_TD = Column(Float)
    PASSING_TOUCHDOWNS = Column(Float)
    PASSING_YARDS = Column(Float)
    PICK = Column(String)
    PLAYER_ID = Column(String)
    PLAYER_KEY = Column(String)
    PLAYER_POINTS = Column(Float)
    POINT_AFTER_ATTEMPT_MADE = Column(Float)
    POINT_AFTER_ATTEMPT_MISSED = Column(Float)
    POINTS_ALLOWED = Column(Float)
    POINTS_ALLOWED_0_POINTS = Column(Float)
    POINTS_ALLOWED_14_20_POINTS = Column(Float)
    POINTS_ALLOWED_1_6_POINTS = Column(Float)
    POINTS_ALLOWED_21_27_POINTS = Column(Float)
    POINTS_ALLOWED_28_34_POINTS = Column(Float)
    POINTS_ALLOWED_35_POINTS = Column(Float)
    POINTS_ALLOWED_7_13_POINTS = Column(Float)
    POSITION_TYPE = Column(String)
    RECEIVING_TOUCHDOWNS = Column(Float)
    RECEIVING_YARDS = Column(Float)
    RECEPTIONS = Column(Float)
    RETURN_TOUCHDOWNS = Column(Float)
    RETURN_YARDS = Column(Float)
    ROUND = Column(String)
    RUSHING_ATTEMPTS = Column(Float)
    RUSHING_TOUCHDOWNS = Column(Float)
    RUSHING_YARDS = Column(Float)
    SACK = Column(Float)
    SACKS = Column(Float)
    SAFETY = Column(Float)
    SEASON = Column(String)
    TARGETS = Column(Float)
    TEAM = Column(String)
    TEAM_ABBR = Column(String)
    TEAM_KEY = Column(String)
    TWO_POINT_CONVERSIONS = Column(Float)
    TOUCHDOWN = Column(Float)
    UNIFORM_NUMBER = Column(String)

    def __repr__(self):
        return "<Draft(PLAYER_KEY='%s', NAME='%s')>" % (self.PLAYER_KEY, self.NAME)

class team_details(Base):
    __tablename__ = 'team_details'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    DRAFT_GRADE = Column(String)
    DRAFT_RECAP_URL = Column(String)
    LEAGUE_KEY = Column(String)
    LOGO_URL = Column(String)
    MANAGER_EMAIL = Column(String)
    MANAGER_GUID = Column(String)
    MANAGER_IMAGE_URL = Column(String)
    MANAGER_NICKNAME = Column(String)
    NAME = Column(String)
    NUMBER_OF_MOVES = Column(Integer)
    NUMBER_OF_TRADES = Column(Integer)
    SEASON = Column(String)
    TEAM_ID = Column(String)
    TEAM_KEY = Column(String)
    URL = Column(String)
    WAIVER_PRIORITY = Column(String)


    def __repr__(self):
        return "<Team Details(Name='%s', email='%s')>" % (self.name, self.manager_email)

class league_standings(Base):
    __tablename__ = 'league_standings'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    CLINCHED_PLAYOFFS = Column(String)
    DRAFT_POSITION = Column(String)
    DRAFT_GRADE = Column(String)
    DRAFT_RECAP_URL = Column(String)
    HAS_DRAFT_GRADE = Column(String)
    IS_OWNED_BY_CURRENT_LOGIN = Column(String)
    LEAGUE_SCORING_TYPE = Column(String)
    LEAGUE_KEY = Column(String)
    MANAGERS_MANAGER_GUID = Column(String)
    MANAGERS_MANAGER_EMAIL = Column(String)
    MANAGERS_MANAGER_IMAGE_URL = Column(String)
    MANAGERS_MANAGER_IS_CURRENT_LOGIN = Column(String)
    MANAGERS_MANAGER_IS_COMMISSIONER = Column(String)
    MANAGERS_MANAGER_MANAGER_ID = Column(String)
    MANAGERS_MANAGER_NICKNAME = Column(String)
    NAME = Column(String)
    NUMBER_OF_MOVES = Column(Integer)
    NUMBER_OF_TRADES = Column(Integer)
    ROSTER_ADDS_COVERAGE_TYPE = Column(String)
    ROSTER_ADDS_COVERAGE_VALUE = Column(String)
    ROSTER_ADDS_VALUE = Column(String)
    TEAM_ID = Column(String)
    TEAM_LOGOS_TEAM_LOGO_SIZE = Column(String)
    TEAM_LOGOS_TEAM_LOGO_URL = Column(String)
    TEAM_LOGOS_TEAM_LOGO_MISSING_LOGO_SIZE = Column(String)
    TEAM_POINTS_COVERAGE_TYPE = Column(String)
    TEAM_POINTS_SEASON = Column(String)
    TEAM_POINTS_TOTAL = Column(Float)
    TEAM_STANDINGS_OUTCOME_TOTALS_LOSSES = Column(Integer)
    TEAM_STANDINGS_OUTCOME_TOTALS_PERCENTAGE = Column(Float)
    TEAM_STANDINGS_OUTCOME_TOTALS_TIES = Column(Integer)
    TEAM_STANDINGS_OUTCOME_TOTALS_WINS = Column(Integer)
    TEAM_STANDINGS_PLAYOFF_SEED = Column(Integer)
    TEAM_STANDINGS_POINTS_AGAINST = Column(Float)
    TEAM_STANDINGS_POINTS_FOR = Column(Float)
    TEAM_STANDINGS_RANK = Column(Integer)
    TEAM_STANDINGS_STREAK_TYPE = Column(String)
    TEAM_STANDINGS_STREAK_VALUE = Column(Float)
    URL = Column(String)

    def __repr__(self):
        return "<League Standings(NAME='%s', TEAM_STANDINGS_POINTS_FOR='%f)>" % (self.NAME, self.TEAM_STANDINGS_POINTS_FOR)

class scoreboard_one(Base):
    __tablename__ = 'scoreboard_one'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    T2_CLINCHED_PLAYOFFS = Column(String)
    T2_DRAFT_GRADE = Column(String)
    T2_DRAFT_RECAP_URL = Column(String)
    T2_DRAFT_POSITION = Column(String)
    T2_HAS_DRAFT_GRADE = Column(String)
    T2_IS_OWNED_BY_CURRENT_LOGIN = Column(String)
    T2_LEAGUE_SCORING_TYPE = Column(String)
    T2_TEAM_LOGOS_TEAM_LOGO_MISSING_LOGO_SIZE = Column(String)
    T2_MANAGERS_MANAGER_EMAIL = Column(String)
    T2_MANAGERS_MANAGER = Column(String)
    T2_MANAGERS_MANAGER_GUID = Column(String)
    T2_MANAGERS_MANAGER_IMAGE_URL = Column(String)
    T2_MANAGERS_MANAGER_IS_COMMISSIONER = Column(String)
    T2_MANAGERS_MANAGER_IS_CURRENT_LOGIN = Column(String)
    T2_MANAGERS_MANAGER_MANAGER_ID = Column(String)
    T2_MANAGERS_MANAGER_NICKNAME = Column(String)
    T2_NAME = Column(String)
    T2_NUMBER_OF_MOVES = Column(Integer)
    T2_NUMBER_OF_TRADES = Column(Integer)
    T2_ROSTER_ADDS_COVERAGE_TYPE = Column(String)
    T2_ROSTER_ADDS_COVERAGE_VALUE = Column(String)
    T2_ROSTER_ADDS_VALUE = Column(String)
    T2_TEAM_ID = Column(String)
    T2_TEAM_KEY = Column(String)
    T2_TEAM_LOGOS_TEAM_LOGO_SIZE = Column(String)
    T2_TEAM_LOGOS_TEAM_LOGO_URL = Column(String)
    T2_TEAM_POINTS_COVERAGE_TYPE = Column(String)
    T2_TEAM_POINTS_TOTAL = Column(Float)
    T2_TEAM_POINTS_WEEK = Column(String)
    T2_TEAM_PROJECTED_POINTS_COVERAGE_TYPE = Column(String)
    T2_TEAM_PROJECTED_POINTS_TOTAL = Column(Float)
    T2_TEAM_PROJECTED_POINTS_WEEK = Column(String)
    T2_URL = Column(String)
    T2_WAIVER_PRIORITY = Column(String)
    T2_WIN_PROBABILITY = Column(Float)
    LEAGUE_KEY = Column(String)
    MATCHUP_CONSOLATION = Column(String)
    MATCHUP_IS_PLAYOFFS = Column(String)
    MATCHUP_IS_TIED = Column(String)
    MATCHUP_KEY = Column(String)
    MATCHUP_RECAP_TITLE = Column(String)
    MATCHUP_RECAP_URL = Column(String)
    MATCHUP_WEEK = Column(String)
    MATCHUP_WEEK_END = Column(String)
    MATCHUP_WEEK_START = Column(String)
    MATCHUP_WINNER_TEAM_KEY = Column(String)
    T1_CLINCHED_PLAYOFFS = Column(String)
    T1_DRAFT_GRADE = Column(String)
    T1_DRAFT_RECAP_URL = Column(String)
    T1_DRAFT_POSITION = Column(String)
    T1_HAS_DRAFT_GRADE = Column(String)
    T1_IS_OWNED_BY_CURRENT_LOGIN = Column(String)
    T1_LEAGUE_SCORING_TYPE = Column(String)
    T1_TEAM_LOGOS_TEAM_LOGO_MISSING_LOGO_SIZE = Column(String)
    T1_MANAGERS_MANAGER_EMAIL = Column(String)
    T1_MANAGERS_MANAGER = Column(String)
    T1_MANAGERS_MANAGER_GUID = Column(String)
    T1_MANAGERS_MANAGER_IMAGE_URL = Column(String)
    T1_MANAGERS_MANAGER_IS_COMMISSIONER = Column(String)
    T1_MANAGERS_MANAGER_IS_CURRENT_LOGIN = Column(String)
    T1_MANAGERS_MANAGER_MANAGER_ID = Column(String)
    T1_MANAGERS_MANAGER_NICKNAME = Column(String)
    T1_NAME = Column(String)
    T1_NUMBER_OF_MOVES = Column(Integer)
    T1_NUMBER_OF_TRADES = Column(Integer)
    T1_ROSTER_ADDS_COVERAGE_TYPE = Column(String)
    T1_ROSTER_ADDS_COVERAGE_VALUE = Column(String)
    T1_ROSTER_ADDS_VALUE = Column(String)
    T1_TEAM_ID = Column(String)
    T1_TEAM_KEY = Column(String)
    T1_TEAM_LOGOS_TEAM_LOGO_SIZE = Column(String)
    T1_TEAM_LOGOS_TEAM_LOGO_URL = Column(String)
    T1_TEAM_POINTS_COVERAGE_TYPE = Column(String)
    T1_TEAM_POINTS_TOTAL = Column(Float)
    T1_TEAM_POINTS_WEEK = Column(String)
    T1_TEAM_PROJECTED_POINTS_COVERAGE_TYPE = Column(String)
    T1_TEAM_PROJECTED_POINTS_TOTAL = Column(Float)
    T1_TEAM_PROJECTED_POINTS_WEEK = Column(String)
    T1_URL = Column(String)
    T1_WAIVER_PRIORITY = Column(String)
    T1_WIN_PROBABILITY = Column(Float)


    def __repr__(self):
        return "<Scoreboard One(League Key='%s', Matchup Key='%s')>" % (self.league_key, self.matchup_key)

class scoreboard_two(Base):
    __tablename__ = 'scoreboard_two'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    CLINCHED_PLAYOFFS = Column(String)
    DRAFT_GRADE = Column(String)
    DRAFT_RECAP_URL = Column(String)
    DRAFT_POSITION = Column(String)
    HAS_DRAFT_GRADE = Column(String)
    IS_OWNED_BY_CURRENT_LOGIN = Column(String)
    LEAGUE_SCORING_TYPE = Column(String)
    TEAM_LOGOS_TEAM_LOGO_MISSING_LOGO_SIZE = Column(String)
    MANAGERS_MANAGER_EMAIL = Column(String)
    MANAGERS_MANAGER = Column(String)
    MANAGERS_MANAGER_GUID = Column(String)
    MANAGERS_MANAGER_IMAGE_URL = Column(String)
    MANAGERS_MANAGER_IS_COMMISSIONER = Column(String)
    MANAGERS_MANAGER_IS_CURRENT_LOGIN = Column(String)
    MANAGERS_MANAGER_MANAGER_ID = Column(String)
    MANAGERS_MANAGER_NICKNAME = Column(String)
    NAME = Column(String)
    NUMBER_OF_MOVES = Column(Integer)
    NUMBER_OF_TRADES = Column(Integer)
    ROSTER_ADDS_COVERAGE_TYPE = Column(String)
    ROSTER_ADDS_COVERAGE_VALUE = Column(String)
    ROSTER_ADDS_VALUE = Column(String)
    TEAM_ID = Column(String)
    TEAM_KEY = Column(String)
    TEAM_LOGOS_TEAM_LOGO_SIZE = Column(String)
    TEAM_LOGOS_TEAM_LOGO_URL = Column(String)
    TEAM_POINTS_COVERAGE_TYPE = Column(String)
    TEAM_POINTS_TOTAL = Column(Float)
    TEAM_POINTS_WEEK = Column(String)
    TEAM_PROJECTED_POINTS_COVERAGE_TYPE = Column(String)
    TEAM_PROJECTED_POINTS_TOTAL = Column(Float)
    TEAM_PROJECTED_POINTS_WEEK = Column(String)
    URL = Column(String)
    WAIVER_PRIORITY = Column(String)
    WIN_PROBABILITY = Column(Float)
    LEAGUE_KEY = Column(String)
    MATCHUP_CONSOLATION = Column(String)
    MATCHUP_IS_PLAYOFFS = Column(String)
    MATCHUP_IS_TIED = Column(String)
    MATCHUP_KEY = Column(String)
    MATCHUP_RECAP_TITLE = Column(String)
    MATCHUP_RECAP_URL = Column(String)
    MATCHUP_WEEK = Column(String)
    MATCHUP_WEEK_END = Column(String)
    MATCHUP_WEEK_START = Column(String)
    MATCHUP_WINNER_TEAM_KEY = Column(String)


    def __repr__(self):
        return "<Scoreboard Two(League Key='%s', Matchup Key='%s')>" % (self.league_key, self.matchup_key)

class league_transactions(Base):
    __tablename__ = 'league_transactions'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    DISPLAY_POSITION = Column(String)
    EDITORIAL_TEAM_ABBR = Column(String)
    KEY = Column(String)
    LEAGUE_KEY = Column(String)
    NAME_ASCII_FIRST = Column(String)
    NAME_ASCII_LAST = Column(String)
    NAME_FIRST = Column(String)
    NAME_FULL = Column(String)
    NAME_LAST = Column(String)
    PLAYER_ID = Column(String)
    PLAYER_KEY = Column(String)
    PLAYERS_MOVED = Column(Integer)
    POSITION_TYPE = Column(String)
    STATUS = Column(String)
    TIME = Column(String)
    TRANS_ID = Column(String)
    TRANSACTION_DATA_DESTINATION_TEAM_KEY = Column(String)
    TRANSACTION_DATA_DESTINATION_TEAM_NAME = Column(String)
    TRANSACTION_DATA_DESTINATION_TYPE = Column(String)
    TRANSACTION_DATA_SOURCE_TEAM_KEY = Column(String)
    TRANSACTION_DATA_SOURCE_TEAM_NAME = Column(String)
    TRANSACTION_DATA_SOURCE_TYPE = Column(String)
    TRANSACTION_DATA_TYPE = Column(String)
    TYPE = Column(String)


    def __repr__(self):
        return "<Transactions(TYPE='%s', NAME_FULL='%s')>" % (self.TYPE, self.NAME_FULL)

class player_weekly_stats(Base):
    __tablename__ = 'player_weekly_stats'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    BLOCK_KICK = Column(Float)
    BYE_WEEKS_WEEK = Column(String)
    COMPLETIONS = Column(Float)
    DISPLAY_POSITION = Column(String)
    DRAFT_GRADE = Column(String)
    DRAFT_RECAP_URL = Column(String)
    EDITORIAL_PLAYER_KEY = Column(String)
    EDITORIAL_TEAM_ABBR = Column(String)
    EDITORIAL_TEAM_FULL_NAME = Column(String)
    EDITORIAL_TEAM_KEY = Column(String)
    ELIGIBLE_POSITIONS_POSITION = Column(String)
    EXTRA_POINT_RETURNED = Column(Float)
    FIELD_GOALS_0_19_YARDS = Column(Float)
    FIELD_GOALS_20_29_YARDS = Column(Float)
    FIELD_GOALS_30_39_YARDS = Column(Float)
    FIELD_GOALS_40_49_YARDS = Column(Float)
    FIELD_GOALS_50_YARDS = Column(Float)
    FIELD_GOALS_MISSED_0_19_YARDS = Column(Float)
    FIELD_GOALS_MISSED_20_29_YARDS = Column(Float)
    FIELD_GOALS_MISSED_30_39_YARDS = Column(Float)
    FIELD_GOALS_MISSED_40_49_YARDS = Column(Float)
    FIELD_GOALS_MISSED_50_YARDS = Column(Float)
    FUMBLE_RECOVERY = Column(Float)
    FUMBLES_LOST = Column(Float)
    HAS_PLAYER_NOTES = Column(String)
    HAS_RECENT_PLAYER_NOTES = Column(String)
    HEADSHOT_SIZE = Column(String)
    HEADSHOT_URL = Column(String)
    IMAGE_URL = Column(String)
    INJURY_NOTE = Column(String)
    INTERCEPTION = Column(Float)
    INTERCEPTIONS = Column(Float)
    IS_EDITABLE = Column(String)
    IS_UNDROPPABLE = Column(String)
    LEAGUE_KEY = Column(String)
    LOGO_URL = Column(String)
    MANAGER_EMAIL = Column(String)
    MANAGER_GUID = Column(String)
    MANAGER_IMAGE_URL = Column(String)
    MANAGER_NICKNAME = Column(String)
    NAME = Column(String)
    NAME_ASCII_FIRST = Column(String)
    NAME_ASCII_LAST = Column(String)
    NAME_FIRST = Column(String)
    NAME_FULL = Column(String)
    NAME_LAST = Column(String)
    NUMBER_OF_MOVES = Column(Integer)
    NUMBER_OF_TRADES = Column(Integer)
    OFFENSIVE_FUMBLE_RETURN_TD = Column(Float)
    PASSING_TOUCHDOWNS = Column(Float)
    PASSING_YARDS = Column(Float)
    PLAYER_ID = Column(String)
    PLAYER_KEY = Column(String)
    PLAYER_POINTS_COVERAGE_TYPE = Column(String)
    PLAYER_POINTS_TOTAL = Column(Float)
    PLAYER_POINTS_WEEK = Column(String)
    PLAYER_STATS_COVERAGE_TYPE = Column(String)
    PLAYER_STATS_WEEK = Column(String)
    POINT_AFTER_ATTEMPT_MADE = Column(Float)
    POINT_AFTER_ATTEMPT_MISSED = Column(Float)
    POINTS_ALLOWED = Column(Float)
    POINTS_ALLOWED_0_POINTS = Column(Float)
    POINTS_ALLOWED_14_20_POINTS = Column(Float)
    POINTS_ALLOWED_1_6_POINTS = Column(Float)
    POINTS_ALLOWED_21_27_POINTS = Column(Float)
    POINTS_ALLOWED_28_34_POINTS = Column(Float)
    POINTS_ALLOWED_35_POINTS = Column(Float)
    POINTS_ALLOWED_7_13_POINTS = Column(Float)
    POSITION_TYPE = Column(String)
    RECEIVING_TOUCHDOWNS = Column(Float)
    RECEIVING_YARDS = Column(Float)
    RECEPTIONS = Column(Float)
    RETURN_TOUCHDOWNS = Column(Float)
    RETURN_YARDS = Column(Float)
    RUSHING_ATTEMPTS = Column(Float)
    RUSHING_TOUCHDOWNS = Column(Float)
    RUSHING_YARDS = Column(Float)
    SACK = Column(Float)
    SAFETY = Column(Float)
    SEASON = Column(String)
    SELECTED_POSITION_COVERAGE_TYPE = Column(String)
    SELECTED_POSITION_POSITION = Column(String)
    SELECTED_POSITION_WEEK = Column(String)
    STATUS = Column(String)
    STATUS_FULL = Column(String)
    TARGETS = Column(Float)
    TEAM_ID = Column(String)
    TEAM_KEY = Column(String)
    TOUCHDOWN = Column(Float)
    TWO_POINT_CONVERSIONS = Column(Float)
    UNIFORM_NUMBER = Column(String)
    URL = Column(String)
    WAIVER_PRIORITY = Column(String)
    WEEK = Column(String)


    def __repr__(self):
        return "<Player Weekly Stats(TYPE='%s', NAME_FULL='%s')>" % (self.TYPE, self.NAME_FULL)

class season_stats_all_players(Base):
    __tablename__ = 'season_stats_all_players'

    # Define Columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    BYE_WEEKS_WEEK = Column(String)
    COMPLETIONS = Column(Float)
    DISPLAY_POSITION = Column(String)
    DRAFT_ANALYSIS_AVERAGE_COST = Column(Float)
    DRAFT_ANALYSIS_AVERAGE_PICK = Column(Float)
    DRAFT_ANALYSIS_AVERAGE_ROUND = Column(Float)
    DRAFT_ANALYSIS_PERCENT_DRAFTED = Column(Float)
    EDITORIAL_PLAYER_KEY = Column(String)
    EDITORIAL_TEAM_ABBR = Column(String)
    EDITORIAL_TEAM_FULL_NAME = Column(String)
    EDITORIAL_TEAM_KEY = Column(String)
    ELIGIBLE_POSITIONS_POSITION = Column(String)
    FIELD_GOALS_0_19_YARDS = Column(Float)
    FIELD_GOALS_20_29_YARDS = Column(Float)
    FIELD_GOALS_30_39_YARDS = Column(Float)
    FIELD_GOALS_40_49_YARDS = Column(Float)
    FIELD_GOALS_50_YARDS = Column(Float)
    FIELD_GOALS_MISSED_0_19_YARDS = Column(Float)
    FIELD_GOALS_MISSED_20_29_YARDS = Column(Float)
    FIELD_GOALS_MISSED_30_39_YARDS = Column(Float)
    FIELD_GOALS_MISSED_40_49_YARDS = Column(Float)
    FIELD_GOALS_MISSED_50_YARDS = Column(Float)
    FUMBLES_LOST = Column(Float)
    HAS_PLAYER_NOTES = Column(String)
    HAS_RECENT_PLAYER_NOTES = Column(String)
    HEADSHOT_SIZE = Column(String)
    HEADSHOT_URL = Column(String)
    IMAGE_URL = Column(String)
    INJURY_NOTE = Column(String)
    INTERCEPTIONS = Column(Float)
    INTERCEPTION = Column(Float)
    IS_UNDROPPABLE = Column(String)
    LEAGUE_KEY = Column(String)
    NAME_ASCII_FIRST = Column(String)
    NAME_ASCII_LAST = Column(String)
    NAME_FIRST = Column(String)
    NAME_FULL = Column(String)
    NAME_LAST = Column(String)
    OFFENSIVE_FUMBLE_RETURN_TD = Column(Float)
    ON_DISABLED_LIST = Column(String)
    OWNERSHIP_OWNER_TEAM_KEY = Column(String)
    OWNERSHIP_OWNER_TEAM_NAME = Column(String)
    OWNERSHIP_OWNERSHIP_TYPE = Column(String)
    OWNERSHIP_TEAMS__COUNT = Column(String)
    OWNERSHIP_TEAMS_TEAM_CLINCHED_PLAYOFFS = Column(String)
    OWNERSHIP_TEAMS_TEAM_DRAFT_GRADE = Column(String)
    OWNERSHIP_TEAMS_TEAM_DRAFT_POSITION = Column(String)
    OWNERSHIP_TEAMS_TEAM_DRAFT_RECAP_URL = Column(String)
    OWNERSHIP_TEAMS_TEAM_HAS_DRAFT_GRADE = Column(String)
    OWNERSHIP_TEAMS_TEAM_IS_OWNED_BY_CURRENT_LOGIN = Column(String)
    OWNERSHIP_TEAMS_TEAM_LEAGUE_SCORING_TYPE = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_EMAIL = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_GUID = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_IMAGE_URL = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_IS_COMMISSIONER = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_IS_CURRENT_LOGIN = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_MANAGER_ID = Column(String)
    OWNERSHIP_TEAMS_TEAM_MANAGERS_MANAGER_NICKNAME = Column(String)
    OWNERSHIP_TEAMS_TEAM_NAME = Column(String)
    OWNERSHIP_TEAMS_TEAM_NUMBER_OF_MOVES = Column(Integer)
    OWNERSHIP_TEAMS_TEAM_NUMBER_OF_TRADES = Column(Integer)
    OWNERSHIP_TEAMS_TEAM_ROSTER_ADDS_COVERAGE_TYPE = Column(String)
    OWNERSHIP_TEAMS_TEAM_ROSTER_ADDS_COVERAGE_VALUE = Column(String)
    OWNERSHIP_TEAMS_TEAM_ROSTER_ADDS_VALUE = Column(String)
    OWNERSHIP_TEAMS_TEAM_TEAM_ID = Column(String)
    OWNERSHIP_TEAMS_TEAM_TEAM_KEY = Column(String)
    OWNERSHIP_TEAMS_TEAM_TEAM_LOGOS_TEAM_LOGO_SIZE = Column(String)
    OWNERSHIP_TEAMS_TEAM_TEAM_LOGOS_TEAM_LOGO_URL = Column(String)
    OWNERSHIP_TEAMS_TEAM_URL = Column(String)
    OWNERSHIP_TEAMS_TEAM_WAIVER_PRIORITY = Column(String)
    OWNERSHIP_WAIVER_DATE = Column(String)
    PASSING_TOUCHDOWNS = Column(Float)
    PASSING_YARDS = Column(Float)
    PERCENT_OWNED_COVERAGE_TYPE = Column(String)
    PERCENT_OWNED_DELTA = Column(Integer)
    PERCENT_OWNED_VALUE = Column(Integer)
    PERCENT_OWNED_WEEK = Column(Integer)
    PLAYER_ID = Column(String)
    PLAYER_KEY = Column(String)
    PLAYER_POINTS_COVERAGE_TYPE = Column(String)
    PLAYER_POINTS_SEASON = Column(String)
    PLAYER_POINTS_TOTAL = Column(Float)
    PLAYER_STATS_COVERAGE_TYPE = Column(String)
    PLAYER_STATS_SEASON = Column(String)
    POINT_AFTER_ATTEMPT_MADE = Column(String)
    POINTS_ALLOWED = Column(Float)
    POINTS_ALLOWED_0_POINTS = Column(Float)
    POINTS_ALLOWED_14_20_POINTS = Column(Float)
    POINTS_ALLOWED_1_6_POINTS = Column(Float)
    POINTS_ALLOWED_21_27_POINTS = Column(Float)
    POINTS_ALLOWED_28_34_POINTS = Column(Float)
    POINTS_ALLOWED_35_POINTS = Column(Float)
    POINTS_ALLOWED_7_13_POINTS = Column(Float)
    POSITION_TYPE = Column(String)
    RECEIVING_TOUCHDOWNS = Column(Float)
    RECEIVING_YARDS = Column(Float)
    RECEPTIONS = Column(Float)
    RETURN_TOUCHDOWNS = Column(Float)
    RETURN_YARDS = Column(Float)
    RUSHING_ATTEMPTS = Column(Float)
    RUSHING_TOUCHDOWNS = Column(Float)
    RUSHING_YARDS = Column(Float)
    STATUS = Column(String)
    SACK = Column(Float)
    TARGETS = Column(Float)
    TWO_POINT_CONVERSIONS = Column(Float)
    UNIFORM_NUMBER = Column(String)


    def __repr__(self):
        return "<All Players - Season Stats (TYPE='%s', NAME_FULL='%s')>" % (self.TYPE, self.NAME_FULL)

if __name__ == '__main__':
    handler = Auth()

    # Testing
    data = [['10','andy',31],['9','tyler',31],['8','jaon',49]]
    data_dict = [{'Primary_Key':'100',
                  'name':'andy',
                  'age':100},
                 {'Primary_Key': '110',
                  'name': 'millz',
                  'age': 100},
                 {'Primary_Key': '120',
                  'name': 'millyz',
                  'age': 100}]
    d = db_Storage(handler,'aws_master','test')
    d.create_table(data_dict)


"""
http://alextechrants.blogspot.com/2013/11/10-common-stumbling-blocks-for.html
"""

