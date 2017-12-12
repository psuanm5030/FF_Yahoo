import Auth_Handler
from API_Data import Yahoo_API
import timeit

if __name__ == '__main__':
    # Setup
    h = Auth_Handler.Auth()
    api = Yahoo_API(h)
    d = Auth_Handler.db_Storage(h, 'aws_master')
    d.delete_all_tables()

    """
    Problems: 
    - When running through past years - there are issues with missing data.  MAYBE use a defaultdict?
    
    Validated for all years: 
    - 'league_details'
    - 'league_all_stat_values'
    - 'league_only_stat_values'
    - 'league_standings'
    - 'team_details'
    """

    for lg in api.league_keys_list:
        print 'LEAGUE: {}'.format(lg)

        # DB: Store League / Stat Details
        # api.get_league_settings_stats(lg)
        # d.create_table(api.db_leagues, table_name='league_details')
        # d.create_table(api.db_league_all_stats, table_name='league_all_stat_values')
        # d.create_table(api.db_league_only_stats, table_name='league_only_stat_values')

        # DB: League Standings
        # api.get_league_standings(lg)
        # d.create_table(api.db_league_standings, table_name='league_standings')

        # DB: Store Team Details
        # api.get_teams_detail(lg)
        # d.create_table(api.db_team_details, table_name='team_details')
        # todo guid missing for many

        # SCOREBOARD
        # api.get_scoreboard(lg)
        # d.create_table(api.db_scoreboard_one, table_name='scoreboard_one')
        # d.create_table(api.db_scoreboard_two, table_name='scoreboard_two')

        # DB: League Transactions
        # api.get_league_transactions(lg)
        # d.create_table(api.db_league_transactions, table_name='league_transactions')

        # **************** UNTESTED ****************
        # DB: Draft with Player Stats
        api.get_draft_by_leaguekey(lg)
        d.create_table(api.db_draft_with_players, table_name='draft_with_players')

        # DB: Weekly Player Stats
        # api.get_roster_stats_week(lg)
        # d.create_table(api.db_player_weekly_stats, table_name='player_weekly_stats')