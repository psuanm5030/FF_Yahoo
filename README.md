# Yahoo Fantasy Football - API Wrapper and Data Munging

I have always been fascinated with Fantasy Football and years ago wanted to get my data from Yahoo.  Luckily, Yahoo is one of the Fantasy providers that still has an API (although not well maintained - if at all).  Regardless, there is a plethora of information available, and for someone like me, with over a decade of playing fantasy football on Yahoo, I wanted to gather some insights to share with my buddies and for my personal gain :-).

This repository is essentially a wrapper for the Yahoo API to pull data like league details, standings, draft and scoreboard.  Further, I have spent alot of time munging the data to allow it to be visualized.

## Pre-Requisites
* YDN Free Account.  You'll need to create an 'app' with yahoo.  [Go Here](https://developer.yahoo.com/apps/) and register and "Create an App".  Select "Installed Application" and give the App a name (feel free to fill out the other information). Select "Fantasy Sports" and "Read".  Hit "Create App".  Once approved (if there is a waiting period) you can click into this app and gather the "Client ID (Consumer Key)" and "Client Secret (Consumer Secret)".
* AWS RDS PostgreSQL Instance (Free Tier account).  You can use a PostgresSQL instance ANYWHERE, although I recommend using an RDS instance on AWS because its easy!  You'll need the following values from your instance: DB Name, username, password, and endpoint.

## Setting up the enviornment
1. Ensure that you have virtualenv installed (if not, `run pip install virtualenv`)
2. Clone this project to your desktop.
3. In the terminal, navigate to this directory and create a virtual environment via `virtualenv my_project` (change myproject to whatever you want).  This is where python and the requisite packages will be installed.  Activate the virtualenv using `source my_project/bin/activate`.  Note: If you are using an IDE, make sure to change your python interpreter to the new env you created.
4. NOTE: Make sure your virtualenv created above is active (if your activated you will see "(my_project)" prefixed in your terminal). Run `pip install -r requirements.txt` to install the packages necessary.  This will install the packages into your virtualenv within your project.
5. Update the "credentials_master_example.yml" file with the values you saved from the pre-requisite steps.  Delete the "_example" from the file name.

## Usage
1. To Be Completed - Not Ready for Primetime!

## Tables
* 'league_details' - TBC
* 'league_all_stat_values' - TBC
* 'league_only_stat_values' - TBC
* 'league_standings' - TBC
* 'team_details' - TBC
* 'draft_with_players' - TBC
* 'player_weekly_stats' - TBC
* 'season_stats_all_players' - TBC

### Notes / Disclaimers
1. This continues to be a work-in-process.  A lot of my time has been spent figuring out the API and refactoring my code.  I still have a long way to go.  Please keep that in mind.
2. My goal is to extend this to all plug-in-play to some sort of visualization (maybe Tableau).
3. There are still a lot of issues with the data, some that might be hard to get over or that require less than ideal solutions. See the next section.
4. This can generate a lot of calls.  They allow 20K per hour.  **Working on setting up limits

### Known Issues
* Team Names - Users have one team_key each year for each league.  Early years lack a linkage from league to league or year to year.  There is now a GUID field, but it is still unreliable.  I wam working on finding a solution to this matter.
* New Fields - In recent years, Yahoo has been adding data (e.g., projected points in 200X, matchup recaps in 20XX, etc.).  This makes data inconsistent and missing for some years.  It also makes the munging job more difficult!
* Multiple Owners for a Single Team - This is not handled well right now.  Simply capturing the first team owner.
* Naming conventions - Still working on harmonizing the field names and optimizing the database.

### Thanks
1. Thanks to [Andrew Martin's Repository](https://github.com/almartin82/yahoo_roster_extract) which got me started with even making this possible.  His original implementation, while very script-focused, got me where I am today. I spent lots of time reviewing how he authenticated and parsed the data.  Wouldn't be here without him!
2. Thanks to SO for all the learning!
3. Thanks to [PyCharm](https://www.jetbrains.com/pycharm/) for being an awesome tool and making this easier to do!
3. Thanks to [Yahoo! Fantasy API Node Module](http://yfantasysandbox.herokuapp.com/) for putting together something that I admire.  I didn't use this enough, but its a great resource.  If you are looking to extend this, take a look!