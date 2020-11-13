# Data Engineering Quiz

## Overall Goal

Welcome!  This is the Penn Engineering code challenge.

We hope you find this challenge interesting. In order to help define what we are looking for and to be respectful of your time, we have created a full shell application with a few holes. This is a scenario we deal with day to day but is not an exact representation of what we are working with. You should limit this exercise to no more than an hour or two.  The goal is to test your familiarity with building pipelines and ETL processes.

### Expectations

The goal of this exercise is to develop a simple ETL process for pulling game stats from the NHL website and then shaping the results in a database.  There are a few prerequisites that match our environment here (_Mac OS, docker, make_), you should be able to follow the [Makefile] if your environment doesn't quite match. 

We are looking for you to imagine this a real, running production job.  Assume this job will be run on some schedule continuously in prod. We're looking for:

* **Mostly Working Code** -- given the run steps, the code should download data, format it, load it into the db and the final data marts be queryable.  But we really want you to focus on productionizing this code.  What things could you add, what data could be produced (and where) that would make this job run in a very friendly way?  Specifically "running code" means:
  * `make step1`  -- runs docker images in the foreground for convenience
  * `make step2 points_leaders` -- loads and shapes data then queries the results.  This should just print out the top 10 points leaders.
* **Data Quality** -- An important part of data engineering is providing trustable, high quality data.  Doing that starts with tested code and then moves to tested data in step 2.
* **Production Readyness** -- As we're pretending this is an automated job, we want to see how and what you add to help you verify or troubleshoot a production job.  Basically, within reason, what's necessary to know this job is OK or to quickly diagnose during a failure. 
* **Reliablility** -- Error handling and smart retrying. Notifications of some type on run status.

Now this isn't to say this is 100% production code, there's _a lot_ of bad practices here around password management, data persistance, etc. Feel free to fix things that annoy you but it's not required, we're specifically looking around where the code has comments asking for you to fill in the details.

#### Prerequisites
We use Mac OS for development so if you have one and do any kind of development on it, you're probably good.  Linux environments should work too, this was tested on an Ubuntu machine.  Windows users will need to establish a complete workflow. (Sorry)
* Docker -- native for linux, [Desktop](https://www.docker.com/products/docker-desktop) for Mac and windows users
* `docker-compose` -- comes with desktop. Linux users may need an extra install. 
* a functioning `make` -- if you don't/can't use this, the make file is just wrapping the steps necessary to run the code and should be easy to disect to run manually
* Python 3.7+ with a virtualenv -- This example won't create the virtualenv for you, but will help you install dependencies

#### Solution Submission
Please don't fork this repo.  Clone it, download the zip. etc. And upload to your own repo. 

Once you're done and commit your results to a VCS such as Github, Bitbucket, or Gitlab.

Provide a link to your repo so that we can review the code.  If running the code requires any other steps than those listed below,instructions for running the code should be provided as well in a `README` file.



### Part One
A common task that we encounter in data engineering is batch fetching data.  For part one we will create a job that pulls down some game stats from the NHL API. Don't worry, you won't need to know anything about hockey for this work.  This is true in general of working at PI, while an interest in sports certainly helps, it's not necessary to get the job done.  You probably don't even need to look at the API specs either but they are linked in the extras below if you are curious or get stuck on something weird.

For this we are simulating writing to S3 with a tool called `minio`. It's software that is designed to self host a distributed object store with an API that matches S3.  This means that just by using the `endpoint_url` configuration in boto and on the command line, you can issue comands against that instead of the real S3 with no other code changes.  So no need to have an AWS account for this.

Assuming you have cloned this repository to a directory called `data-eng-challenge` and have created and activated a python 3.7+ virtualenv, you can run `make init` to install the libraries used for development.

#### nhldata/app
If you look into the code at [nhldata/app.py], in the `main()` function you will see the simple steps of operation.  Fetch the schedule for a given date, get the player stats for those games, write them to S3 (simulated using a Min.io docker container when running `step1`).

The idea is that this job will sweep by every day and pick up the previous games stats so that we have a collection of information on every game played. 

Things to think about:
* Is this logging good? Is any of this code gnarly enough that some inline/codedoc should be provided?
* Jobs fail to run for whatever reason, what can we use to know that? Is there anything that can be done that, if a downstream user knew where to look, could also be aware of the issue i.e. can you expose anything into the database from this process?
* Even if a job runs, networking hiccups or server issues could break things.  How do we avoid 2am phone calls?
* When a failure occurs how do we backfill that data?
* If this runs over a long enough time, did the NHL change their API?
* How should we organize this data for use?

Feel free to add any `pytest` code you want to `tests/` to figure stuff out.  That's good to see! (_To be fair to you the whole solution was coded so that there were no gotchas and then code was removed. That process took so much time that unit tests were unfortunately left out_)

When you are done, running `make step1` should successfully bring up a database (localhost:5432), a minio server ([http://localhost:9000]), run your job and you will see the resultant CSV files in [s3_data/data-bucket/].

Leave this running and use a new terminal for part two as this the running postgres instance you'll be working with.

### Part Two
A common pattern we use is downloading very general or wide format data into cheap longterm store and then carve out and clean up the interesting data.  In part one, you wrote a job that fetched game stats from an API, those stats are a bit messy even after they were carved down to just per game,  per player stats.  The CSV files that are created don't need to be modified as they match the table definition.  The data loading here is a `COPY` command in postgres to load those CSV files and the column order has to match.   

For this exercise we want to pretend this is some cloud machinery getting raw data into a database for us.  If the `run_sql` step fails, please reach out to us, this is not your fault and we don't want you to spend time fixing it.

So once raw data lands in a sql engine, its ready for some analysis and carving and cleaning to be used in data marts for reporting or whatever.  In this part you will use a tool called [dbt](https://docs.getdbt.com/) to clean the data up, carving it into the required shape and running some data quality tests.

You are *not* expected to know how to use dbt.  The makefile pulls down a docker image to run the commands and is preconfigured for our exercise here.  This part is really about testing your SQL skills and how you think about data quality. 

#### dbt/
The [dbt/] directory contains all the configuration for dbt.  The only part you need to focus on is the [models/] directory.  This contains the source of the database tables.  In DBT everything that is a `.sql` file is expected to be a select and will be rendered as a view or table in your targeted database.   Here are a few concepts you need to know:
* data `source`s -- these are tables created by outside systems, providing the source data for DBT to work with.  This has been defined for you matching the CSV file you dumped in [models/base/nhl/sources.yml]
* "Base" models -- These are the first layer, abstracting away the source and providing a clean _base_ to work on.  This is where column names are cleaned up, types are cast correctly, any necessary metadata is added for all data models build up from this.  It is the only place `source()`s are refereneced.
* "Mart" models -- These are the refined, question answering, fast reporting data tables build from base models. These use the `ref()` function to refer to base or other tables.

Tables are documented in `*.yml` files.  This is also where data quality tests for the schema are documented.  If you look at any of these files you will see something like
```
- name: column_name
  description: what this column is
  tests:
    - type_of_test
```
When `dbt test` or in this case `make dbt_test` is executed, dbt will generate tests for those columns based on that configuration and verify that no rows exist that fail the test.

The first exercise is in [base/nhl/nhl_players.sql].  Given all the game stat data we want to create a base table of players and their running point tallies.  A basic select is there for you as a reference.  Note the jinja syntax `{{ ref('player_game_stats') }}` this is how you reference other tables by their file name.  Also notice directory structure doesn't matter here, the filename is enough to find the table. 

Look into [base/nhl/nhl_players.yml] to find all the columns we wish you to calculate and the specifications on that data as tests we need to have pass.  If it's not clear, points are just assists + goals. 

Now that you have built a base table, use it to generate a mart table of points leaders per team. Take a look at [mart/nhl/points_leaders.yml] and see the three columns and their expectations (tests).  Here you will need to flex some sql knowledge to just select the top point scorer (or all the tied point leaders) per team.  And for this table we're only interested in teams where they have a player with at least 1 point.

When you are developing or finished you can run 
* `make dbt_run` to build/rebuild your tables and views when you change the sql
* `make dbt_test` to run the schema tests defined in the yml files.  You should need to change the yml this part should just pass when your SQL is correct.

The final bit of polish is `make points_leader` which is simulating a user running a report on that data and will chose the top 10 points leaders`

## Finally
As stated above, the steps we are looking to run, which simulate a running job, some data load mechanism and final shapping for use are:
1. `make step1`  -- leaving the terminal open
1. `make step2 points_leader`  

If you add a step or otherwise need us to run something outside of those steps please document in the submission.   There will be a follow up call to discuss your submission so a detailed write up isn't necessary, but if you feel the need to explain anything else, please do.

## Tips
* You don't need to get the whole season, if you do you'll change the shape of the data stored so you'll need to update the ingest step to match. TGhere's two commented out dates that have data already, focus your testing/work on those dates and you'll be OK.
* Pandas has a function called `json_normalize` which will flatten out nested json into dotted notation. You can then replace `.` with `_` and should be able to match the column naming convention.
ex:
```python
df = pd.json_normalize(#dict here)
df = df.rename(columns=lambda col: col.replace('.', '_'))
```
* Think about what happens when `make step2` is run twice, potential fixes for that can be in the `dbt` part of the code, doesn't have to be a purely pythonic fix.

## Extras
**NHL API Spec**
* [https://raw.githubusercontent.com/erunion/sport-api-specifications/master/nhl/nhl.yaml]
* [Head to the swagger editor](https://editor.swagger.io/) and from the menu select `File -> Import URL` and paste in the aabove swagger address.


**Docker**
* [https://www.docker.com/products/docker-desktop] mac, windows
* [https://docs.docker.com/engine/install/] linux (under "server")


**DBT**
* [https://docs.getdbt.com]

**Minio**
Self hosted, distributed object store with an S3 api
* [https://min.io/]
* [Using with boto3](https://docs.min.io/docs/how-to-use-aws-sdk-for-python-with-minio-server.html)
* [Using with the awscli](https://docs.min.io/docs/aws-cli-with-minio.html)


