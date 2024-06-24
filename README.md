The source files at hand are, note that this is our 🥉bronze layer🥉 :
runs.csv : this file contains all of the runs collected in the app.
The columns are :
- run_id : integer - unique identifier of the run
- duration : float - duration of the run in seconds
- distance : float - distance in meters
- date : string - date of the run (in different format depending on the user preference)
- location : string - latitude and longitude of the runner’s location when the run started
- temperature : string - temperature in degree Celsius or Fahrenheit
- user_id : integer - unique identifier of the runner
users.csv : this file contains all of the users that created an account.
The columns are :
- user_id : integer - unique identifier of the runner
In our 🥈silver layer🥈, we will :
 modify the runs.date column to have consistent formatting
convert the runs.temperature column so that all of the rows are expressed in degree Celsius
filter out the runs that lasted less than 5 minutes
filter out runners that never ran a marathon
We will then aggregate the data to obtain the following tables in our 🥇gold layer🥇 :
runner_performances : a table that contains for each runner their last performance on the following distances : 5K, 10K, Half-marathon, Marathon
runner_activity :  a table that contains for each runner the number of kilometers they ran per week during the last month.
