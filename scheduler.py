import schedule
import time
import sys
import subprocess
from datetime import datetime
import zoneinfo

#Ensures scheduler is ran from the virtual environment to prevent import errors
PYTHON = sys.executable 

#Check if current time is within US stock market hours (9:30am-4pm ET, Mon-Fri)
def is_market_open():
    now = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    if now.weekday() > 4:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close

#Run the strategy script, which will fetch new data, update the database, and execute the backtest and paper trading logic
def run_strategy():
    if not is_market_open():
        print("Market closed — skipping")
        return
    print(f"Running strategy at {datetime.now()}")
    subprocess.run([PYTHON, "src/etl.py", "--auto"])
    subprocess.run([PYTHON, "src/main.py"])

schedule.every(1).hours.do(run_strategy)

#Keep the script running to allow scheduled tasks to execute
print("Scheduler running. Ctrl+C to stop.")
while True:
    schedule.run_pending()
    time.sleep(60)