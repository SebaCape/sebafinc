import schedule
import time
import subprocess
from datetime import datetime

#Check if current time is within US stock market hours (9:30am-4pm ET, Mon-Fri)
def is_market_open():
    now = datetime.now()
    # Monday=0, Friday=4
    if now.weekday() > 4:
        return False
    market_open  = now.replace(hour=9, minute=30, second=0)
    market_close = now.replace(hour=16, minute=0, second=0)
    return market_open <= now <= market_close

#Run the strategy script, which will fetch new data, update the database, and execute the backtest and paper trading logic
def run_strategy():
    if not is_market_open():
        print("Market closed — skipping")
        return
    print(f"Running strategy at {datetime.now()}")
    subprocess.run(["python", "src/etl.py", "--auto"])
    subprocess.run(["python", "src/main.py"])

schedule.every(1).hours.do(run_strategy)

#Keep the script running to allow scheduled tasks to execute
print("Scheduler running. Ctrl+C to stop.")
while True:
    schedule.run_pending()
    time.sleep(60)