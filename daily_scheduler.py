import time
from datetime import datetime
from integration_config import date_ranges
from resource_blocking import resource_blocker
from resource_calculation import resource_calculator
from available_resources import available_resources_collector


def scheduler():
    start_time = time.time()
    current_date = datetime.today().strftime('%Y-%m-%d')
    for date_range in date_ranges:
        print("==========" * 15)
        print(f"CURRENT CONFIG: {date_range}\n")
        print(f"Starting Resource Blocker")
        resource_blocker(date_range['start'], date_range['end'], None)
        time.sleep(5)
        print(f"Starting Resource Calculator")
        resource_calculator(date_range['start'], date_range['end'])
        time.sleep(5)

    print(f"Starting Available Resources Collector")
    available_resources_collector(current_date)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.5f} seconds")


scheduler()
