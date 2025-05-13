import time
import requests
from integration_config import CLICKUP_HEADERS, RESOURCES_LIST_ID
from help_functions import datetime_str_to_unix


class ResourceClosing:
    def __init__(self, start_date, end_date, close, archieve):
        self.start_date = start_date
        self.end_date = end_date
        self.close = close
        self.archieve = archieve
        self.filter_start = str(datetime_str_to_unix(start_date, 2, 0)) if start_date else None
        self.filter_end = str(datetime_str_to_unix(end_date, 22, 0)) if end_date else None

    def get_resources(self):
        all_resources = []
        page = 0
        while True:
            page_response = requests.get(
                f"https://api.clickup.com/api/v2/list/{RESOURCES_LIST_ID}/task?page={page}&include_closed=true&due_date_gt={self.filter_start}&due_date_lt={self.filter_end}",
                headers=CLICKUP_HEADERS
            )
            if not page_response.json()['tasks']:
                break
            all_resources.extend(page_response.json()['tasks'])
            page += 1
        print(f"Total Resources: {len(all_resources)}")
        return all_resources

    def close_task(self, task_id, status):
        task_data = {
            "status": status,
        }
        if self.archieve:
            task_data['archived'] = True
        close_task = requests.put(f"https://api.clickup.com/api/v2/task/{task_id}", headers=CLICKUP_HEADERS, json=task_data)
        print(f"Closing Task {task_id}. Status Code: {close_task.status_code}")
        time.sleep(1)
        return close_task.status_code

    def launch(self):
        resources = self.get_resources()
        counter = 0
        for resource in resources:
            # input(resource['name'])
            counter += 1
            print("---------------------------------")
            resource_id = resource['id']
            print(f"{counter}/{len(resources)}. Resource ID: {resource_id}")
            resource_cfs = resource['custom_fields']
            linked_blocking_ids = []
            for resource_cf in resource_cfs:
                if resource_cf['id'] == "e3b6c6ea-c8a4-4318-90f8-168f6b54307e":
                    linked_blockings = resource_cf['value']
                    for linked_blocking in linked_blockings:
                        linked_blocking_ids.append(linked_blocking['id'])
            self.close_task(resource_id, 'finished')
            for linked_blocking_id in linked_blocking_ids:
                self.close_task(linked_blocking_id, 'complete')


def main(month_start_date, month_end_date, close, archieve):
    resource_blocking_handler = ResourceClosing(month_start_date, month_end_date, close, archieve)
    resource_blocking_handler.launch()


date_ranges = [
    {'start': '2025-03-01', 'end': '2025-03-31', 'close': True, 'archieve': True},
    {'start': '2025-04-01', 'end': '2025-04-30', 'close': True, 'archieve': False},
]
for date_range in date_ranges:
    main(date_range['start'], date_range['end'], date_range['close'], date_range['archieve'])
    time.sleep(5)

