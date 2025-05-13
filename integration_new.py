import time
import requests
from zoho_api.api import api_request
from integration_config import CLICKUP_HEADERS, SKIP_USERS
import datetime
from help_functions import batch_generator, format_hours, datetime_str_to_unix
from bq import get_data_from_bq
import re
import json

OPERATIONS_TEAM = ["artem.iakovenko@kitrum.com", "olha.dzhychko@kitrum.com", "katherine.hlotova@kitrum.com", "vladyslav.havrilov@kitrum.com", "roman.pomin@kitrum.com"]
#   DATE(TIMESTAMP_MILLIS(CAST(start AS INT) + 3600000)) AS log_date
START_DATE = '2025-04-01'
END_DATE = '2025-04-30'
TIMELOGS_QUERY = f""" 
SELECT 
  time_logs.id AS timelog_id,
  JSON_EXTRACT_SCALAR(user, '$.email') AS email,
  JSON_EXTRACT_SCALAR(task, '$.id') AS task_id,
  JSON_EXTRACT_SCALAR(task, '$.name') AS task_name,
  COALESCE(lists.name, folderless_lists.name) AS list_name,
  COALESCE(tasks.custom_item_id, folderless_tasks.custom_item_id) AS task_type,
  COALESCE(tasks.custom_fields, folderless_tasks.custom_fields) AS task_custom_fields,
  time_logs.billable AS is_billable,
  JSON_EXTRACT_SCALAR(task_location, '$.folder_id') AS folder_id,
  folders.name AS folder_name,
  JSON_EXTRACT_SCALAR(task_location, '$.list_id') AS list_id,
  COALESCE(lists.name, folderless_lists.name) AS list_name,
  JSON_EXTRACT_SCALAR(task_location, '$.space_id') AS space_id,
  spaces.name AS space_name,
  DATE(TIMESTAMP_MILLIS(CAST(start AS INT))) AS log_date,
  time_logs.start AS start_timestamp,
  `end` AS end_timestamp,
  time_logs.duration AS duration_ms,
  CAST(time_logs.duration AS FLOAT64) / 60000  AS duration_m,
  CAST(time_logs.duration AS FLOAT64) / 3600000 AS duration_h,
  time_logs.description
FROM `kitrum-cloud.clickup_kitrum.Time_Tracking` time_logs
LEFT JOIN `kitrum-cloud.clickup_kitrum.Spaces` spaces 
  ON spaces.id = JSON_EXTRACT_SCALAR(task_location, '$.space_id')
LEFT JOIN `kitrum-cloud.clickup_kitrum.Folders` folders 
  ON folders.id = JSON_EXTRACT_SCALAR(task_location, '$.folder_id')
LEFT JOIN `kitrum-cloud.clickup_kitrum.Lists` lists 
  ON lists.id = JSON_EXTRACT_SCALAR(task_location, '$.list_id')
LEFT JOIN `kitrum-cloud.clickup_kitrum.Folderless_Lists` folderless_lists 
  ON folderless_lists.id = JSON_EXTRACT_SCALAR(task_location, '$.list_id')
LEFT JOIN `kitrum-cloud.clickup_kitrum.Tasks` tasks 
  ON tasks.id =   JSON_EXTRACT_SCALAR(task, '$.id')
LEFT JOIN `kitrum-cloud.clickup_kitrum.Foldeless_Tasks` folderless_tasks 
  ON folderless_tasks.id =   JSON_EXTRACT_SCALAR(task, '$.id')
where DATE(TIMESTAMP_MILLIS(CAST(start AS INT))) between '{START_DATE}' AND '{END_DATE}'
"""


def prettify_task_name(task_name):
    prettified_name = re.sub(r'[^a-zA-Z0-9 \-]', '', task_name)
    prettified_name = re.sub(r'\s+', ' ', prettified_name).strip()
    if len(prettified_name) > 90:
        return f"{prettified_name[0:85]}..."
    else:
        return prettified_name


def unix_to_date(ts_value):
    try:
        unix_timestamp_sec = (int(ts_value) + 3600000) / 1000
        # unix_timestamp_sec = int(ts_value) / 1000
        date = datetime.datetime.fromtimestamp(unix_timestamp_sec)
        return date.strftime('%Y-%m-%d')
    except:
        return None

def batch_generator(data, batch_size=100):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def get_zp_employees():
    s_index = 1
    result = []
    while True:
        try:
            page_employees = api_request(
                "https://people.zoho.com/people/api/forms/employee/getRecords?sIndex=" + str(
                    s_index) + "&searchParams={searchField: 'Employeestatus', searchOperator: 'Is', searchText : 'Active'}",
                "zoho_people",
                "get",
                None
            )['response']['result']
        except KeyError:
            break
        for page_employee in page_employees:
            zp_employee_id = list(page_employee.keys())[0]
            zp_employee_data = page_employee[zp_employee_id][0]
            result.append({"email": zp_employee_data['EmailID'], 'id': zp_employee_data['Zoho_ID'], 'name': f'{zp_employee_data["FirstName"]} {zp_employee_data["LastName"]}', 'short_id': zp_employee_data['EmployeeID']})
        s_index += 200
    return result


def clickup_get_task(task_id):
    response = requests.get(f"https://api.clickup.com/api/v2/task/{task_id}?include_subtasks=true", headers=CLICKUP_HEADERS)
    return response.json()



blockings_mapping = {
    "869775k91": {"name": "Kitrum Internal Activities", "list_id": ""}
}
internal_activities_task = clickup_get_task(list(blockings_mapping.keys())[0])
internal_activities_subtasks = internal_activities_task["subtasks"]
for internal_activities_subtask in internal_activities_subtasks:
    internal_activity_task = clickup_get_task(internal_activities_subtask['id'])
    time.sleep(1)
    related_list_id = ""
    for custom_field in internal_activity_task['custom_fields']:
        if custom_field['id'] == 'cb56dae4-b0bc-43fd-b9a8-9c6d3e3e4e51':
            related_list_id = custom_field['value'] if 'value' in custom_field else ""
    blockings_mapping[internal_activities_subtask['id']] = {"name": internal_activities_subtask['name'], "list_id": related_list_id}


class LogsDuplicator:
    def __init__(self, users):
        self.users = users
        self.zp_employees = get_zp_employees()
        self.zp_projects = []
        self.timelogs_by_user = {}
        self.project_splitted_logs_by_user = {}
        self.project_types = {}
        self.all_clickup_users = []

    def get_month_logs(self):
        response = get_data_from_bq(TIMELOGS_QUERY)
        for timelog in response:
            if timelog['email'] not in self.timelogs_by_user:
                self.timelogs_by_user[timelog['email']] = [timelog]
            else:
                self.timelogs_by_user[timelog['email']].append(timelog)
        print(self.timelogs_by_user[timelog['email']])

    def get_zp_projects(self):
        s_index = 1
        while True:
            try:
                response = api_request(
                    f"https://people.zoho.com/people/api/forms/P_TimesheetJobsList/getRecords?sIndex={s_index}&limit=200",
                    "zoho_people",
                    "get",
                    None
                )['response']['result']
            except KeyError:
                response = []
            if not response:
                break
            self.zp_projects.extend(response)
            s_index += 200

    def split_logs_by_project(self):
        for user_email, user_logs in self.timelogs_by_user.items():
            if self.users and user_email not in self.users:
                continue

            print("==============" * 10)
            print(f"User: {user_email}")
            if user_email in SKIP_USERS:
                print(f"Skipping user: {user_email}")
                continue
            print(f"\nTimelogs Available: {len(user_logs)}")
            user_logs_by_project = {}
            for user_log in user_logs:
                task_id = user_log['task_id']
                # if not task_id:
                #     continue
                # if task_id in ['8698jayu2', '8698ruw35']:
                #     continue
                task_type = user_log['task_type']
                space_id = user_log['space_id']
                folder_id = user_log['folder_id']
                list_id = user_log['list_id']
                # if list_id != "901204073035":
                #     continue
                if list_id in ["901204775879"]:
                    # leaves
                    continue
                zp_project_id = None
                if space_id == "90121864869":
                    # KITRUM PROJECT SPACE - COMMERCIAL PROJECTS
                    for zp_project in self.zp_projects:
                        zp_project_details = list(zp_project.values())[0][0]
                        if zp_project_details['Clickup_ID'] == list_id:
                            zp_project_id = list(zp_project.keys())[0]
                            break
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "commercial-project"
                elif space_id == '90123406123':
                    # nexumous
                    zp_project_id = "378942000029403117"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "commercial-project"
                elif list_id in ['901205193720'] or task_id in ['8697pfw5v', '8698fgf4z']:
                    # CTO SPACE
                    zp_project_id = "378942000026337117"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"
                elif task_id in ["8696w2rgd"]:
                    # PRESALE VLADYSLAV SYDORENKO
                    zp_project_id = "378942000023897482"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-presale"
                elif list_id in ['901209566746']:
                    # QA TASKS
                    zp_project_id = "378942000031293378"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"
                elif list_id in ["901207881892"]:
                    # INTERNAL PM TASKS
                    zp_project_id = "378942000028727416"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"
                elif list_id in ["901206162641", "901207881892"]:
                    # INTERNAL BA TASKS
                    zp_project_id = "378942000007241305"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"
                elif list_id in ["901202188927", "901202189134"]:
                    # JAVA UNIT - REMOVE LAST ADMIN SPACE
                    zp_project_id = "378942000024070314"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"
                elif list_id == "901201952497":
                    # PRESALE LIST
                    for zp_project in self.zp_projects:
                        zp_project_details = list(zp_project.values())[0][0]
                        if zp_project_details['Project_Name'].strip() == user_log['task_name'].replace(" (Presale)", "").replace("Help with Outstaff Presale - ", "").strip():
                            zp_project_id = list(zp_project.keys())[0]
                            break
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "presale"
                elif list_id == "901205350291" and task_type in [1013]:
                    # TECH INTERVIEW - INTERNAL ACTIVITIES LIST
                    zp_project_id = "378942000015322402"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-activities"
                elif list_id in ['901200697017']:
                    # MARKETING ACTIVITIES
                    zp_project_id = "378942000013754065"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"

                elif list_id in ["901205350291"] and task_type in [0]:
                    # USUSAL TASKS - INTERNAL ACTIVITIES LIST AND CTO SPACE
                    zp_project_id = "378942000015322402"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"
                elif list_id == "901205350291" and task_type in [1014]:
                    # IDLE TIME - INTERNAL ACTIVITIES LIST
                    zp_project_id = "378942000004253148"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "bench"

                elif user_email in OPERATIONS_TEAM:
                    zp_project_id = "378942000009246025"
                    if zp_project_id not in self.project_types:
                        self.project_types[zp_project_id] = "internal-team-project"


                if not zp_project_id:
                    print(user_log)
                    zp_project_id = input(f"Specify Zoho People ID for for the log above: ")

                if zp_project_id not in user_logs_by_project:
                    user_logs_by_project[zp_project_id] = [user_log]
                else:
                    user_logs_by_project[zp_project_id].append(user_log)
            self.project_splitted_logs_by_user[user_email] = user_logs_by_project

    def get_zp_logs(self, user_email):
        s_index = 1
        user_zp_logs = []
        while True:
            try:
                page_logs = api_request(
                    f"https://people.zoho.com/people/api/timetracker/gettimelogs?fromDate={START_DATE}&toDate={END_DATE}&billingStatus=all&user={user_email}&sIndex={s_index}&limit=200",
                    "zoho_people",
                    "get",
                    None
                )['response']['result']
            except KeyError:
                page_logs = []
            if not page_logs:
                break
            user_zp_logs.extend(page_logs)
            s_index += 200
        return user_zp_logs

    def update_zp_project(self, project_id, new_project_users):
        update_project = api_request(
            "https://people.zoho.com/people/api/forms/json/P_TimesheetJobsList/updateRecord?inputData={ProjectUsers:'" + ';'.join(
                new_project_users) + "'}&recordId=" + project_id,
            "zoho_people",
            "post",
            None
        )
        print(f"\t\tProject Update: {update_project['response']['message']}")

    def get_project_jobs(self, project_id):
        response = api_request(f"https://people.zoho.com/people/api/timetracker/getjobs?assignedTo=all&projectId={project_id}&sIndex=0&limit=200", "zoho_people", "get", None)
        return response['response']['result']

    def get_zp_job_by_clickup_id(self, clickup_task_id):
        try:
            zp_job = api_request(
                "https://people.zoho.com/people/api/forms/P_TimesheetJob/getRecords?sIndex=1&rec_limit=200&searchParams={searchField: 'Clickup_ID', searchOperator: 'Is', searchText : '" + clickup_task_id + "'}",
                "zoho_people", "get", None)['response']['result']
        except KeyError:
            zp_job = []
        return zp_job

    def update_zp_job(self, job_id, job_assignees):
        update_job = api_request(
            "https://people.zoho.com/people/api/forms/json/P_TimesheetJob/updateRecord?inputData={Assignees:'" + ';'.join(job_assignees) + "'}&recordId=" + str(job_id),
            "zoho_people",
            "post",
            None
        )
        print(f"\t\tUpdate Job: {update_job['response']['message']}")

    def create_zp_job(self, project_id, task_id, job_name, job_assignees):
        if job_name == 'Смена рейта для контракторов':
            job_name = "Change Rates for contractors"
        response = api_request(
            "https://people.zoho.com/people/api/forms/json/P_TimesheetJob/insertRecord?inputData={Job_Name:'" + prettify_task_name(job_name) + "',Project:'" + project_id + "',Assignees:'" + ';'.join(job_assignees) + "',Clickup_ID:'" + task_id + "'}",
            "zoho_people",
            "post",
            None
        )
        print(f"\t\tCreate Job: {response['response']['message']}")
        print(response)
        return response['response']['result']['pkId']

    def get_zp_job_by_id(self, job_id):
        response = api_request(
            f"https://people.zoho.com/people/api/forms/P_TimesheetJob/getDataByID?recordId={job_id}",
            "zoho_people",
            "get",
            None
        )
        return response['response']['result'][0]

    def get_log_billability(self, timelog):
        task_custom_fields = timelog['task_custom_fields'] or []
        is_log_billable = timelog['is_billable']
        is_task_non_billable = False
        task_billable_status = None
        for task_custom_field in task_custom_fields:
            if task_custom_field['id'] == 'ff1292a4-81ea-483e-8bae-9d13dff5d1c0':
                billability_cf_option = task_custom_field['value'] if 'value' in task_custom_field else ""
                if billability_cf_option == 1:
                    is_task_non_billable = True
                    task_billable_status = "non-billable"
                elif billability_cf_option == 0:
                    task_billable_status = "billable"
        return task_billable_status if task_billable_status else "billable" if is_log_billable else "non-billable"
        # if is_task_non_billable:
        #     return "non-billable"
        # else:
        #     # return "billable" if is_log_billable else "non-billable"
        #     return "billable"

    def delete_time_tracked(self, timelog_ids):
        response = requests.post("https://www.zohoapis.com/crm/v7/functions/delete_bulk_timelogs/actions/execute?auth_type=apikey&zapikey=1003.4dc6e131901e2b9c2dc52b73bc81a5ad.b66d89f85e0370bb8fb7ba2a082271de", json={"timelogs": timelog_ids})
        status_code = response.status_code
        print(f"\tDeleting Time Logs Status: {status_code}")
        return status_code

    def clone_time_tracked(self, timelogs):
        print(f"Total Timelogs to Push: {len(timelogs)}")
        batch_counter = 0
        for logs_batch in batch_generator(timelogs):
            batch_counter += 1
            print(f"Batch: {batch_counter}")
            batch_post_data = {"timelogs": logs_batch}
            response = requests.post(
                "https://www.zohoapis.com/crm/v7/functions/addbulktimelogs/actions/execute?auth_type=apikey&zapikey=1003.4dc6e131901e2b9c2dc52b73bc81a5ad.b66d89f85e0370bb8fb7ba2a082271de",
                json=batch_post_data
            )
            output = response.json()['details']['output']
            print(f"\tBatch Status: {json.loads(output)['response']['message']}")
            print(response.json())

    def find_bench_job(self, job_name):
        try:
            zp_jobs = api_request(
                "https://people.zoho.com/people/api/forms/P_TimesheetJob/getRecords?sIndex=1&rec_limit=200&searchParams={searchField: 'Job_Name', searchOperator: 'Is', searchText : '" + job_name + "'}",
                "zoho_people", "get", None)['response']['result']
            if "378942000004253148" not in str(zp_jobs):
                return []
            for zp_job in zp_jobs:
                zp_job_id = list(zp_job.keys())[0]
                zp_job_details = zp_job[zp_job_id][0]
                if zp_job_details['Project.ID'] == "378942000004253148":
                    return [zp_job]
        except KeyError:
            zp_job = []
        return zp_job


    def find_interviewing_job(self, job_name):
        try:
            zp_jobs = api_request(
                "https://people.zoho.com/people/api/forms/P_TimesheetJob/getRecords?sIndex=1&rec_limit=200&searchParams={searchField: 'Job_Name', searchOperator: 'Is', searchText : '" + job_name + "'}",
                "zoho_people", "get", None)['response']['result']
            if "378942000015322402" not in str(zp_jobs):
                return []
            for zp_job in zp_jobs:
                zp_job_id = list(zp_job.keys())[0]
                zp_job_details = zp_job[zp_job_id][0]
                if zp_job_details['Project.ID'] == "378942000015322402":
                    return [zp_job]
        except KeyError:
            zp_job = []
        return zp_job


    def get_user_blockings(self, user_id):
        start_date_unix = str(datetime_str_to_unix(START_DATE, 2, 0))
        end_date_unix = str(datetime_str_to_unix(END_DATE, 22, 0))
        response = requests.get(
            'https://api.clickup.com/api/v2/list/901204980269/task?custom_fields=[{"field_id":"912a953f-4c89-44cb-844d-603111aa7eb1","operator":"ANY","value":["' + str(user_id) + '"]}]&due_date_lt=' + end_date_unix + '&due_date_gt=' + start_date_unix,
            headers=CLICKUP_HEADERS
        )
        blockings = {}
        for task in response.json()['tasks']:
            custom_fields = task['custom_fields']
            is_internal_blocking = False
            is_internal_head = False
            blocked_hours = 0
            for custom_field in custom_fields:
                if custom_field['id'] == '7f3b4b79-b252-42ea-83fc-8c59445148f9':
                    try:
                        related_project_id = custom_field['value'][0]['id']
                    except:
                        related_project_id = None
                    if related_project_id and related_project_id in blockings_mapping:
                        is_internal_blocking = True
                        if related_project_id == "869775k91":
                            is_internal_head = True
                elif custom_field['id'] == '5078d821-4695-4e09-ae6c-81e29081ef66':
                    blocked_hours = float(custom_field['value'])

            if is_internal_blocking and blocked_hours > 0:
                print(f"Is Internal Blocking: {is_internal_blocking}")
                print(f"Is Head Blocking: {is_internal_head}")
                print(f"Blocked Hours: {blocked_hours}")
                blocking_info = blockings_mapping[related_project_id]
                blocking_info['blocked_hours'] = blocked_hours
                blockings[related_project_id] = blocking_info
        return blockings

    def bench_no_bench_sort(self, logs, user_email):
        clickup_user_id = None
        for clickup_user in self.all_clickup_users:
            if user_email == clickup_user['user']['email']:
                clickup_user_id = clickup_user['user']['id']
        user_blockings = self.get_user_blockings(clickup_user_id)

        # input(user_blockings)
        # user_blockings = {'869775k91': {'name': 'Kitrum Internal Activities', 'list_id': '', 'blocked_hours': 95.5}}
        bench_no_bench_sorted = []
        total_krw = 0
        if user_email in OPERATIONS_TEAM:
            print("No need to check")
            bench_no_bench_sorted = [log for log in logs]
        elif '869775k91' in user_blockings:
            print("Internal Head Lead")
            internal_limit = user_blockings['869775k91']['blocked_hours']

            for log in logs:
                log_project_type = self.project_types[log['projectId']]
                if log_project_type in ['commercial-project', 'bench']:
                    bench_no_bench_sorted.append(log)
                    continue
                log_duration = log['hours']
                minutes, seconds = map(int, log_duration.split(":"))
                time_float = minutes + (seconds / 60)
                total_krw += time_float
                if time_float <= internal_limit:
                    internal_limit -= time_float
                    bench_no_bench_sorted.append(log)
                else:
                    if internal_limit > 0:
                        non_bench_time = internal_limit
                        bench_time = time_float - non_bench_time
                        internal_limit = 0
                    else:
                        non_bench_time = 0
                        bench_time = time_float
                    if non_bench_time > 0:
                        log_copy_no_bench = log.copy()
                        log_copy_no_bench['hours'] = format_hours(round(non_bench_time, 2))
                        bench_no_bench_sorted.append(log_copy_no_bench)
                    if bench_time > 0:
                        log_copy_bench = log.copy()
                        log_copy_bench['hours'] = format_hours(round(bench_time, 2))
                        log_copy_bench['description'] = f"{log['description']} [bench]"
                        bench_no_bench_sorted.append(log_copy_bench)
        else:
            print("Need to check")
            for log in logs:
                log_project_type = self.project_types[log['projectId']]
                if log_project_type in ['commercial-project', 'bench']:
                    bench_no_bench_sorted.append(log)
                    continue
                log_duration = log['hours']
                minutes, seconds = map(int, log_duration.split(":"))
                # time_float = minutes + (seconds / 60)
                is_bench = True
                if log['projectId'] == "378942000015322402":
                    if '86980z1gb' in user_blockings:
                        is_bench = False
                else:
                    user_blocking_list_ids = []
                    for blocking_values in list(user_blockings.values()):
                        user_blocking_list_ids.append(blocking_values['list_id'])
                    if log['list_id'] in user_blocking_list_ids:
                        is_bench = False
                if is_bench:
                    log_copy = log.copy()
                    log_copy['description'] = f"{log['description']} [bench]"
                    bench_no_bench_sorted.append(log_copy)
                else:
                    bench_no_bench_sorted.append(log)
        print(total_krw)
        return bench_no_bench_sorted

    def clean_logs(self, logs):
        relevant_keys = ["user", "jobId", "date", "billableStatus", "hours", "workItem", "description"]
        for log in logs:
            log_keys = list(log.keys())
            for log_key in log_keys:
                if log_key not in relevant_keys:
                    del log[log_key]
        return logs


    def push_logs(self):
        for user_email, logs_by_project in self.project_splitted_logs_by_user.items():
            print(f"Starting... User: {user_email}")
            time.sleep(5)
            zp_user_id = None
            # if user_email == "daryna.gudyma@kitrum.com":
            #     user_email = "gudymamain@gmail.com"

            for zp_employee in self.zp_employees:
                if zp_employee['email'] == user_email:
                    zp_user_id = str(zp_employee['id'])
                    break

            all_formatted_user_logs, all_logs_to_delete = [], []
            user_zp_logs = self.get_zp_logs(user_email)
            for user_zp_log in user_zp_logs:
                try:
                    log_desc = int(user_zp_log['description'].replace("[bench]", ""))
                    is_clickup_log = True
                except:
                    is_clickup_log = False
                if is_clickup_log:
                    all_logs_to_delete.append(user_zp_log['timelogId'])

            for project_id, project_logs in logs_by_project.items():
                print("--------------" * 10)
                print(f"Project ID: {project_id}")
                formatted_project_logs, project_logs_to_delete = [], []
                project_type = self.project_types[project_id] if project_id in self.project_types else "internal-activities"
                print(f"Project Type: {project_type}")
                print("\nAudit Logs: ")

                # logs_submitted = False
                # for user_zp_log in user_zp_logs:
                #     pass
                    # if user_zp_log['approvalStatus'] in ['draft', 'pending', 'approved'] and user_zp_log['projectId'] == project_id:
                    #     logs_submitted = True
                    # if user_zp_log['projectId'] == project_id:
                    #     project_logs_to_delete.append(user_zp_log['timelogId'])
                    # try:
                    #     log_desc = int(user_zp_log['description'].replace("[bench]", ""))
                    #     is_clickup_log = True
                    # except:
                    #     is_clickup_log = False
                    # if is_clickup_log:
                    #     project_logs_to_delete.append(user_zp_log['timelogId'])

                # Uncomment
                # if logs_submitted:
                #     print("Some Logs are already submitted!!!")
                #     continue
                # all_logs_to_delete.extend(project_logs_to_delete)

                zp_project_details = None
                for zp_project in self.zp_projects:
                    if project_id in zp_project:
                        zp_project_details = zp_project[project_id][0]

                project_head = zp_project_details['ProjectHead.details']
                project_users = zp_project_details['ProjectUsers.details'] if 'ProjectUsers.details' in zp_project_details else []

                print(f"\tChecking if user is assigned to project: {project_id}")
                if user_email not in str(project_head) and user_email not in str(project_users):
                    print("\t\tUser Assigned to Project: False")
                    new_project_users = [x['erecno'] for x in project_users]
                    new_project_users.append(zp_user_id)
                    self.update_zp_project(project_id, new_project_users)
                else:
                    print("\t\tUser Assigned to Project: True")

                if project_type == "presale":
                    project_jobs = self.get_project_jobs(project_id)
                    presale_job_id = None
                    for project_job in project_jobs:
                        if ' - presale' in project_job['jobName'].lower():
                            presale_job_id = project_job['jobId']

                    presale_job = self.get_zp_job_by_id(presale_job_id)

                    job_assignee_ids = presale_job['Assignees.ID'] or ''
                    if zp_user_id not in job_assignee_ids:
                        job_assignees = job_assignee_ids.split(";")
                        job_assignees.append(zp_user_id)
                        self.update_zp_job(presale_job_id, job_assignees)
                    for project_log in project_logs:
                        formatted_log = {
                            "user": user_email,
                            "jobId": presale_job_id,
                            "date": unix_to_date(project_log['start_timestamp']),
                            "billableStatus": self.get_log_billability(project_log),
                            "hours": format_hours(round(project_log['duration_h'], 2)),
                            "workItem": project_log['description'],
                            "description": project_log['timelog_id'],
                            "projectId": project_id,
                            "list_id": project_log['list_id']
                        }
                        formatted_project_logs.append(formatted_log)
                elif project_type == "internal-presale":
                    for project_log in project_logs:
                        formatted_log = {
                            "user": user_email,
                            "jobId": "378942000023897516",
                            "date": unix_to_date(project_log['start_timestamp']),
                            "billableStatus": self.get_log_billability(project_log),
                            "hours": format_hours(round(project_log['duration_h'], 2)),
                            "workItem": project_log['description'],
                            "description": project_log['timelog_id'],
                            "projectId": project_id,
                            "list_id": project_log['list_id']
                        }
                        formatted_project_logs.append(formatted_log)
                elif project_type == "internal-activities":
                    checked_job_ids = {}
                    for project_log in project_logs:
                        clickup_task_id = project_log['task_id']
                        if project_log['task_type'] == 1013:
                            # interviewing_job_name = project_log['task_name'].replace("Technical Interviews for ", "Tech Interview for ")
                            interviewing_job_name = project_log['task_name']
                            zp_job = self.find_interviewing_job(interviewing_job_name)
                        elif project_log['task_type'] == 1014:
                            bench_job_name = "Idle Time"
                            zp_job = self.find_bench_job(bench_job_name)
                        else:
                            input(f"Unknown Task Type {project_log['task_type']}")
                            print(project_log)
                        if zp_job:
                            print("\t\tJob Exist: True")
                            zp_job_id = list(zp_job[0].keys())[0]
                            zp_job_details = zp_job[0][zp_job_id][0]
                            job_assignee_ids = zp_job_details['Assignees.ID']
                            print(f"\tChecking is user is assigned to job {zp_job_id}")
                            if zp_user_id not in job_assignee_ids:
                                print("\t\tUser Assigned to Job: False")
                                job_assignees = job_assignee_ids.split(";")
                                job_assignees.append(zp_user_id)
                                self.update_zp_job(zp_job_id, job_assignees)
                            else:
                                print("\t\tUser Assigned to Job: True")
                        else:
                            print("\t\tJob Exist: False")
                            zp_job_id = input(f"Provide Interviewing Job ID for {interviewing_job_name}")
                        checked_job_ids[clickup_task_id] = zp_job_id

                    for project_log in project_logs:
                        formatted_log = {
                            "user": user_email,
                            "jobId": checked_job_ids[project_log['task_id']],
                            "date": unix_to_date(project_log['start_timestamp']),
                            "billableStatus": "non-billable",
                            "hours": format_hours(round(project_log['duration_h'], 2)),
                            "workItem": project_log['description'],
                            "description": project_log['timelog_id'],
                            "projectId": project_id,
                            "list_id": project_log['list_id']

                        }
                        formatted_project_logs.append(formatted_log)
                else:
                    checked_job_ids = {}
                    for project_log in project_logs:
                        clickup_task_id = project_log['task_id']
                        clickup_task_name = project_log['task_name']
                        if clickup_task_id in checked_job_ids:
                            continue
                        # if clickup_task_id in checked_job_ids:
                        #     continue
                        print(f"\tSearching for job {clickup_task_id} in Zoho People")
                        print(project_log)

                        zp_job = self.get_zp_job_by_clickup_id(clickup_task_id)
                        if zp_job:
                            print("\t\tJob Exist: True")
                            zp_job_id = list(zp_job[0].keys())[0]
                            zp_job_details = zp_job[0][zp_job_id][0]
                            job_assignee_ids = zp_job_details['Assignees.ID']
                            print(f"\tChecking is user is assigned to job {zp_job_id}")
                            if zp_user_id not in job_assignee_ids:
                                print("\t\tUser Assigned to Job: False")
                                job_assignees = job_assignee_ids.split(";")
                                job_assignees.append(zp_user_id)
                                self.update_zp_job(zp_job_id, job_assignees)
                            else:
                                print("\t\tUser Assigned to Job: True")
                        else:
                            print("\t\tJob Exist: False")
                            zp_job_id = self.create_zp_job(project_id, clickup_task_id, clickup_task_name, [zp_user_id])
                        checked_job_ids[clickup_task_id] = zp_job_id
                    for project_log in project_logs:
                        formatted_log = {
                            "user": user_email,
                            "jobId": checked_job_ids[project_log['task_id']],
                            "date": unix_to_date(project_log['start_timestamp']),
                            "billableStatus": self.get_log_billability(project_log),
                            "hours": format_hours(round(project_log['duration_h'], 2)),
                            "workItem": project_log['description'],
                            "description": project_log['timelog_id'],
                            "projectId": project_id,
                            "list_id": project_log['list_id']
                        }
                        formatted_project_logs.append(formatted_log)
                all_formatted_user_logs.extend(formatted_project_logs)
            sorted_people_logs = sorted(all_formatted_user_logs, key=lambda x: datetime.datetime.strptime(x["date"], "%Y-%m-%d"))
            bench_no_bench_sorted = self.bench_no_bench_sort(sorted_people_logs, user_email)
            final_zp_logs = self.clean_logs(bench_no_bench_sorted)

            if all_logs_to_delete:
                print("\nDeleting Existed Logs")
                self.delete_time_tracked(all_logs_to_delete)
            print(f"Formatted Logs:")

            if final_zp_logs:
                print("\nAdding New Logs")
                self.clone_time_tracked(final_zp_logs)

    def get_all_clickup_users(self):
        response = requests.get("https://api.clickup.com/api/v2/team", headers=CLICKUP_HEADERS)
        self.all_clickup_users = response.json()['teams'][0]['members']

    def launch(self):
        self.get_all_clickup_users()
        self.get_zp_projects()
        self.get_month_logs()
        self.split_logs_by_project()
        self.push_logs()


logs_handler = LogsDuplicator([
    'daryna.gudyma@kitrum.com'
])
logs_handler.launch()




