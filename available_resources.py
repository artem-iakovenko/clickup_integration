import time
from datetime import datetime, timedelta
from integration_config import CLICKUP_HEADERS
from zoho_api.api import api_request
import requests
import json
from help_functions import datetime_str_to_unix, unix_to_date

PRIORITY_MAPPING = {
    "Low": "👀  Low",
    "Normal": "⚠️ Normal",
    "High": "🔥 High",
    "Urgent": "🚨Super high"
}


CFS_MAPPING = {
    "dev_direction": {"field_id": "baf5146f-534c-44e0-9b9d-f84329154369", "field_type": "dropdown"},
    "work_achievements": {"field_id": "76ba0dbe-7241-4941-ba9f-f958cf8b033c", "field_type": "multiline"},
    "cv": {"field_id": "ea0f0886-07b5-4f1e-bfbc-a9567bfb57fd", "field_type": "link"},
    "domains": {"field_id": "167e8ad1-1310-4a6a-b47b-abc67a0615e0", "field_type": "labels"},
    "rate": {"field_id": "07860e8a-ffc6-4e34-9842-036ed141404c", "field_type": "currency"},
    "salary": {"field_id": "d6109753-ffbb-4163-9b61-9c89c3571a0c", "field_type": "currency"},
    "seniority": {"field_id": "006faef7-4e5a-41c7-8b54-a8ed2665bb70", "field_type": "dropdown"},
    "specialization": {"field_id": "060bc090-91d8-4dd1-8e3f-f7e0340a8f27", "field_type": "text"},
    "tech_summary": {"field_id": "3943bf3f-676e-40ec-8f83-fb9d68b91570", "field_type": "multiline"},
    "title": {"field_id": "33cc9332-2bda-43e3-97fc-1131c8a0d5ee", "field_type": "dropdown"},
    "not_work_domains": {"field_id": "7c619406-2737-4546-b2e3-b812d914cb91", "field_type": "labels"},
    "dev_url": {"field_id": "e6b5529b-167f-45f5-998c-cbeece722706", "field_type": "link"},
    "country": {"field_id": "8a8370c7-1a6c-400d-a272-f9bb38c81f58", "field_type": "dropdown"},
    "workload_details": {"field_id": "065bbcec-d27a-4aa5-8ac9-8648267ad50e", "field_type": "multiline"},
    "priority": {"field_id": "88343262-ed95-4283-b899-13f68b232c63", "field_type": "dropdown"},
    # "workload": {"field_id": "66c97dab-0d09-4dc2-9416-ae666a6e6d42", "field_type": "dropdown"},
    "free_hours": {"field_id": "cf0b9445-8383-4d93-bc56-52a2c2c551b7", "field_type": "number"},
    "search_reason": {"field_id": "5bd446d7-881f-4cd2-83e5-45ccab230b53", "field_type": "labels"},
    "developer": {"field_id": "912a953f-4c89-44cb-844d-603111aa7eb1", "field_type": "user"},
    "kitrum_available_date": {"field_id": "5f628402-6989-4feb-9d57-16b8512a6b25", "field_type": "date"},
    "ready_to_strat_date": {"field_id": "eb73cb70-9a97-4632-aa6e-aef694afb958", "field_type": "date"}
}


class AvailableResources:
    def __init__(self, cursor_date):
        self.picklist_options = {}
        self.all_clickup_users = []
        self.zp_employees = []
        self.cursor_date = cursor_date
        self.available_resources = []
        self.rm_forms = []
        self.clickup_blocks = []
        self.clickup_blocks_by_dev = {}
        self.clickup_blocks_by_crm_dev = {}
        self.involved_task_ids = []
        self.untouched_forms = []

    def get_free_hours_str(self, free_hours_dict):
        free_hours_list = []
        for month, hours in free_hours_dict.items():
            formatted_date = datetime.strptime(month, "%Y-%m-%d").strftime("%b %Y")
            free_hours_list.append(f"{formatted_date}: {hours} hours")
        free_hours_list.reverse()
        return "\n".join(free_hours_list)

    def get_all_clickup_users(self):
        response = requests.get("https://api.clickup.com/api/v2/team", headers=CLICKUP_HEADERS)
        self.all_clickup_users = response.json()['teams'][0]['members']

    def get_list_picklists(self):
        response = requests.get("https://api.clickup.com/api/v2/list/901202112299/field", headers=CLICKUP_HEADERS)
        time.sleep(1)
        fields = response.json()['fields']
        for field in fields:
            field_id, field_type = field['id'], field['type']
            if field_type in ["drop_down", "labels"]:
                field_options = {}
                picklist_options = field['type_config']['options']
                for picklist_option in picklist_options:
                    if field_type == "drop_down":
                        field_options[picklist_option['name']] = picklist_option['id']
                    elif field_type == "labels":
                        field_options[picklist_option['label']] = picklist_option['id']
                self.picklist_options[field_id] = field_options

    def get_zp_employees(self):
        s_index = 1
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
                self.zp_employees.append({"email": zp_employee_data['EmailID'], 'id': zp_employee_data['Zoho_ID'],
                               'name': f'{zp_employee_data["FirstName"]} {zp_employee_data["LastName"]}',
                               'short_id': zp_employee_data['EmployeeID'], "crm_id": zp_employee_data['CRM_Developer_ID']})
            s_index += 200

    def get_rm_forms(self):
        self.rm_forms = api_request(
            "https://www.zohoapis.com/crm/v2/RM_Forms?cvid=1576533000406527038",
            "zoho_crm",
            "get",
            None
        )['data']

    def get_available_resources(self):
        response = requests.get(f"https://api.clickup.com/api/v2/list/901202112299/task", headers=CLICKUP_HEADERS)
        self.available_resources = response.json()['tasks']

    def get_clickup_blocks(self):
        # custom_fields_param = json.dumps([{"field_id": "cf0b9445-8383-4d93-bc56-52a2c2c551b7", "operator": ">", "value": "0"}])
        custom_fields_param = json.dumps([
            {"field_id": "cf0b9445-8383-4d93-bc56-52a2c2c551b7", "operator": ">", "value": "0"},
            {"field_id": "031efcab-a89c-4f7f-bb03-208b209943a9", "operator": "=", "value": "ed86b075-14fa-4983-a77a-3a3e3321f2ed"}
        ])
        due_date_param = str(datetime_str_to_unix(self.cursor_date, 2, 0))
        response = requests.get(f"https://api.clickup.com/api/v2/list/901204930768/task?due_date_gt={due_date_param}&custom_fields={custom_fields_param}", headers=CLICKUP_HEADERS)
        self.clickup_blocks = response.json()['tasks']

    def prepare_clickup_blocks(self):
        for clickup_block in self.clickup_blocks:
            resource_id = clickup_block['id']
            due_date = clickup_block['due_date']
            due_date_str = unix_to_date(due_date)
            related_developer_id, related_developer_email = None, None
            zcrm_dev_url = None
            free_hours = 0
            custom_fields = clickup_block['custom_fields']

            for custom_field in custom_fields:
                if custom_field['id'] == "912a953f-4c89-44cb-844d-603111aa7eb1" and 'value' in custom_field:
                    related_developer_id, related_developer_email = custom_field['value'][0]['id'], custom_field['value'][0]['email']
                elif custom_field['id'] == "e6b5529b-167f-45f5-998c-cbeece722706" and 'value' in custom_field:
                    zcrm_dev_url = custom_field['value']
                elif custom_field['id'] == "cf0b9445-8383-4d93-bc56-52a2c2c551b7" and 'value' in custom_field:
                    free_hours = round(float(custom_field['value']), 2)
            if related_developer_id and free_hours > 0:
                if related_developer_id not in self.clickup_blocks_by_dev:
                    self.clickup_blocks_by_dev[related_developer_id] = {'resource_id': resource_id, 'user_email': related_developer_email, "free_hours": {due_date_str: free_hours}}
                else:
                    self.clickup_blocks_by_dev[related_developer_id]['free_hours'][due_date_str] = free_hours

            if zcrm_dev_url and free_hours > 0:
                zcrm_dev_id = zcrm_dev_url.replace("https://crm.zoho.com/crm/org55415226/tab/CustomModule1/", "")
                if zcrm_dev_id not in self.clickup_blocks_by_crm_dev:
                    self.clickup_blocks_by_crm_dev[zcrm_dev_id] = {"free_hours": {due_date_str: free_hours}}
                else:
                    self.clickup_blocks_by_crm_dev[zcrm_dev_id]['free_hours'][due_date_str] = free_hours

    def get_zp_data_by_email(self, email):
        for zp_employee in self.zp_employees:
            if zp_employee['email'] == email:
                return zp_employee

    def get_zp_data_by_crm_id(self, crm_id):
        for zp_employee in self.zp_employees:
            if zp_employee['crm_id'] == crm_id:
                return zp_employee

    def get_clickup_user_by_email(self, user_email):
        for user in self.all_clickup_users:
            if user['user']['email'] == user_email:
                return user

    def collect_resource_details(self, developer_id):
        dev_response = api_request(
            f"https://www.zohoapis.com/crm/v2/Developers/{developer_id}",
            "zoho_crm",
            "get",
            None
        )['data'][0]
        return {
            "dev_name": dev_response["Name"],
            "dev_direction": dev_response["Direction"],
            "work_achievements": dev_response["Work_achievements"],
            "cv": dev_response["Core_CV"],
            "domains": (dev_response["Has_experience_in_domains_New"] or []) + (dev_response["Has_experience_in_subdomains"] or []),
            "subdomains": dev_response["Has_experience_in_subdomains"],
            "rate": dev_response["Dev_s_Rate_1"],
            "salary": dev_response["Salary_monthly_gross"],
            "seniority": dev_response["Seniority"],
            "specialization": dev_response["Specialization"],
            "tech_summary": dev_response["Test_test"],
            "title": dev_response["Title"],
            "not_work_domains": (dev_response["Will_never_work_with_domains_New"] or []) + (dev_response["Will_never_work_with_subdomains"] or []),
            "dev_url": f"https://crm.zoho.com/crm/org55415226/tab/CustomModule1/{developer_id}",
            "country": dev_response["Location"]
        }

    def form_ar_task_data(self, combined_data):
        custom_fields_data = []
        for field_name, field_value in combined_data.items():
            if field_name in CFS_MAPPING:
                field_id = CFS_MAPPING[field_name]['field_id']
                field_type = CFS_MAPPING[field_name]['field_type']
                if field_type == "dropdown":
                    try:
                        value_id = self.picklist_options[field_id][field_value]
                    except KeyError:
                        value_id = ""
                    custom_fields_data.append({"id": field_id, "value": value_id})
                elif field_type == "labels":
                    value_ids = []
                    for x in field_value:
                        try:
                            value_ids.append(self.picklist_options[field_id][x])
                        except KeyError:
                            continue
                    custom_fields_data.append({"id": field_id, "value": value_ids})
                elif field_type == "user":
                    if not field_value:
                        continue
                    custom_fields_data.append({"id": field_id, "value": {"add": [field_value]}, "rem": []})
                elif field_type == "date":
                    custom_fields_data.append({"id": field_id, "value": str(datetime_str_to_unix(field_value, 2, 0))})
                else:
                    custom_fields_data.append({"id": field_id, "value": field_value or ""})
        return {
            "name": combined_data['dev_name'],
            "tags": ["available now"],
            "custom_fields": custom_fields_data
        }

    def create_or_update_block_by_free_time(self):
        counter = 0
        # for user_id, blocking_data in self.clickup_blocks_by_dev.items():
        for crm_developer_id, blocking_data in self.clickup_blocks_by_crm_dev.items():
            counter += 1
            time.sleep(2)
            user_zp_data = self.get_zp_data_by_crm_id(crm_developer_id)
            user_email = user_zp_data['email'] if user_zp_data else None
            print(f"{counter}/{len(list(self.clickup_blocks_by_crm_dev.keys()))}. Developer: {user_email}")

            # FIND RESOURCE CARD
            available_resource_id = None
            for available_resource in self.available_resources:
                custom_fields = available_resource['custom_fields']
                crm_profile_link = None
                for custom_field in custom_fields:
                    if custom_field['id'] == 'e6b5529b-167f-45f5-998c-cbeece722706' and 'value' in custom_field:
                        crm_profile_link = custom_field['value']
                        break
                if crm_profile_link and crm_developer_id in crm_profile_link:
                    available_resource_id = available_resource['id']
                    break

            if available_resource_id and available_resource_id in self.involved_task_ids:
                print("Task already updated")
                continue
            print("----------" * 10)

            # COLLECT RM DATA
            free_hours_dict = blocking_data['free_hours']
            free_hours_str = self.get_free_hours_str(free_hours_dict)
            min_date_key = min(free_hours_dict.keys())
            free_hours = free_hours_dict[min_date_key]
            if 0 <= free_hours <= 10:
                priority = "👀  Low"
            elif 11 <= free_hours <= 40:
                priority = "⚠️ Normal"
            elif 41 <= free_hours <= 80:
                priority = "🔥 High"
            elif free_hours > 80:
                priority = "🚨Super high"
            if free_hours <= 80:
                workload = 'Part-time'
            else:
                workload = 'Full-time'
            blocking_details = {
                "workload_details": free_hours_str,
                "priority": priority,
                "workload": workload,
                "free_hours": free_hours,
                "search_reason": ["Underloaded"]
            }

            # COLLECT DEVELOPER DETAILS AND FILL CLICKUP USER ID
            developer_details = self.collect_resource_details(crm_developer_id)
            clickup_user = self.get_clickup_user_by_email(user_email) if user_email else None
            user_id = clickup_user['user']['id'] if clickup_user else None
            developer_details['developer'] = user_id

            # COMBINE ALL COLLECTED DATA
            combined_details = developer_details | blocking_details

            # PREPARE DATA TO BE POSTED TO CLICKUP
            ar_task_data = self.form_ar_task_data(combined_details)

            # UPDATE RESOURCE
            if available_resource_id:
                print(f'Updating Resource: {available_resource_id}')
                for custom_field in ar_task_data['custom_fields']:
                    if custom_field['id'] in ["88343262-ed95-4283-b899-13f68b232c63"]:
                        continue
                    response = requests.post(f"https://api.clickup.com/api/v2/task/{available_resource_id}/field/{custom_field['id']}", headers=CLICKUP_HEADERS, json={'value': custom_field['value']})
                    print(f'\tUpdate CF Response: {response.status_code}')
                self.involved_task_ids.append(available_resource_id)
            # CREATE RESOURCE
            else:
                print('Creating New Resource')
                response = requests.post("https://api.clickup.com/api/v2/list/901202112299/task", headers=CLICKUP_HEADERS, json=ar_task_data)
                created_task_id = response.json()['id']
                print(f"Created Task ID: {created_task_id}")
                self.involved_task_ids.append(created_task_id)

    def create_or_update_block_by_rm_form(self):
        counter = 0
        for rm_form in self.rm_forms:
            counter += 1
            time.sleep(2)

            # GET BASIC DEVELOPER DATA
            crm_developer_name = rm_form['Developer_Name']['name']
            crm_developer_id = rm_form['Developer_Name']['id']
            user_zp_data = self.get_zp_data_by_crm_id(crm_developer_id)
            user_email = user_zp_data['email'] if user_zp_data else None
            print(f"{counter}/{len(self.rm_forms)}. Developer: {user_email or crm_developer_name}")

            # GET DEVELOPER BLOCKS WITH FREE HOURS
            developer_blocks = self.clickup_blocks_by_crm_dev[crm_developer_id] if crm_developer_id in self.clickup_blocks_by_crm_dev else {}
            free_hours_dict = developer_blocks['free_hours'] if developer_blocks else {}

            # GET FREE HOURS
            workload = rm_form['Possible_workload'] or ""
            if free_hours_dict:
                min_date_key = min(free_hours_dict.keys())
                free_hours = free_hours_dict[min_date_key]
            else:
                if workload in "Full-time":
                    free_hours = 160
                elif workload in "Part-time":
                    free_hours = 80
                else:
                    free_hours = ""

            # FIND AVAILABLE RESOURCE CARD
            available_resource_id = None
            for available_resource in self.available_resources:
                custom_fields = available_resource['custom_fields']
                crm_profile_link = None
                for custom_field in custom_fields:
                    if custom_field['id'] == 'e6b5529b-167f-45f5-998c-cbeece722706' and 'value' in custom_field:
                        crm_profile_link = custom_field['value']
                        break
                if crm_profile_link and crm_developer_id in crm_profile_link:
                    available_resource_id = available_resource['id']
                    break

            # CHECK IF TASK WAS ALREADY UPDATED DURING SYNC
            if available_resource_id and available_resource_id in self.involved_task_ids:
                print("Task already updated")
                continue

            # COLLECT DEVELOPER DETAILS AND FILL CLICKUP USER ID
            developer_details = self.collect_resource_details(crm_developer_id)
            clickup_user = self.get_clickup_user_by_email(user_email) if user_email else None
            user_id = clickup_user['user']['id'] if clickup_user else None
            developer_details['developer'] = user_id

            # COLLECT RM + RESOURCE WORKLOAD DATA
            rm_status = rm_form['RM_status']
            search_reason = [rm_status]
            if developer_blocks:
                search_reason.append("Underloaded")
            rm_workload_details = rm_form['Availability_Workload_details']
            clickup_workload_details = self.get_free_hours_str(free_hours_dict) if free_hours_dict else ""
            combined_workload_details_list = []
            if rm_workload_details:
                combined_workload_details_list.append(f"RM Form Data:\n{rm_workload_details}")
            if clickup_workload_details:
                combined_workload_details_list.append(f"Clickup Workload List Data:\n{clickup_workload_details}")
            combined_workload_details = "\n\n".join(combined_workload_details_list)
            kitrum_available_from_date = ""
            if rm_status in ['Required project transfer', 'Need an additional project', 'Possible project transfer']:
                kitrum_available_from_date = rm_form['Final_date_on_project']
            elif rm_status in ['Bench']:
                kitrum_available_from_date = rm_form['Bench_end_date']
            elif rm_status in ['Technical leave']:
                kitrum_available_from_date = rm_form['Tech_Leave_end_date']

            if developer_blocks and free_hours > 10:
                priority = "Urgent"
            elif developer_blocks and free_hours < 10:
                priority = "High"
            else:
                priority = rm_form['Search_Priority']

            rm_details = {
                'workload_details':  combined_workload_details,
                'search_reason': search_reason,
                'kitrum_available_date': kitrum_available_from_date,
                'ready_to_strat_date': rm_form['Ready_to_start_date'],
                'priority': PRIORITY_MAPPING[priority] if priority else "",
                'workload':  workload,
                "free_hours": free_hours
            }

            # COMBINE ALL COLLECTED DATA
            combined_details = developer_details | rm_details

            # PREPARE DATA TO BE POSTED TO CLICKUP
            ar_task_data = self.form_ar_task_data(combined_details)

            # UPDATE RESOURCE
            if available_resource_id:
                print('Updating Resource')
                for custom_field in ar_task_data['custom_fields']:
                    if custom_field['id'] in ["88343262-ed95-4283-b899-13f68b232c63"]:
                        continue
                    response = requests.post(f"https://api.clickup.com/api/v2/task/{available_resource_id}/field/{custom_field['id']}", headers=CLICKUP_HEADERS, json={'value': custom_field['value']})
                    print(f'\tUpdate CF Response: {response.status_code}')
                self.involved_task_ids.append(available_resource_id)
            # CREATE RESOURCE
            else:
                print('Creating New Resource')
                response = requests.post("https://api.clickup.com/api/v2/list/901202112299/task", headers=CLICKUP_HEADERS, json=ar_task_data)
                created_task_id = response.json()['id']
                print(f"Created Task ID: {created_task_id}")
                self.involved_task_ids.append(created_task_id)
            print("----------" * 10)

    def find_untouched_forms(self):
        for available_resource in self.available_resources:
            if available_resource['id'] in self.involved_task_ids:
                continue
            self.untouched_forms.append(available_resource['id'])

    def archive_tasks(self):
        for task_id in self.untouched_forms:
            task_data = requests.get(f"https://api.clickup.com/api/v2/task/{task_id}", headers=CLICKUP_HEADERS)
            task_creator_id = task_data.json()['creator']['id']
            if task_creator_id == 81706052:
                task_update_data = {"status": "finished"}
                close_task = requests.put(f"https://api.clickup.com/api/v2/task/{task_id}", headers=CLICKUP_HEADERS, json=task_update_data)
                print(f"Closing Task {task_id}. Status Code: {close_task.status_code}")
            else:
                try:
                    due_date = int(task_data.json()['due_date'])
                except:
                    due_date = None
                if not due_date:
                    continue
                timestamp_s = due_date / 1000
                dt = datetime.fromtimestamp(timestamp_s)
                today = datetime.now().date()
                yesterday = today - timedelta(days=1)
                is_yesterday_or_earlier = dt.date() <= yesterday
                if is_yesterday_or_earlier:
                    task_update_data = {"status": "finished"}
                    close_task = requests.put(f"https://api.clickup.com/api/v2/task/{task_id}", headers=CLICKUP_HEADERS,
                                              json=task_update_data)
                    print(f"Closing Task {task_id}. Status Code: {close_task.status_code}")

    def launch(self):
        self.get_available_resources()
        self.get_all_clickup_users()
        self.get_list_picklists()
        self.get_zp_employees()
        print(f"Cursor Date: {self.cursor_date}")
        print(f"Total Available Resources Cards: {len(self.available_resources)}")
        self.get_rm_forms()
        print(f"Total RM Forms: {len(self.rm_forms)}")
        self.get_clickup_blocks()
        print(f"Total Clickup Blocks: {len(self.clickup_blocks)}")
        self.prepare_clickup_blocks()
        self.create_or_update_block_by_rm_form()
        self.create_or_update_block_by_free_time()
        self.find_untouched_forms()
        print(f"Untouched Forms: {self.untouched_forms}")
        self.archive_tasks()


def available_resources_collector(cursor_date):
    available_resources_handler = AvailableResources(cursor_date)
    available_resources_handler.launch()


# available_resources_collector('2025-05-12')
