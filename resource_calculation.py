import time
import requests
from integration_config import CLICKUP_HEADERS, RESOURCES_LIST_ID, date_ranges
from help_functions import str_to_unix, datetime_str_to_unix, str_to_date, get_working_days
from zoho_api.api import api_request
from datetime import datetime
INTERNAL_TASKS = []


class ResourceCalculation:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.filter_start = str(datetime_str_to_unix(start_date, 2, 0)) if start_date else None
        self.filter_end = str(datetime_str_to_unix(end_date, 22, 0)) if end_date else None
        self.month_blocks = []
        self.month_leaves = []
        self.active_dev_infos = []
        self.potentials = []
        self.developers = {}
        self.clickup_users = []
        self.zp_employees = []

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
                               'short_id': zp_employee_data['EmployeeID'], "crm_id": zp_employee_data['CRM_Developer_ID'], "joining_date": zp_employee_data["Dateofjoining"], "exit_date": zp_employee_data['Dateofexit']})
            s_index += 200

    def get_zp_data_by_crm_id(self, crm_id):
        for zp_employee in self.zp_employees:
            if zp_employee['crm_id'] == crm_id:
                return zp_employee

    def get_clickup_users(self):
        response = requests.get("https://api.clickup.com/api/v2/team/", headers=CLICKUP_HEADERS)
        return response.json()['teams'][0]['members']

    def get_potentials(self):
        cv_id = "1576533000417812003"
        page = 1
        while True:
            try:
                page_potentials = api_request(
                    f"https://www.zohoapis.com/crm/v2/Deals?cvid={cv_id}&page={page}",
                    "zoho_crm",
                    "get",
                    None
                )['data']
            except:
                break
            page += 1
            self.potentials.extend(page_potentials)

    def get_potential(self, potential_id):
        for potential in self.potentials:
            if potential['id'] == potential_id:
                return potential

    def get_active_devs(self):
        cv_id = "1576533000362341337"
        try:
            self.active_dev_infos = api_request(
                f"https://www.zohoapis.com/crm/v2/Project_Details?cvid={cv_id}",
                "zoho_crm",
                "get",
                None
            )['data']
        except:
            pass

    def get_developer(self, developer_id):
        if developer_id in self.developers:
            return self.developers[developer_id]
        response = api_request(
            f"https://www.zohoapis.com/crm/v2/Developers/{developer_id}",
            "zoho_crm",
            "get",
            None
        )['data'][0]
        self.developers[developer_id] = response
        return response

    def get_month_leaves(self):
        page = 0
        while True:
            resources_response = requests.get(
                f'https://api.clickup.com/api/v2/list/901204775879/task?page={page}&include_closed=true&due_date_lt={self.filter_end}&due_date_gt={self.filter_start}',
                headers=CLICKUP_HEADERS)
            page += 1
            if len(resources_response.json()['tasks']) == 0:
                break
            self.month_leaves.extend(resources_response.json()['tasks'])

    def get_month_blocks(self):
        page = 0
        while True:
            resources_response = requests.get(
                f'https://api.clickup.com/api/v2/list/901204980269/task?page={page}&include_closed=true&due_date_lt={self.filter_end}&due_date_gt={self.filter_start}',
                headers=CLICKUP_HEADERS)
            page += 1
            if len(resources_response.json()['tasks']) == 0:
                break
            self.month_blocks.extend(resources_response.json()['tasks'])


    def get_resources(self):
        all_resources = []
        page = 0
        while True:
            page_response = requests.get(
                f"https://api.clickup.com/api/v2/list/{RESOURCES_LIST_ID}/task?page={page}&due_date_gt={self.filter_start}&due_date_lt={self.filter_end}",
                headers=CLICKUP_HEADERS
            )
            if len(page_response.json()['tasks']) == 0:
                break
            all_resources.extend(page_response.json()['tasks'])
            page += 1
        return all_resources

    def get_task_by_id(self, task_id):
        time.sleep(1)
        response = requests.get(f"https://api.clickup.com/api/v2/task/{task_id}?include_subtasks=true", headers=CLICKUP_HEADERS)
        return response.json()

    def update_task(self, task_id, task_data, cf_fields):
        time.sleep(1)
        if task_data:
            response = requests.put(
                f"https://api.clickup.com/api/v2/task/{task_id}",
                headers=CLICKUP_HEADERS,
                json=task_data
            )
            print(f"\tUpdating Task: {response.status_code}")
        if cf_fields:
            for cf_field in cf_fields:
                response = requests.post(
                    f"https://api.clickup.com/api/v2/task/{task_id}/field/{cf_field['id']}",
                    headers=CLICKUP_HEADERS,
                    json={"value": cf_field['value']}
                )
                print(f"\tUpdating Custom Field Value: {response.status_code}")

    def get_leave_details(self, leave_id):
        for leave in self.month_leaves:
            if leave['id'] == leave_id:
                return leave

    def get_block_details(self, block_id):
        for block in self.month_blocks:
            if block['id'] == block_id:
                return block

    def get_dev_info_details(self, dev_info_id):
        for dev_info in self.active_dev_infos:
            if dev_info['id'] == dev_info_id:
                return dev_info

    def delete_task(self, task_id):
        response = requests.delete(f"https://api.clickup.com/api/v2/task/{task_id}", headers=CLICKUP_HEADERS)
        print(f"\tDeleting Task Status: {response.status_code}")

    def launch(self, trigger_task_id):
        self.clickup_users = self.get_clickup_users()
        self.get_zp_employees()
        self.get_potentials()
        self.get_month_blocks()
        self.get_month_leaves()
        self.get_active_devs()
        internal_project = self.get_task_by_id("869775k91")
        internal_task_ids = ["8696eea8p", "869775k91", "869775k91"]
        internal_project_subtasks = internal_project['subtasks'] if 'subtasks' in internal_project else []
        for internal_project_subtask in internal_project_subtasks:
            internal_task_ids.append(internal_project_subtask['id'])
        if trigger_task_id:
            print("Launching for singular Resource")
            trigger_task_details = self.get_task_by_id(trigger_task_id)
            trigger_location = trigger_task_details['list']['id']
            if trigger_location == "901204775879":
                resource_cf_id = "9f9c873c-dea9-4f61-b427-00e92038e756"
            else:
                resource_cf_id = "e3b6c6ea-c8a4-4318-90f8-168f6b54307e"
            trigger_resource_id = None
            for custom_field in trigger_task_details['custom_fields']:
                if custom_field['id'] == resource_cf_id:
                    trigger_resource_id = custom_field['value'][0]['id']
            print(f"Trigger Resource ID: {trigger_resource_id}")
            resource_data = self.get_task_by_id(trigger_resource_id)
            resources = [resource_data]
        else:
            print("Launching for all Resources")
            resources = self.get_resources()

        counter = 0
        for resource in resources:
            counter += 1
            print("-------" * 20)
            print(f"{counter}/{len(resources)}. Resource Name: {resource['name']}")
            resource_id = resource['id']
            # if resource_id != "8698jxuhy":
            #     continue
            custom_fields = resource['custom_fields']
            month_hours = 0
            blocked_hours = 0
            blocking_task_ids = []
            leave_hours = 0
            leave_task_ids = []
            ut_billable_hours = 0
            ut_internal_hours = 0

            developer_url, resource_current_developer_id = None, None
            for custom_field in custom_fields:
                if custom_field['id'] == "3fac0ff8-6981-463a-b7e0-a375f86aed24" and 'value' in custom_field:
                    month_hours = round(float(custom_field['value']), 2)
                elif custom_field['id'] == "e3b6c6ea-c8a4-4318-90f8-168f6b54307e" and 'value' in custom_field:
                    for blocking_task in custom_field['value']:
                        blocking_task_ids.append(blocking_task['id'])
                elif custom_field['id'] == "9f9c873c-dea9-4f61-b427-00e92038e756" and 'value' in custom_field:
                    for leave_task in custom_field['value']:
                        leave_task_ids.append(leave_task['id'])
                elif custom_field['id'] == "e6b5529b-167f-45f5-998c-cbeece722706" and 'value' in custom_field:
                    developer_url = custom_field['value']
                elif custom_field['id'] == "912a953f-4c89-44cb-844d-603111aa7eb1" and 'value' in custom_field:
                    resource_current_developer_id = custom_field['value'][0]['id'] if custom_field['value'] else None

            dev_id = None
            if developer_url:
                try:
                    dev_id = developer_url.split("?")[0].split("/")[-1]
                except Exception as e:
                    print(e)
                    dev_id = None

            zp_employee_data = self.get_zp_data_by_crm_id(dev_id)
            dev_email = zp_employee_data['email'] if zp_employee_data else None

            clickup_developer_id = ""
            for clickup_user in self.clickup_users:
                if dev_email == clickup_user['user']['email']:
                    clickup_developer_id = clickup_user['user']['id']
                if clickup_developer_id:
                    break

            print(f"CURRENT RESOURCE DEVELOPER: {resource_current_developer_id}")
            print(f"CLICKUP DEVELOPER ID: {clickup_developer_id}")
            for leave_task_id in leave_task_ids:
                leave_data = self.get_leave_details(leave_task_id)
                if not leave_data:
                    leave_data = self.get_task_by_id(leave_task_id)
                for custom_field in leave_data['custom_fields']:
                    if custom_field['id'] == "5078d821-4695-4e09-ae6c-81e29081ef66":
                        leave_hours += round(float(custom_field['value']), 2)
            print(f"Leave Hours: {leave_hours}")
            print(f"Total Blocks Available: {len(blocking_task_ids)}")

            for blocking_task_id in blocking_task_ids:
                blocking_data = self.get_block_details(blocking_task_id)
                if not blocking_data:
                    blocking_data = self.get_task_by_id(blocking_task_id)
                start_date_unix = int(blocking_data['start_date'])
                due_date_unix = int(blocking_data['due_date'])
                start_date = datetime.utcfromtimestamp(start_date_unix / 1000).strftime('%Y-%m-%d')
                due_date = datetime.utcfromtimestamp(due_date_unix / 1000).strftime('%Y-%m-%d')
                crm_id = None

                current_am_id, current_developer_id = None, None
                for custom_field in blocking_data['custom_fields']:
                    if custom_field['id'] == "7084b6d7-c48a-4288-b779-35731156b2fa" and 'value' in custom_field:
                        dev_info_url = custom_field['value']
                        crm_id = dev_info_url.split("?")[0].replace("https://crm.zoho.com/crm/org55415226/tab/LinkingModule4/", "")
                    elif custom_field['id'] == "6400cee4-b94c-45a0-ac67-02ec18770c8e" and 'value' in custom_field:
                        current_am_id = custom_field['value'][0]['id'] if custom_field['value'] else None
                    elif custom_field['id'] == "912a953f-4c89-44cb-844d-603111aa7eb1" and 'value' in custom_field:
                        current_developer_id = custom_field['value'][0]['id'] if custom_field['value'] else None

                print(f"Zoho CRM Dev Info ID: {crm_id}")
                dev_info = None
                if crm_id:
                    dev_info = self.get_dev_info_details(crm_id)
                    if not dev_info:
                        print("requesting devionfo...")
                        try:
                            dev_info = api_request(
                                f"https://www.zohoapis.com/crm/v2/Project_Details/{crm_id}",
                                "zoho_crm",
                                "get",
                                None
                            )['data'][0]
                        except Exception as e:
                            print(e)
                            dev_info = None
                dev_info_status = dev_info['Status']
                if dev_info_status == "OnHold":
                    # input("OnHold. Need to delete")
                    self.delete_task(blocking_task_id)
                    continue

                potential_id = dev_info['Multi_Select_Lookup_1']['id'] if dev_info['Multi_Select_Lookup_1'] else None
                potential_details = self.get_potential(potential_id)

                if potential_id == "1576533000386486133":
                    project_manager_email = "valia@kitrum.com"
                else:
                    potential_delivery = potential_details['Potential_Delivery_Owner']
                    project_manager_email = f'{potential_delivery.replace(" ", ".")}@kitrum.com'.lower() if potential_delivery else None

                clickup_manager_id = ""
                for clickup_user in self.clickup_users:
                    if project_manager_email == clickup_user['user']['email']:
                        clickup_manager_id = clickup_user['user']['id']
                    if clickup_manager_id:
                        break

                # if not clickup_manager_id:
                #     input(f"Clickup Manager ID: {clickup_manager_id}")

                final_date_on_project = dev_info['Final_Date_on_Project'] if dev_info else None
                if final_date_on_project:
                    if str_to_date(final_date_on_project) >= str_to_date(start_date) and str_to_date(final_date_on_project) < str_to_date(due_date):
                        pass
                    elif str_to_date(final_date_on_project) < str_to_date(start_date):
                        self.delete_task(blocking_task_id)
                        continue
                    else:
                        final_date_on_project = None

                # CHECK WORKLOAD
                is_full_time_blocking = False
                for custom_field in blocking_data['custom_fields']:
                    if custom_field['id'] == "66c97dab-0d09-4dc2-9416-ae666a6e6d42":
                        if custom_field['value'] == 0:
                            is_full_time_blocking = True
                            break
                print(f"Is Full Time Blocking: {is_full_time_blocking}")
                abh = 0
                blocking_hours = 0
                is_commercial = True
                for custom_field in blocking_data['custom_fields']:
                    # BLOCKED HOURS
                    if custom_field['id'] == "5078d821-4695-4e09-ae6c-81e29081ef66" and 'value' in custom_field:
                        blocking_hours = round(float(custom_field['value']), 2)
                    # AVAILABLE HOURS
                    elif custom_field['id'] == "9a832c69-edab-40eb-a81d-03be6078b0d9" and 'value' in custom_field:
                        abh = round(float(custom_field['value']), 2)
                    elif custom_field['id'] == "7f3b4b79-b252-42ea-83fc-8c59445148f9" and 'value' in custom_field:
                        related_project_id = custom_field['value'][0]['id']
                        if related_project_id in internal_task_ids:
                            is_commercial = False

                update_abh = False
                if is_full_time_blocking and final_date_on_project:
                    blocking_working_days = get_working_days(start_date, final_date_on_project)
                    blocking_working_hours = blocking_working_days * 8
                    if abh != blocking_working_hours:
                        abh = blocking_working_hours
                        update_abh = True

                if is_full_time_blocking:
                    planned = abh - leave_hours
                else:
                    planned = blocking_hours

                # UPDATE BLOCKING
                # update_main_data
                user_cfs = []
                # CHECK AM
                if clickup_manager_id and clickup_manager_id != current_am_id:
                    user_cfs.append({"id": "6400cee4-b94c-45a0-ac67-02ec18770c8e", "value": {"add": [clickup_manager_id], "rem": [current_am_id]}})
                # elif not clickup_manager_id and current_developer_id:
                #     user_cfs.append({"id": "6400cee4-b94c-45a0-ac67-02ec18770c8e", "value": {"add": [], "rem": [current_am_id]}})

                # CHECK DEV
                if clickup_developer_id and clickup_developer_id != current_developer_id:
                    user_cfs.append({"id": "912a953f-4c89-44cb-844d-603111aa7eb1", "value": {"add": [clickup_developer_id], "rem": [current_developer_id]}})
                elif not clickup_developer_id and current_developer_id:
                    user_cfs.append({"id": "912a953f-4c89-44cb-844d-603111aa7eb1", "value": {"add": [], "rem": [current_developer_id]}})

                if user_cfs:
                    self.update_task(blocking_task_id, None, user_cfs)

                if update_abh:
                    blocking_cf_update = [{"id": "9a832c69-edab-40eb-a81d-03be6078b0d9", "value": str(abh)}]
                    self.update_task(blocking_task_id, None, blocking_cf_update)
                if final_date_on_project:
                    self.update_task(blocking_task_id, {"due_date": str_to_unix(final_date_on_project)}, None)
                if planned != blocking_hours:
                    blocking_cf_update = [{"id": "5078d821-4695-4e09-ae6c-81e29081ef66", "value": str(planned)}]
                    self.update_task(blocking_task_id, None, blocking_cf_update)

                blocked_hours += planned

                if is_commercial:
                    ut_billable_hours += planned
                else:
                    ut_internal_hours += planned

            free_hours = month_hours - blocked_hours - leave_hours
            # print(f"BILLABLE HOURS: {ut_billable_hours}")
            # print(f"MONTH HOURS: {month_hours}")
            # print(f"INTERNAL HOURS: {ut_internal_hours}")
            # print(f"LEAVE HOURS: {leave_hours}")
            # time.sleep(11111)
            try:
                utilization_percent = round(ut_billable_hours / (month_hours - ut_internal_hours - leave_hours) * 100, 2)
            except ZeroDivisionError:
                utilization_percent = 0

            # REMOVE BLOCKING IF EXIT DATE FROM KITRUM IS IN THE PAST
            if not blocking_task_ids:
                # input(f"There are no blocking assigned to resource {resource_id}")
                try:
                    developer_id = developer_url.replace("https://crm.zoho.com/crm/org55415226/tab/CustomModule1/", "").split("?")[0]
                    developer_details = self.get_developer(developer_id)
                    exit_date = developer_details['Exit_Date']
                    if exit_date and str_to_date(exit_date) < str_to_date(self.start_date):
                        # input(f"Delete Resource ID: {resource_id}")
                        self.delete_task(resource_id)
                        continue
                except:
                    pass

            custom_fields = [
                {"id": "cf0b9445-8383-4d93-bc56-52a2c2c551b7", "value": str(free_hours)},
                {"id": "703dd683-1188-4427-a1a3-205733badd3f", "value": str(utilization_percent)},
            ]

            if clickup_developer_id and clickup_developer_id != resource_current_developer_id:
                custom_fields.append({"id": "912a953f-4c89-44cb-844d-603111aa7eb1",
                                 "value": {"add": [clickup_developer_id], "rem": [resource_current_developer_id]}})

            elif not clickup_developer_id and resource_current_developer_id:
                custom_fields.append(
                    {"id": "912a953f-4c89-44cb-844d-603111aa7eb1", "value": {"add": [], "rem": [resource_current_developer_id]}})
            self.update_task(resource_id, None, custom_fields)


def resource_calculator(month_start_date, month_end_date):
    resource_calculation = ResourceCalculation(month_start_date, month_end_date)
    resource_calculation.launch(None)


# for date_range in date_ranges:
#     print("-----" * 10)
#     print(date_range)
#     resource_calculator(date_range['start'], date_range['end'])
#     time.sleep(500)


# def single_resource_calculation(blocking_id):
#     print(blocking_id)
#     resource_calculation = ResourceCalculation(None, None)
#     resource_calculation.launch(blocking_id)
#
#
# single_resource_calculation("8698rmq0z")


