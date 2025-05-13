import time
import requests
from zoho_api.api import api_request
from integration_config import CLICKUP_HEADERS, type_mapping, workload_mapping, date_ranges
from help_functions import str_to_unix, str_to_date, str_to_str_date, get_working_days, datetime_str_to_unix


class ResourceBlocking:
    def __init__(self, start_date, end_date, developer_info_id, mode):
        self.zp_employees = []
        self.month_resources = []
        self.created_resources = {}
        self.month_blocks = []
        self.potentials = {}
        self.developers = {}
        self.clickup_users = []
        self.resource_cards = {}
        self.month_start_date = start_date
        self.month_end_date = end_date
        self.filter_start = str(datetime_str_to_unix(start_date, 2, 0))
        self.filter_end = str(datetime_str_to_unix(end_date, 22, 0))
        self.developer_info_id = developer_info_id
        self.mode = mode

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

    def get_month_resources(self):
        page = 0
        while True:
            resources_response = requests.get(f'https://api.clickup.com/api/v2/list/901204930768/task?page={page}&include_closed=true&due_date_lt={self.filter_end}&due_date_gt={self.filter_start}', headers=CLICKUP_HEADERS)
            page += 1
            if len(resources_response.json()['tasks']) == 0:
                break
            self.month_resources.extend(resources_response.json()['tasks'])

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

    def get_active_devs(self):
        cv_id = "1576533000398035053" if self.mode == "daily" else "1576533000362341337"
        try:
            return api_request(
                f"https://www.zohoapis.com/crm/v2/Project_Details?cvid={cv_id}",
                "zoho_crm",
                "get",
                None
            )['data']
        except:
            return []

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

    def get_potential(self, potential_id):
        if potential_id in self.potentials:
            return self.potentials[potential_id]
        response = api_request(
            f"https://www.zohoapis.com/crm/v2/Deals/{potential_id}",
            "zoho_crm",
            "get",
            None
        )['data'][0]
        self.potentials[potential_id] = response
        return response

    def get_clickup_users(self):
        response = requests.get("https://api.clickup.com/api/v2/team/", headers=CLICKUP_HEADERS)
        return response.json()['teams'][0]['members']



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

    def get_or_create_resource_card(self, developer_id, resource_card_data, update_resource):
        if developer_id in self.created_resources:
            print("RESOURCE EXIST: TRUE")
            return self.created_resources[developer_id]
        for resource in self.month_resources:
            resource_cfs = resource['custom_fields']
            for resource_cf in resource_cfs:
                if resource_cf['id'] == 'e6b5529b-167f-45f5-998c-cbeece722706' and 'value' in resource_cf:
                    if developer_id in resource_cf['value']:
                        print("RESOURCE EXIST: TRUE")
                        if update_resource:
                            task_data = {"due_date": resource_card_data['due_date']}
                            cf_fields = []
                            for custom_field in resource_card_data['custom_fields']:
                                if custom_field['id'] in ["3fac0ff8-6981-463a-b7e0-a375f86aed24"]:
                                    cf_fields.append(custom_field)
                            print("Updating Reosource Card...")
                            self.update_task(resource['id'], task_data, cf_fields)
                        return resource['id']
        print("RESOURCE EXIST: FALSE")
        response = requests.post("https://api.clickup.com/api/v2/list/901204930768/task", headers=CLICKUP_HEADERS, json=resource_card_data)
        if response.status_code == 200:
            self.resource_cards[developer_id] = response.json()['id']
            time.sleep(1)
            self.assign_leaves(developer_id, response.json()['id'])
            self.created_resources[developer_id] = response.json()['id']
            return response.json()['id']
        return None

    def check_if_blocking_exist(self, dev_info_id):
        for block in self.month_blocks:
            block_cfs = block['custom_fields']
            for block_cf in block_cfs:
                if block_cf['id'] == '7084b6d7-c48a-4288-b779-35731156b2fa' and 'value' in block_cf:
                    if dev_info_id in block_cf['value']:
                        print("BLOCKING EXIST: TRUE")
                        return True
        print("BLOCKING EXIST: FALSE")
        return False

    def create_blocking_task(self, task_data):
        time.sleep(1)
        response = requests.post("https://api.clickup.com/api/v2/list/901204980269/task", headers=CLICKUP_HEADERS, json=task_data)
        return response.json()['id'] if response.status_code == 200 else None

    def assign_leaves(self, developer_id, resource_card_id):
        print("Assigning Leaves...")
        time.sleep(1)
        developer_url = f"https://crm.zoho.com/crm/org55415226/tab/CustomModule1/{developer_id}"
        # response = requests.get('https://api.clickup.com/api/v2/list/901204775879/task?due_date_lt=' + self.filter_end + '&due_date_gt=' + self.filter_start + '&include_closed=true&custom_fields=[{"field_id": "912a953f-4c89-44cb-844d-603111aa7eb1","operator":"ANY","value":[' + str(clickup_user_id) + ']}]', headers=CLICKUP_HEADERS)
        response = requests.get('https://api.clickup.com/api/v2/list/901204775879/task?due_date_lt=' + self.filter_end + '&due_date_gt=' + self.filter_start + '&include_closed=true&custom_fields=[{"field_id": "e6b5529b-167f-45f5-998c-cbeece722706","operator":"=","value": "' + developer_url + '"}]', headers=CLICKUP_HEADERS)
        for leave_task in response.json()['tasks']:
            leave_task_id = leave_task['id']
            leave_cfs = leave_task['custom_fields']
            cf_is_set = False

            for leave_cf in leave_cfs:
                if leave_cf['id'] == "9f9c873c-dea9-4f61-b427-00e92038e756":
                    if leave_cf['value'] if 'value' in leave_cf else False:
                        cf_is_set = True
                    break
            if not cf_is_set:
                link_data = {"value": {"add": [resource_card_id], "rem": []}}
                link_leave = requests.post(f"https://api.clickup.com/api/v2/task/{leave_task_id}/field/9f9c873c-dea9-4f61-b427-00e92038e756", headers=CLICKUP_HEADERS, json=link_data)
                print(f"\tLinking Leave to resource: {link_leave.status_code}")

    def get_list_custom_field(self, list_id):
        response = requests.get("https://api.clickup.com/api/v2/list/" + list_id + "/field", headers=CLICKUP_HEADERS)
        return response.json()['fields']

    def get_cf_option_id(self, custom_fields, custom_field_id, search_value):
        for custom_field in custom_fields:
            try:
                if custom_field['id'] != custom_field_id:
                    continue
                custom_field_options = custom_field['type_config']['options']
                for custom_field_option in custom_field_options:
                    if custom_field_option['name'] == search_value:
                        return custom_field_option['id']
            except:
                pass
        return None

    def get_hpd(self, dev_info_id, developer_id):
        hpd = 6 if developer_id in ['1576533000099422137'] else 8
        # try:
        #     dev_info_notes = api_request(
        #         f"https://www.zohoapis.com/crm/v2/Project_Details/{dev_info_id}/Notes",
        #         "zoho_crm",
        #         "get",
        #         None
        #     )['data']
        #     for dev_info_note in dev_info_notes:
        #         if '6 hours per day' in dev_info_note['Note_Content']:
        #             hpd = 6
        # except:
        #     pass
        return hpd

    def get_zp_data_by_crm_id(self, crm_id):
        for zp_employee in self.zp_employees:
            if zp_employee['crm_id'] == crm_id:
                return zp_employee

    def launch(self):
        self.get_zp_employees()
        self.get_month_resources()
        self.get_month_blocks()
        self.clickup_users = self.get_clickup_users()
        active_developers = self.get_active_devs()
        print(f"Total Active Developers Info Available: {len(active_developers)}")

        resource_available_cfs = self.get_list_custom_field("901204930768")
        # blocking_available_cfs = self.get_list_custom_field("901204980269")
        counter = 0
        for active_developer in active_developers:
            counter += 1
            dev_info_id = active_developer['id']
            if self.developer_info_id:
                if dev_info_id != self.developer_info_id:
                    continue

            print("--------------" * 10)
            print(f"{counter}/{len(active_developers)}. Developer Info ID: {dev_info_id}")
            # input("go?")

            # GET DEVELOPER AND POTENTIAL + ALL NEEDE FIELDS
            vendor_name = active_developer['Vendor_Name']['name']
            developer, potential = active_developer['Developers_on_project'], active_developer['Multi_Select_Lookup_1']
            developer_status = active_developer['Status']
            developer_id, potential_id = developer['id'], potential['id']
            # REMOVe this
            # if developer_id != "1576533000070249036":
            #     continue
            zp_employee_data = self.get_zp_data_by_crm_id(developer_id)

            # Check Hours Per Day for Custom Cases
            hpd = self.get_hpd(dev_info_id, developer_id)

            developer_name = developer['name']
            developer_details = self.get_developer(developer_id)
            potential_details = self.get_potential(potential_id)
            type_of_member = developer_details['Type_of_member']

            # RECORD URLS
            dev_info_url = f"https://crm.zoho.com/crm/org55415226/tab/LinkingModule4/{dev_info_id}"
            developer_url = f"https://crm.zoho.com/crm/org55415226/tab/CustomModule1/{developer_id}"

            print(f"Developer Name: {developer_name} - {type_of_member}")
            print(f"Project Name: {potential['name']}")
            print(f"Vendor Name: {vendor_name}")

            seniority = developer_details["Seniority"]
            direction = developer_details["Direction"]
            title = developer_details["Title"]
            potential_delivery = potential_details['Potential_Delivery_Owner']

            start_at_kitrum = zp_employee_data["joining_date"] if zp_employee_data else None or developer_details['Start_Date_at_KITRUM']
            end_at_kitrum = zp_employee_data['exit_date'] if zp_employee_data else developer_details['Exit_Date']
            print(f"Company Date Range: {start_at_kitrum} - {end_at_kitrum or 'present'}")

            # GET DATES FOR RESOURCE CARD
            resource_start_date = self.month_start_date
            resource_end_date = self.month_end_date

            if start_at_kitrum:
                if str_to_date(start_at_kitrum) > str_to_date(self.month_start_date) and str_to_date(start_at_kitrum) < str_to_date(self.month_end_date):
                    resource_start_date = start_at_kitrum

            if end_at_kitrum:
                if str_to_date(end_at_kitrum) < str_to_date(self.month_start_date):
                    continue
                if str_to_date(end_at_kitrum) > str_to_date(self.month_start_date) and str_to_date(end_at_kitrum) < str_to_date(self.month_end_date):
                    resource_end_date = end_at_kitrum

            if potential_id == "1576533000386486133":
                project_manager_email = "valia@kitrum.com"
            else:
                project_manager_email = f'{potential_delivery.replace(" ", ".")}@kitrum.com'.lower() if potential_delivery else None

            workload = active_developer['Workload']
            estimated_hours = active_developer['Number_of_hours'] or 0
            start_on_project = active_developer['Start_Date_on_Project']
            end_on_project = active_developer['Final_Date_on_Project']
            print(f"Project Date Range: {start_on_project} - {end_on_project or 'present'}")

            # GET BLOCKINGS Start and End Date
            blocking_start_date = self.month_start_date
            blocking_end_date = self.month_end_date

            if start_on_project:
                if str_to_date(start_on_project) > str_to_date(self.month_start_date) and str_to_date(start_on_project) < str_to_date(self.month_end_date):
                    blocking_start_date = start_on_project
            if end_on_project:
                if str_to_date(end_on_project) < str_to_date(self.month_start_date):
                    continue
                if str_to_date(end_on_project) > str_to_date(self.month_start_date) and str_to_date(end_on_project) < str_to_date(self.month_end_date):
                    blocking_end_date = end_on_project

            resource_working_days = get_working_days(resource_start_date, resource_end_date)
            resource_working_hours = resource_working_days * hpd

            print(f"Resource Hours: {resource_working_hours}")

            project_clickup_id = potential_details['ClickUp_ID']
            developer_email = zp_employee_data['email'] if zp_employee_data else active_developer['Email']

            if workload == "Full-time":
                available_hours = get_working_days(blocking_start_date, blocking_end_date) * 8
            else:
                if estimated_hours <= 0:
                    continue
                available_hours = estimated_hours

            # FIND CLICKUP USER ID
            clickup_user_id, clickup_manager_id = "", ""
            for clickup_user in self.clickup_users:
                if developer_email == clickup_user['user']['email']:
                    clickup_user_id = clickup_user['user']['id']
                if project_manager_email == clickup_user['user']['email']:
                    clickup_manager_id = clickup_user['user']['id']
                if clickup_manager_id and clickup_user_id:
                    break

            print(f"Clickup User ID: {clickup_user_id}")

            # FILL RESOURCE CARD CUSTOM FIELDS
            clickup_resource_custom_fields = [
                {"id": "912a953f-4c89-44cb-844d-603111aa7eb1", "value": {"add": [clickup_user_id], "rem": []}},
                {"id": "031efcab-a89c-4f7f-bb03-208b209943a9", "value": type_mapping[type_of_member]},
                {"id": "3fac0ff8-6981-463a-b7e0-a375f86aed24", "value": str(resource_working_hours)},
                {"id": "e6b5529b-167f-45f5-998c-cbeece722706", "value": developer_url}
            ]

            seniority_option_id = self.get_cf_option_id(resource_available_cfs, "006faef7-4e5a-41c7-8b54-a8ed2665bb70", seniority)
            if seniority_option_id:
                clickup_resource_custom_fields.append({"id": "006faef7-4e5a-41c7-8b54-a8ed2665bb70", "value": seniority_option_id})
            direction_option_id = self.get_cf_option_id(resource_available_cfs, "baf5146f-534c-44e0-9b9d-f84329154369", direction)
            if direction_option_id:
                clickup_resource_custom_fields.append({"id": "baf5146f-534c-44e0-9b9d-f84329154369", "value": direction_option_id})
            title_option_id = self.get_cf_option_id(resource_available_cfs, "33cc9332-2bda-43e3-97fc-1131c8a0d5ee", title)
            if title_option_id:
                clickup_resource_custom_fields.append({"id": "33cc9332-2bda-43e3-97fc-1131c8a0d5ee", "value": title_option_id})

            # FILL RESOURCE CARD DATA
            clickup_resource_data = {
                "name": f"{developer['name']} - {str_to_str_date(self.month_end_date)}",
                "status": "work on project",
                # "start_date": str_to_unix(self.month_start_date),
                "start_date": str_to_unix(resource_start_date),
                "due_date": str_to_unix(resource_end_date),
                "custom_item_id": 1001,
                "custom_fields": clickup_resource_custom_fields
            }

            update_resource = False
            if end_at_kitrum and str_to_date(self.month_start_date) < str_to_date(end_at_kitrum) < str_to_date(self.month_end_date):
                update_resource = True

            resource_card_id = self.get_or_create_resource_card(developer_id, clickup_resource_data, update_resource)
            blocking_name = f"{developer['name']} - {potential['name']}"
            blocking_exist = self.check_if_blocking_exist(dev_info_id)

            if blocking_exist:
                continue
            if developer_status in ['OnHold']:
                # input(f"Developer is: {developer_status}")
                continue

            clickup_blocking_custom_fields = [
                {"id": "031efcab-a89c-4f7f-bb03-208b209943a9", "value": type_mapping[type_of_member]},
                {"id": "66c97dab-0d09-4dc2-9416-ae666a6e6d42", "value": workload_mapping[workload]},
                {"id": "912a953f-4c89-44cb-844d-603111aa7eb1", "value": {"add": [clickup_user_id], "rem": []}},
                {"id": "6400cee4-b94c-45a0-ac67-02ec18770c8e", "value": {"add": [clickup_manager_id], "rem": []}},
                {"id":  "5078d821-4695-4e09-ae6c-81e29081ef66", "value": str(available_hours)},
                {"id": "9a832c69-edab-40eb-a81d-03be6078b0d9", "value": str(available_hours)},
                {"id": "e3b6c6ea-c8a4-4318-90f8-168f6b54307e", "value": {"add": [resource_card_id], "rem": []}},
                {"id": "7f3b4b79-b252-42ea-83fc-8c59445148f9", "value": {"add": [project_clickup_id], "rem": []}},
                {"id": "7084b6d7-c48a-4288-b779-35731156b2fa", "value": dev_info_url}
            ]
            if seniority_option_id:
                clickup_blocking_custom_fields.append({"id": "006faef7-4e5a-41c7-8b54-a8ed2665bb70", "value": seniority_option_id})
            if direction_option_id:
                clickup_blocking_custom_fields.append({"id": "baf5146f-534c-44e0-9b9d-f84329154369", "value": direction_option_id})
            if title_option_id:
                clickup_blocking_custom_fields.append({"id": "33cc9332-2bda-43e3-97fc-1131c8a0d5ee", "value": title_option_id})

            clickup_blocking_data = {
                "name": blocking_name,
                "status": "in progress",
                "start_date": str_to_unix(blocking_start_date),
                "due_date": str_to_unix(blocking_end_date),
                "custom_item_id": 1006,
                "custom_fields": clickup_blocking_custom_fields
            }
            blocking_task_id = self.create_blocking_task(clickup_blocking_data)
            print(f"BLOCKING FOR {developer['name']} - {potential['name']}: {blocking_task_id}")


def resource_blocker(start_date, end_date, dev_info_id):
    resource_blocking_handler = ResourceBlocking(start_date, end_date, dev_info_id, 'monthly')
    resource_blocking_handler.launch()


# for date_range in date_ranges:
#     print("==========" * 15)
#     print(f"CURRENT CONFIG: {date_range}\n")
#     resource_blocker(date_range['start'], date_range['end'], None)
#     time.sleep(5)









