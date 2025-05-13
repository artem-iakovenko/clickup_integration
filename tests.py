import time
import requests
from zoho_api.api import api_request
from integration_config import CLICKUP_HEADERS
from help_functions import str_to_str_date, datetime_str_to_unix


class ResourceBlocking:
    def __init__(self, start_date, end_date, developer_info_id, mode):
        self.month_resources = []
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
        self.zp_employees = []

    def get_month_resources(self):
        page = 0
        while True:
            resources_response = requests.get(
                f'https://api.clickup.com/api/v2/list/901204930768/task?page={page}&include_closed=true&due_date_lt={self.filter_end}&due_date_gt={self.filter_start}',
                headers=CLICKUP_HEADERS)
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
        except Exception as e:
            print(e)
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

    def get_or_create_reource_card(self, developer_id, clickup_user_id, resource_card_data):
        if developer_id in self.resource_cards:
            return self.resource_cards[developer_id]

        resources_response = requests.get(
            'https://api.clickup.com/api/v2/list/901204930768/task?include_closed=true&custom_fields=[{"field_id": "912a953f-4c89-44cb-844d-603111aa7eb1","operator":"ANY","value":[' + str(
                clickup_user_id) + ']}]', headers=CLICKUP_HEADERS)
        existing_resource_id = None
        for resource in resources_response.json()['tasks']:
            if str_to_str_date(self.month_end_date) in resource['name']:
                existing_resource_id = resource['id']

        if existing_resource_id:
            return existing_resource_id

        print("Creating Resource Card")
        response = requests.post("https://api.clickup.com/api/v2/list/901204930768/task", headers=CLICKUP_HEADERS,
                                 json=resource_card_data)
        if response.status_code == 200:
            self.resource_cards[developer_id] = response.json()['id']
            time.sleep(1)
            return response.json()['id']
        return None

    def check_if_blocking_exist(self, clickup_user_id, clickup_project_id):
        time.sleep(1)
        response = requests.get(
            'https://api.clickup.com/api/v2/list/901204980269/task?due_date_lt=' + self.filter_end + '&due_date_gt=' + self.filter_start + '&include_closed=true&custom_fields=[{"field_id": "912a953f-4c89-44cb-844d-603111aa7eb1","operator":"ANY","value":[' + str(
                clickup_user_id) + ']}]', headers=CLICKUP_HEADERS)
        blocking_exist = False
        for blocking in response.json()['tasks']:
            blocking_cfs = blocking['custom_fields']
            for blocking_cf in blocking_cfs:
                if blocking_cf['id'] == "7f3b4b79-b252-42ea-83fc-8c59445148f9":
                    if blocking_cf['value'][0]['id'] == clickup_project_id:
                        return True
        return blocking_exist

    def create_blocking_task(self, task_data):
        time.sleep(1)
        response = requests.post("https://api.clickup.com/api/v2/list/901204980269/task", headers=CLICKUP_HEADERS,
                                 json=task_data)
        return response.json()['id'] if response.status_code == 200 else None

    def assign_leaves(self, clickup_user_id, resource_card_id):
        print("Checking Leaves")
        time.sleep(1)
        response = requests.get(
            'https://api.clickup.com/api/v2/list/901204775879/task?due_date_lt=' + self.filter_end + '&due_date_gt=' + self.filter_start + '&include_closed=true&custom_fields=[{"field_id": "912a953f-4c89-44cb-844d-603111aa7eb1","operator":"ANY","value":[' + str(
                clickup_user_id) + ']}]', headers=CLICKUP_HEADERS)
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
                link_leave = requests.post(
                    f"https://api.clickup.com/api/v2/task/{leave_task_id}/field/9f9c873c-dea9-4f61-b427-00e92038e756",
                    headers=CLICKUP_HEADERS, json=link_data)
                print(f"\tLinking Leave to resource: {link_leave.status_code}")

    def get_zp_employee(self, developer_id):
        try:
            response = api_request(
                "https://people.zoho.com/people/api/forms/employee/getRecords?searchParams={searchField: 'CRM_Developer_ID', searchOperator: 'Is', searchText : '" + developer_id + "'}",
                "zoho_people",
                "get",
                None
            )['response']['result'][0]
            employee_id = list(response.keys())[0]
            return response[employee_id][0]['EmailID']
        except:
            return None

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
            except Exception as e:
                print(e)
        return None

    def get_hpd(self, dev_info_id):
        hpd = 8
        try:
            dev_info_notes = api_request(
                f"https://www.zohoapis.com/crm/v2/Project_Details/{dev_info_id}/Notes",
                "zoho_crm",
                "get",
                None
            )['data']
            for dev_info_note in dev_info_notes:
                if '6 hours per day' in dev_info_note['Note_Content']:
                    hpd = 6
        except:
            print("Error Occured while fetching dev info notes")
        return hpd


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


    def get_zp_data_by_email(self, email):
        for zp_employee in self.zp_employees:
            if zp_employee['email'] == email:
                return zp_employee

    def launch(self):
        # self.get_month_resources()
        self.get_month_blocks()
        active_devs = self.get_active_devs()
        print(len(active_devs))
        # self.get_zp_employees()
        # self.clickup_users = self.get_clickup_users()

        for block in self.month_blocks:
            block_id = block['id']
            print(f'Block ID: {block_id}')
            dev_name = block['name'].split(" - ")[0]
            project_name = block['name'].replace(f"{dev_name} - ", "")

            dev_info_id = None
            for active_dev in active_devs:
                if active_dev['Developers_on_project']['name'] == dev_name and active_dev['Multi_Select_Lookup_1']['name'] == project_name:
                    dev_info_id = active_dev['id']
            if dev_info_id:
                dev_info_url = f"https://crm.zoho.com/crm/org55415226/tab/LinkingModule4/{dev_info_id}"
                cf_data = {"value": dev_info_url}
                response = requests.post(
                    f"https://api.clickup.com/api/v2/task/{block_id}/field/7084b6d7-c48a-4288-b779-35731156b2fa",
                    headers=CLICKUP_HEADERS, json=cf_data)
                print(response.status_code)
            else:
                print('err')


        # for resource in self.month_resources:
        #     print('------------------------')
        #     resource_id = resource['id']
        #     print(resource_id)
        #     resource_cfs = resource['custom_fields']
        #     developer_email = None
        #     for resource_cf in resource_cfs:
        #         if resource_cf['id'] == "912a953f-4c89-44cb-844d-603111aa7eb1" and 'value' in resource_cf:
        #             developer_email = resource_cf['value'][0]['email']
        #     try:
        #         crm_id = self.get_zp_data_by_email(developer_email)['crm_id']
        #         developer_url = f"https://crm.zoho.com/crm/org55415226/tab/CustomModule1/{crm_id}"
        #     except:
        #         input("err")
        #         continue
        #     cf_data = {"value": developer_url}
        #     response = requests.post(
        #         f"https://api.clickup.com/api/v2/task/{resource_id}/field/e6b5529b-167f-45f5-998c-cbeece722706",
        #         headers=CLICKUP_HEADERS, json=cf_data)
        #     print(response.status_code)



def main(start_date, end_date, dev_info_id):
    resource_blocking_handler = ResourceBlocking(start_date, end_date, dev_info_id, 'monthly')
    resource_blocking_handler.launch()


date_ranges = [
    {'start': '2025-03-01', 'end': '2025-03-31'}
    # {'start': '2025-04-01', 'end': '2025-04-30'}
]

for date_range in date_ranges:
    print("==========" * 15)
    print(f"CURRENT CONFIG: {date_range}\n")
    main(date_range['start'], date_range['end'], "1576533000403724461")
    time.sleep(5)









