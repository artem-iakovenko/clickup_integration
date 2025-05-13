import time
import requests
from zoho_api.api import api_request
from help_functions import get_zp_employees
leave_sync_url = "https://www.zohoapis.com/crm/v7/functions/createleaveclickup/actions/execute?auth_type=apikey&zapikey=1003.4dc6e131901e2b9c2dc52b73bc81a5ad.b66d89f85e0370bb8fb7ba2a082271de"
relevant_departments = ['Development', 'Java Unit', "Business Analysts Team", "Project Management Team", "Partner's Devs", "Development Contractors"]


class LeaveSync:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.month_leaves = []
        self.zp_employees = get_zp_employees()

    def sync_leave(self, leave_id):
        post_data = {"leaveId": leave_id}
        response = requests.post(leave_sync_url, json=post_data)
        print(f"Status Code: {response.status_code}")
        return response.json()

    def launcher(self):
        s_index = 1
        month_leaves = {}
        while True:
            time.sleep(1)
            print(s_index)
            try:
                page_leaves = api_request(
                    f'https://people.zoho.com/api/v2/leavetracker/leaves/records?from={self.start_date}&to={self.end_date}&approvalStatus=["APPROVED"]&startIndex={s_index}&limit=200',
                    "zoho_people",
                    "get",
                    None
                )['records']
            except:
                page_leaves = []
            if not page_leaves:
                break
            month_leaves.update(page_leaves)
            s_index += 200

        print(f"Total Leaves: {len(list(month_leaves.keys()))}")
        input("Go?")
        for leave_id in list(month_leaves.keys()):
            leave_details = month_leaves[leave_id]
            employee_id = leave_details['Employee.ID']
            employee_email = None
            employee_department = None
            employee_team = None
            employee_staff_type = None
            for employee in self.zp_employees:
                if employee_id == employee['id']:
                    employee_email = employee['email']
                    employee_department = employee['department']
                    employee_team = employee['team']
                    employee_staff_type = employee['staff_type']
                    break
            print("-------" * 10)
            print(f"Email: {employee_email}")
            print(f"Staff Type: {employee_staff_type}")
            if employee_staff_type in ['Administrative team']:
                continue
            if employee_department and employee_department in relevant_departments:
                print(f"Leave ID: {leave_id}")
                print(f"Employee: {leave_details['Employee']}")
                print(f"Leave from {leave_details['From']} to {leave_details['To']}")
                response = self.sync_leave(leave_id)
                print(f"Response: {response}")
                time.sleep(5)


leave_handler = LeaveSync("2025-04-01", "2025-04-30")
leave_handler.launcher()
