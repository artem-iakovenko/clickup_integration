import time

from zoho_api.api import api_request


class TimesheetsSubmit:
    def __init__(self, start_date, end_date, mode, clickup_project_id, user_email):
        self.start_date = start_date
        self.end_date = end_date
        self.zp_logs = []
        self.mode = mode
        self.clickup_project_id = clickup_project_id
        self.user_email = user_email
        self.zp_project_id = ""
        self.timesheets_by_user = {}
        self.timesheet_name = f"Timesheet ({start_date} - {end_date})"

    def get_zp_project(self):
        zp_project_response = api_request(
            "https://people.zoho.com/people/api/forms/P_TimesheetJobsList/getRecords?searchParams={searchField: 'Clickup_ID', searchOperator: 'Is', searchText : '" + self.clickup_project_id + "'}",
            "zoho_people", "get", None)['response']['result'][0]
        self.zp_project_id = list(zp_project_response.keys())[0]
        self.zp_project = zp_project_response[self.zp_project_id][0]

    def get_zp_logs(self):
        all_logs = []
        date_ranges = [
            {'from': self.start_date, 'to': self.end_date}
        ]
        for date_range in date_ranges:
            s_index = 1
            while True:
                if self.mode == "project":
                    page_logs_response = api_request(
                        f"https://people.zoho.com/people/api/timetracker/gettimelogs?fromDate={date_range['from']}&toDate={date_range['to']}&billingStatus=all&user=all&projectId={self.zp_project_id}&sIndex={s_index}&limit=200",
                        "zoho_people",
                        "get",
                        None
                    )
                elif self.mode == "user":
                    page_logs_response = api_request(
                        f"https://people.zoho.com/people/api/timetracker/gettimelogs?fromDate={date_range['from']}&toDate={date_range['to']}&billingStatus=all&user={self.user_email}&sIndex={s_index}&limit=200",
                        "zoho_people",
                        "get",
                        None
                    )
                page_logs = page_logs_response['response']['result']
                if not page_logs:
                    break
                all_logs.extend(page_logs)
                s_index += 200
        self.zp_logs = all_logs

    def prepare_timesheets(self):
        for zp_log in self.zp_logs:
            approval_status = zp_log['approvalStatus']
            if approval_status != "notsubmitted":
                continue
            user_email = zp_log['employeeMailId']
            project_id = zp_log['projectId']
            try:
                clickup_log_id = int(zp_log['description'])
                is_id = True
            except:
                is_id = False
            if is_id and len(zp_log['description']) == 19:
                pass
            else:
                continue
            if user_email in self.timesheets_by_user:
                if project_id not in self.timesheets_by_user[user_email]:
                    self.timesheets_by_user[user_email].append(project_id)
            else:
                self.timesheets_by_user[user_email] = [project_id]

    def submit_timesheets(self):
        user_emails = list(self.timesheets_by_user.keys())
        for user_email in user_emails:
            print(f"Current User: {user_email}")
            user_projects = self.timesheets_by_user[user_email]
            for user_project in user_projects:
                time.sleep(3)
                print(user_project)
                project_details = api_request(
                    f"https://people.zoho.com/people/api/timetracker/getprojectdetails?projectId={user_project}",
                    "zoho_people",
                    "get",
                    None
                )
                client_id = str(project_details['response']['result'][0]['clientId'])
                print(f"\tProject ID: {user_project}")
                submit_url = f"https://people.zoho.com/people/api/timetracker/createtimesheet?user={user_email}&timesheetName={self.timesheet_name}&fromDate={self.start_date}&toDate={self.end_date}&billableStatus=all&projectId={user_project}&sendforApproval=true&approvalStatus=approved"

                create_timesheet = api_request(submit_url, "zoho_people", "post", None)
                time.sleep(1)
                timesheet_id = create_timesheet['response']['result']['timesheetId'][0]
                approve_timesheet = "No need"
                if client_id != "378942000000199393":
                    approval_url = f'https://people.zoho.com/people/api/timetracker/approvetimesheet?timesheetId={timesheet_id}&approvalStatus=approved&isAllLevelApprove=true'
                    approve_timesheet = api_request(approval_url, "zoho_people", "post", None)
                time.sleep(1)
                print(f"\tSubmission Status: {create_timesheet}")
                print(f"\tApproval Status: {approve_timesheet}")




    def launcher(self):
        if self.mode == "project":
            self.get_zp_project()
        self.get_zp_logs()
        self.prepare_timesheets()
        print(self.timesheets_by_user)
        self.submit_timesheets()


def main():
    start_date = "2025-04-01"
    end_date = "2025-04-30"
    users = [
        'pavlo.kotelnytskyi@kitrum.com',
        'oleh.motrunych@kitrum.com',
        'paulo.araujo@kitrum.com',
        'olga.krekhovetska@kitrum.com',
        'yevhen.nikulin@kitrum.com',
        'aleksey.grigoriev@kitrum.com',
        'alex.sergeev@kitrum.com',
        'alexandr.lapin@kitrum.com',
        'andrei.stan@kitrum.com',
        'andrii.ashomok@kitrum.com',
        'andrii.makivnychuk@kitrum.com',
        'anxhelo.sako@kitrum.com',
        'artur.brutian@kitrum.com',
        'artur.kiiko@kitrum.com',
        'artur.mihura@kitrum.com',
        'artur.pazyniuk@kitrum.com',
        'artur.vitiuk@kitrum.com',
        'bohdan.zavhorodskyi@kitrum.com',
        'dauletkhan.tulendiyev@kitrum.com',
        'dmytro.lukianchikov@kitrum.com',
        'dmytro.timchenko@kitrum.com',
        'dmytro.usenko@kitrum.com',
        'evgeniy.kravets@kitrum.com',
        'farrukh.khamidov@kitrum.com',
        'gleb.cherkashyn@kitrum.com',
        'iryna.khoruz@kitrum.com',
        'ivan.kostromin@kitrum.com',
        'kostiantyn.horbach@kitrum.com',
        'kyrylo.sokolov@kitrum.com',
        'oleksandr.prylypko@kitrum.com',
        'pavlo.borshchenko@kitrum.com',
        'pedro.tiple@kitrum.com',
        'przemyslaw.przybyszewski@kitrum.com',
        'serhiy.tanchenets@kitrum.com',
        'stanislav.bytsko@kitrum.com',
        'tamta.khvichia@kitrum.com',
        'temirlan.tashbolotov@kitrum.com',
        'thomas.vandervoort@kitrum.com',
        'turgut.atashov@kitrum.com',
        'vander.nunes@kitrum.com',
        'vlada.biriuk@kitrum.com',
        'vyacheslav.buzivskyy@kitrum.com',
        'yevhenii.skrypnik@kitrum.com',
        'yuliia.pabot@kitrum.com',
        'yuriy.polishchuk@kitrum.com',
        'max.popovitch@kitrum.com',
        'olha.dzhychko@kitrum.com',
        'artem.iakovenko@kitrum.com',
        'katherine.hlotova@kitrum.com',
        'vladyslav.havrilov@kitrum.com',
        'elena.podpalchenko@kitrum.com',
        'eugene.sakun@kitrum.com',
        'andrii.tatarynov@kitrum.com',
        'roman.pomin@kitrum.com',
        'ivan@kitrum.com',
        'mykhailo.zhukovskyi@kitrum.com',
        'kateryna.lytvynova@kitrum.com'
    ]
    for user in users:
        print("=============" * 10)
        hanlder = TimesheetsSubmit(start_date, end_date, "user", "", user)
        hanlder.launcher()



main()