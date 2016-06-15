#!/usr/bin/env python
import csv
import json
import os
import requests
import sys

# Globals
parameters = {}
total_request_limit = 0
total_request_count = 0
script_path = os.path.dirname(os.path.realpath(__file__))
# *************CSV build functions*************


def get_auth():
    url = parameters["url_base"] + "/identity/oauth/token?"\
          + "client_id=" + parameters["client_id"]\
          + "&client_secret=" + parameters["client_secret"]\
          + "&grant_type=client_credentials"
    token = get_uri_data(url)
    return token


def execute_web_request(url):
    global total_request_count
    sys.stdout.write("\t Executing request " +
                     str(total_request_count) + 
                     ", uri: " + url + "\n") 
    response = requests.get(url)
    total_request_count += 1
    return response


def get_uri_data(url):
    global total_request_count
    global total_request_limit
    response = execute_web_request(url)
    payload = json.loads(response.content)
    if ("access_token" in payload.keys()):
        return payload["access_token"]
    # If initial payload is empty, return None.
    if ("result" not in payload.keys()):
        return None
    else:
        data = payload["result"]
    while ("nextPageToken" in payload.keys() and
           total_request_count < total_request_limit):
        response = execute_web_request(url +
                                       "&nextPageToken=" +
                                       payload["nextPageToken"])
        payload = json.loads(response.content)
        # If payload is empty, return what we've combined so far.
        if ("result" not in payload.keys()):
            break
        payload = json.loads(response.content)
        for item in payload["result"]:
            data.append(item)
    return data


def fix_dates(item):
    # Format date for redshift
    item["createdAt"] = item["createdAt"].replace("T", " ").replace("Z", "")
    item["updatedAt"] = item["updatedAt"].replace("T", " ").replace("Z", "")


def add_null_keys(item, fields):
        for field in fields:
            if not (field in item.keys()):
                item[field] = None


def build_row_array(item, fields):
    item_arr = []
    for field in fields:
        if (type(item[field]) == unicode):
            item_arr.append(item[field].encode('ascii',
                                               errors='backslashreplace'))
        else:
            item_arr.append(item[field])
    return item_arr


def get_campaigns(access_token):
    campaign_fields = ["id", "name", "description",
                       "type", "programName", "programId",
                       "workspaceName", "createdAt", "updatedAt",
                       "active"]
    campaign_ids = []
    url = parameters["url_base"] + "/rest/v1/campaigns.json?"\
        "access_token=" + access_token
    table_name = "campaigns"
    c = get_uri_data(url)
    # Build CSV
    f = open(table_name + ".csv", "wb+")
    w = csv.writer(f)
    w.writerow(campaign_fields)
    for item in c:
        campaign_ids.append(item["id"])
        add_null_keys(item, campaign_fields)
        fix_dates(item)
        # Remove line endings from description...
        if item["description"] is not None:
            item["description"] = item["description"]\
                                  .replace("\r", "").replace("\n", "")
        w.writerow(build_row_array(item, campaign_fields))
    f.close()


def get_lists(access_token):
    list_fields = ["id", "name", "programName",
                   "workspaceName", "createdAt", "updatedAt"]
    list_ids = []
    url = parameters["url_base"] + "/rest/v1/lists.json?access_token="\
        + access_token
    table_name = "lists"
    l = get_uri_data(url)
    f = open(table_name + ".csv", "wb+")
    w = csv.writer(f)
    w.writerow(list_fields)
    for item in l:
        list_ids.append(item["id"])
        add_null_keys(item, list_fields)
        fix_dates(item)
        w.writerow(build_row_array(item, list_fields))
    f.close()
    return list_ids


def get_programs(access_token):
    program_fields = ["id", "name", "description",
                      "createdAt", "updatedAt", "url",
                      "type", "channel", "status",
                      "workspace"]
    program_ids = []
    programs = []
    offset = 0
    max_return = 200
    url = parameters["url_base"]\
        + "/rest/asset/v1/programs.json?access_token="\
        + access_token + "&offset=" + str(offset) + "&maxReturn="\
        + str(max_return)
    table_name = "programs"
    while True:
        url = parameters["url_base"]\
            + "/rest/asset/v1/programs.json?access_token="\
            + access_token + "&offset=" + str(offset) + "&maxReturn="\
            + str(max_return)
        res = get_uri_data(url)
        if(res is None):
            break
        for item in res:
            programs.append(item)
        offset += max_return
    f = open(table_name + ".csv", "wb+")
    w = csv.writer(f)
    w.writerow(program_fields)
    for item in programs:
        program_ids.append(item["id"])
        add_null_keys(item, program_fields)
        fix_dates(item)
        w.writerow(build_row_array(item, program_fields))
    f.close()
    return program_ids


def get_leads_by_list(access_token, lists, fields):
    global parameters
    lead_fields = fields
    table_name = "leads"
    f = open(table_name + ".csv", "wb+")
    w = csv.writer(f)
    w.writerow(lead_fields)
    for l in lists:
        sys.stdout.write("*****Processing list " + str(l) + ".*****\n")
        url = parameters["url_base"] + "/rest/v1/list/" + str(l)\
            + "/leads.json?access_token=" + access_token
        leads = get_uri_data(url)
        for lead in leads:
            lead["listId"] = l
            add_null_keys(lead, lead_fields)
            fix_dates(lead)
            w.writerow(build_row_array(lead, lead_fields))
    f.close()


def get_leads_by_program(access_token, programs, fields):
    global parameters
    lead_fields = fields
    table_name = "leads_by_program"
    f = open(table_name + ".csv", "wb+")
    w = csv.writer(f)
    w.writerow(lead_fields)
    for p in programs:
        sys.stdout.write("*****Processing program " + str(p) + ".*****\n")
        url = parameters["url_base"]\
            + "/rest/v1/leads/pr`ograms/" + str(p) + ".json"\
            + "?access_token=" + access_token
        print url
        leads = get_uri_data(url)
        for lead in leads:
            lead["programID"] = p
            add_null_keys(lead, lead_fields)
            fix_dates(lead)
            w.writerow(build_row_array(lead, lead_fields))
    f.close()


def get_lead_fields():
    with open(script_path + "/lead_fields.json", "r") as f:
        lead_fields = json.load(f)
    return lead_fields


def load_parameters():
    global parameters
    global total_request_limit
    global total_request_count
    with open(script_path + "/config.json", "r") as f:
        parameters = json.load(f)
    total_request_limit = parameters["total_request_limit"]
    total_request_count = parameters["total_request_count"]


def main():
    load_parameters()
    access_token = get_auth()
    lead_fields = get_lead_fields()
    campaigns = get_campaigns(access_token)
    lists = get_lists(access_token)
    programs = get_programs(access_token)
    # get_leads(access_token, lists)
    # get_leads_by_program(access_token, programs, lead_fields)


if __name__ == "__main__":
    main()
else:
    load_parameters()
