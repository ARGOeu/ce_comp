#!/usr/bin/env python
import os
import sys
import requests
import json
import argparse
import ConfigParser
from termcolor import colored, cprint
from prettytable import PrettyTable
import pandas as pd


def date_format(year, month, day):
    """ Date formater
    Example:
          if the integer representing the day is above 9,
          we keep as it is, e.g. 10
          else we format it in the following way, e.g. 08
    Attributes:
              year(str): the year of the in use date
              month(str): the month of the in use date
              day(int): the day of the in use date
    Returns:
           str: the formated date to be used in the next api request
    """
    
    if len(str(day)) == 2:
        conv_day = str(day)
    else:
        conv_day = "0"+str(day)

    # date format expected by the api: 2018-02-24

    return year+"-"+month+"-"+conv_day


def generate_endpoints(data):
    """ take the response JSON and return a dictionary of endpoints
    Example:
           { endpoint_name@group : {"availability" : x, "reliability": x} }
    Attributes:
              data(dict): the api response
    Returns:
           (dict): endpoints converted to an easily manipulated form
    """
    
    _endpoints = {}
    
    for line in data["results"]:

        for endpoint in line["endpoints"]:

            _dict = {}

            _dict["availability"] = endpoint["results"][0]["availability"]
            _dict["reliability"] = endpoint["results"][0]["reliability"]

            _endpoints[endpoint["name"]+"@"+line["name"]] = _dict
            
    return _endpoints


def endpoint_comparison(_dict, error, count):
    """ for each endpoint, calculate the difference in
    availability and reliability given the a/r for
    both engines (hadoop and flink)

    Attributes:
              _dict(dict):a dictionary representing an endpoint
              error(float): total error for both availability and reliability
              count(int): total computations performed
    Returns:
           (dict): updated representation for the respective  endpoint
           (float): updated total error
           (int): updated total count
    """

    if _dict["a_prod"] != -1 and _dict["a_devel"] != -1:

        a_er = abs(_dict["a_prod"] - _dict["a_devel"])

        _dict["d_a"] = round(a_er, 2)

        error += a_er

        count += 1

    if _dict["r_prod"] != -1 and _dict["r_devel"] != -1:

        r_er = abs(_dict["r_prod"] - _dict["r_devel"])

        _dict["d_r"] = round(r_er, 2)

        error += r_er

        count += 1

    return _dict, error, count


def main(args=None):

    tenant = args.Tenant

    threshold = args.Threshold

    config_path = args.ConfigPath

    config = ConfigParser.ConfigParser()
    config.read(config_path)

    output_format = args.OutputFormat

    if args.SavePath is None:
        save_path = config.get("SaveLocation", "path")
    else:
        save_path = args.SavePath

    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))

    # check if the user has given start and end date, otherwise load them from conf file

    if args.StartDate is None:
        year, month, start_date_day = config.get("PERIOD", "start_date").split("-")
    else:
        year, month, start_date_day = args.StartDate.split("-")

    if args.EndDate is None:
        end_date_day = config.get("PERIOD", "end_date").split("-")[2]
    else:
        end_date_day = args.EndDate.split("-")[2]
    # loop through the range of days
    # use +1 since range function excludes last element
    for day in range(int(start_date_day), int(end_date_day)+1):

        date = date_format(year, month, day)

        prod_url = config.get(tenant, "hadoop").strip().replace("{start_date}", date)

        devel_url = config.get(tenant, "flink").strip().replace("{start_date}", date)

        headers = {"accept": "application/json",
                   "x-api-key": config.get(tenant, "token")}

        prod_response = requests.get(prod_url, headers=headers)
        prod_data = json.loads(prod_response.text)

        devel_response = requests.get(devel_url, headers=headers)
        devel_data = json.loads(devel_response.text)

        # all the production endpoints formated for the following calculations
        prod_dict = generate_endpoints(prod_data)

        # all the devel endpoints formated for the following calculations
        devel_dict = generate_endpoints(devel_data)

        # points found in either prod or devel but not in both
        missing_points = set(prod_dict.keys()) ^ set(devel_dict.keys())

        # total error for both reliability and availability
        error = 0

        # total endpoints tested for both reliability and availability
        count = 0

        # dictionary holding the availability and reliability error for each endpoint
        endpoint_error_stats = {}

        # endpoints with an availability difference above the the threshold
        thresholded_points_by_da = {}

        # endpoints with a reliability difference above the threshold
        thresholded_points_by_dr = {}

        # initialize dataframe
        if output_format == "csv" or output_format == "html":
            df = pd.DataFrame([], columns=["Endpoint",
                                           "A_prod",
                                           "A_devel",
                                           "R_prod",
                                           "R_devel",
                                           "D_a",
                                           "D_r"])

        # for each endpoint in production, generate its results
        for key in prod_dict.keys():

            _dict = {}

            if key not in missing_points:

                _dict["a_prod"] = float(prod_dict[key]["availability"])
                _dict["a_devel"] = float(devel_dict[key]["availability"])
                _dict["r_prod"] = float(prod_dict[key]["reliability"])
                _dict["r_devel"] = float(devel_dict[key]["reliability"])
                _dict["d_a"] = "na"
                _dict["d_r"] = "na"

                _dict, error, count = endpoint_comparison(_dict, error, count)

                if _dict["d_a"] != "na" and _dict["d_a"] >= threshold:
                    thresholded_points_by_da[key] = _dict["d_a"]

                if _dict["d_r"] != "na" and _dict["d_r"] >= threshold:
                    thresholded_points_by_dr[key] = _dict["d_r"]

                if output_format == "csv" or output_format == "html":
                    _df = pd.DataFrame([[key,
                                         _dict["a_prod"],
                                         _dict["a_devel"],
                                         _dict["r_prod"],
                                         _dict["r_devel"],
                                         _dict["d_a"],
                                         _dict["d_r"]]],
                                       columns=["Endpoint",
                                                "A_prod",
                                                "A_devel",
                                                "R_prod",
                                                "R_devel",
                                                "D_a",
                                                "D_r"])

                    df = df.append(_df, ignore_index=True)

                endpoint_error_stats[key] = _dict

        #
        error_count_tb = PrettyTable()
        error_count_tb.field_names = ["Error", "Endpoints", "Avg Error"]
        error_count_tb.add_row([error, count, error/count])

        if output_format == "csv":
            df.to_csv(save_path+tenant+"@"+date+"_report.csv", index=False, sep=",")
        elif output_format == "html":
            df.to_html(save_path+tenant+"@"+date+"_report.html", index=False)
        else:
            with open(save_path+tenant+"@"+date+"_report.json", "w") as fw:
                json.dump(endpoint_error_stats, fw)

        # table representation of the missing points
        missing_points_tb = PrettyTable()
        missing_points_tb.field_names = ["Name",
                                         "Found in prod",
                                         "Found in devel"]

        for point in missing_points:
            if point in prod_dict.keys():
                missing_points_tb.add_row([point, "Yes", "No"])
            else:
                missing_points_tb.add_row([point, "No", "Yes"])

        # terminal output for the calculated results
        print("\n########################################")

        print("\t\t\tDate: " + date+"\n")

        print("Endpoints that were not found in both engines")

        print(missing_points_tb)

        print(error_count_tb)

        print("\nEndpoints with a/r higher than the threshold"+"("+str(threshold)+").")
        print("---------------------------------------------")

        print("\nAvailability Difference\n")
        for point in sorted(thresholded_points_by_da, key=thresholded_points_by_da.get, reverse=True):
            cprint(colored(point + " - " + str(thresholded_points_by_da[point])+" "), "cyan", attrs=['bold'])

        print("\nReliability Difference\n")
        for point in sorted(thresholded_points_by_dr, key=thresholded_points_by_dr.get, reverse=True):
            cprint(colored(point + " - " + str(thresholded_points_by_dr[point])+" "), "yellow", attrs=['bold'])


if __name__ == '__main__':

    # get the arguments we need for the script execution,
    # Everything is mandatory except SavePath
    # (output_format, config_path, tenant, threshold, save_path)
    
    parser = argparse.ArgumentParser(description="Comparing AR results between hadoop and flink infrastructure")
    parser.add_argument(
        "-s", "--OutputFormat", type=str, default="json", help="Report's output format, default is json")
    parser.add_argument(
        "-c", "--ConfigPath", type=str, help="Path for the config file", required=True)
    parser.add_argument(
        "-t", "--Tenant", type=str, help="Name of the tenant", required=True)
    parser.add_argument(
        "-th", "--Threshold", type=float, default=1.0, help="Threshold")
    parser.add_argument(
        "-sp", "--SavePath", type=str, help="Save path for the generated report. If left empty, the default location will be loaded from the configuration file")
    parser.add_argument(
        "-sd", "--StartDate", type=str, help="Starting date")
    parser.add_argument(
        "-ed", "--EndDate", type=str, help= "End date")
    
    # Parse arguments and pass them to main
    sys.exit(main(parser.parse_args()))
