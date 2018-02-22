# Compute Engine comparison script

## Description
An automatic mechanism to gather and compare results between Hadoop and Flink infrastructures.
ARGO serves availability/reliability results along with status timelines of the monitored elements (sites, services, endpoints, endpoint metrics). The current stable infrastructure is using Hadoop to compute availability and reliability and status results. Computation logic has been transferred to Flink also (currently running in a devel Flink cluster) and soon to be deployed in a Flink production cluster. It is essential for the switch to have a mechanism to compare results between the legacy and the new infrastructure.

## How to use
`ce_compy.py -s <Output format> -t <Tenant> -c <Conf file> -th <Threshold> -sp <Report's save path>[Optional] -sd <StartDate>[Optional] -ed <EndDate>[Optional]`

| argument | Description |
| --- | --- |
| -s <Output format> | Report's file format. Right now it supports csv and html, with the default being json. |
| -t <Tenant> | Tenant. |
| -c <Conf file> |  Path to the config file. |
| -th <Threshold> | Threshold availability and reliability results by a minimum threshold. |
| -sp <Save path> [Optional] | If not specified, the script will create a directory named **generated_reports** in the same directory with itself, and store the results there. |
| -sd <StartingDate>[Optional] | If not specified the script will load the starting date from the conf file. |
| -ed <EndDate>[Optional] | If not specified the script will load the end date from the conf file. |

**to run the script execute the following command**

`$ ./ce_comp.py -s csv -t TenantA -c conf.cfg -th 1 -sp ~/my/reports/out -sd 2018-02-01 -ed 2018-02-10`

The report's file name will be: <tenant>@<date>_report.<output_format>
e.g. TenantA@2018-02-15_report.csv

### Configuration file

```
[LOGS]
handler_path:

[TENANTA]
hadoop: https://web-api-devel.argo.grnet.gr/api/v2/results/reportA/SERVICEGROUPS?start_time={start_date}T00%3A00%3A00Z&end_time={start_date}T23%3A53%3A00Z

flink: https://web-api-devel.argo.grnet.gr/api/v2/results/reportB/SERVICEGROUPS?start_time={start_date}T00%3A00%3A00Z&end_time={start_date}T23%3A53%3A00Z

token: *********************

[TENANTB]
hadoop: https://web-api-devel.argo.grnet.gr/api/v2/results/reportA/SITES?start_time={start_date}T00%3A00%3A00Z&end_time={start_date}T23%3A53%3A00Z

flink: https://web-api-devel.argo.grnet.gr/api/v2/results/reportB/SITES?start_time={start_date}T00%3A00%3A00Z&end_time={start_date}T23%3A53%3A00Z

token: ***********************

[PERIOD]
start_date: 2018-02-01
end_date: 2018-02-12

[SaveLocation]
path: ./generated_reports/ 
```
 - Each tenant is described by two urls,one for each infrastructure respectively, and a token, in order to access its data and compare.
 - The script supports functionality to calculate results for a period of time in a single execution. All of the results will be saved into the **[SaveLocation]** described by the tenant and the day.

### Results
The report produced by the script contains information about each site found under a group that belogs to the specified tenant.
e.g. The first row of the report for the  TENANTA.

| Endpoint | A_prod | A_devel | R_prod | R_devel | D_a | D_r |
| --- | --- | --- | --- | --- | --- | -- |
|siteA@groupA|100|98|95|-1|2|na|

For each endpoint, it shows the site it belogs to, the availability and reliability metrics coming from production and devel respectively. At the end it shows the calculated differences.
**Note:** If the availability or reliability metric for the day is -1, meaning that the owner stated scheduled downtime, it produced a non applicable value.

Also during execution the script prints results in the command line regarding which points were missing from either, The Compute Engine(hadoop based) or the Argo Streaming Engine(flink based), the average error and the thresholded endpoints sorted in descednding order.

### Prequisites

**Navigate to the cloned directory**

```shell
$ pip install -r requirements.txt
$ mv conf.template conf.cfg
```
**Note:** After generating conf.cfg, you need to manually edit it and add the information you need for your computations.

### Testing
Inside the script's directory you can run the `$ pytest` command.






