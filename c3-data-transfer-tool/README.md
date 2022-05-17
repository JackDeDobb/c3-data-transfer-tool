# C3 Data Transfer Tool Package

## Summary
Welcome to the C3 Data Transfer Tool package! Below you fill find documentation\
on how to quickly transfer data between C3 environments.

Note: We have 2 step process for transferring data:
1. Utilize c3DataTransfer.downloadDataFromC3Env() to extract data from an environment.
1. Utilize c3DataTransfer.uploadDataToC3Env() to load data into an environment.

## Python Function Documentation
* c3DataTransfer.parseEnvironmentArguments()
    * Run python *pyScriptName*.py -h to see a list of parameters to call python script with.\
    You must include a call to c3DataTransfer.parseEnvironmentArguments() within script to see -h.
    * sendDeveloperData = True: send usage statistics to better improve application.

* c3DataTransfer.downloadDataFromC3Env():
    * environmentArguments: pass in the output from c3DataTransfer.parseEnvironmentArguments().
    * dataTypeExports: 2d array where first column is string of C3Type and second column\
    is boolean dict with the following keys: downloadData, refreshCalcFields, numRecordsPerFile, & filter.
        * downloadData: upload data for this type to the env.
        * refreshCalcFields: refresh calc fields for this type on env.
        * numRecordsPerFile = 2000: number of files to split records into, also the number of map-reduce jobs.
        * filter = '1 == 1': filter on C3 type for which records to extract.
    * dataDownloadFolder: filePath to where to download the exported files to.
    * errorOutputFolder = dataDownloadFolder + '_Errors': filePath to where refreshCalc errors are stored.
    * errorSleepTimeSeconds = 15: time to sleep when request fails before retrying.
    * refreshPollTimeSeconds = 15: time between refreshCalc status pings.
    * stripMetadataAndDerived = True: strips out the metadata & derived fields (calcs, fkey, etc.).
    * masterRefreshDataSwitch = True: has to be true in order to refresh any C3 types.
    * masterDownloadDataSwitch = True: has to be true in order to download any C3 types.
    * maxColumnPrintLength = 150: max print length.
    * promptUsersForWarnings = True: prompt users for warnings for accidental folder removals and resuming of queues.
    * sendDeveloperData = True: send usage statistics to better improve application.

* c3DataTransfer.uploadDataToC3Env():
    * environmentArguments: pass in the output from c3DataTransfer.parseEnvironmentArguments().
    * dataTypeImports: 2d array where first column is string of C3Type and second column\
    is boolean dict with the following keys: removeData, uploadData, refreshCalcFields, useSQLOnRemove, & disableDownstreamOnRemove.
        * removeData: remove current data for this type on env.
        * uploadData: upload data for this type to the env.
        * refreshCalcFields: refresh calc fields for this type on env.
        * useSQLOnRemove = False: if the operation can be handled via multi-row SQL operations, it will.
        * disableDownstreamOnRemove = False: disables any downstream asynchronous processing that would happen as a result of the operation (e.g. calc fields).
    * dataUploadFolder: filePath to where dataUploads folder is located.
    * errorOutputFolder = dataUploadFolder + '_Errors': filePath to where refreshCalc errors are stored.
    * batchSize = 250: size of batches to upload data.
    * errorSleepTimeSeconds = 15: time to sleep when request fails before retrying.
    * refreshPollTimeSeconds = 15: time between refreshCalc status pings.
    * masterRemoveDataSwitch = True: has to be true in order to remove any C3 types.
    * masterUploadDataSwitch = True: has to be true in order to upload any C3 types.
    * masterRefreshDataSwitch = True: has to be true in order to refresh any C3 types.
    * maxColumnPrintLength = 150: max print length.
    * promptUsersForWarnings = True: prompt users for warnings for accidental folder removals and resuming of queues.
    * sendDeveloperData = True: send usage statistics to better improve application.

* c3DataTransfer.callC3TypeAction() [Helper, Dynamic Filters]
    * environmentArguments: pass in the output from c3DataTransfer.parseEnvironmentArguments().
    * c3Type: the C3 type that contains the API.
    * action: the API off the C3 type to call.
    * payload: the payload to send with function parameters of the action.
    * sendDeveloperData = True: send usage statistics to better improve application.


## Usage Instructions
#### Download Procedure
Generate a python script that utilizes this library that specifies the types\
you would like to download and the folder location in which to download to.\
Call C3DataTransfer.downloadDataToC3Env(). See [Example PY Data Download Script](#example-py-data-upload-script).

Run the script with the proper command line parameters, pointing to the\
environment in which you wish to download data from. See [Example Command Line Commands](#example-command-line-commands).

#### Upload Procedure
Generate a python script that utilizes this libary to specify what data\
from the folder (that was just migrated to) you would like to upload.\
Call c3DataTransfer.uploadDataToC3Env(). See [Example PY Data Upload Script](#example-py-data-upload-script).

Run the script with the proper command line parameters, pointing to the\
environment in which you wish to upload data to. See [Example Command Line Commands](#example-command-line-commands).


## Example Migration

### Example PY Data Download Script
```
# clear && python [PY_FILE_PATH] -env [ENVIRONMENT] -tt [TENANT]:[TAG] -up [USER]:[PASS]
# clear && python [PY_FILE_PATH] -env [ENVIRONMENT] -tt [TENANT]:[TAG] -auth [AUTH_TOKEN]

# pip install c3-data-transfer-tool-jackdedobb

import json
import os
from c3DataMigration import c3DataTransfer


if __name__ == '__main__':
  dataDownloadFolder = '/'.join([os.path.dirname(os.path.abspath(__file__)), 'dataDownloads'])
  environmentArguments = c3DataTransfer.parseEnvironmentArguments()

  def formatIntersectsFilter (field, arrayValues):
    arrayValuesString = ','.join('"{}"'.format(x) for x in arrayValues)
    return 'intersects({field}, [{arrayValuesString}])'.format(
      field=field,
      arrayValuesString=arrayValuesString
    )

  consumptionForecastRunIds = [
    '2021-10-01T00:00:00.000',
    '2021-11-01T00:00:00.000',
    '2021-12-01T00:00:00.000',
    '2022-01-01T00:00:00.000',
    '2022-02-01T00:00:00.000',
  ]

  consumptionForecastRunFilter = formatIntersectsFilter('id', consumptionForecastRunIds)

  # Example Static Filter
  itemFacilityConsumptionForecastFilter = formatIntersectsFilter('run', consumptionForecastRunIds)

  # Example Dynamic Filter
  itemFacilityConsumptionForecastFetchResult = c3DataTransfer.callC3TypeAction(environmentArguments, 'ItemFacilityConsumptionForecast', 'fetch', {
    'spec': {
      'filter': formatIntersectsFilter('run', consumptionForecastRunIds),
      'include': 'id',
      'limit':   -1,
    }
  })
  itemFacilityConsumptionForecastFetchResults = json.loads(itemFacilityConsumptionForecastFetchResult.text)
  itemFacilityConsumptionForecastIds = [x['id'] for x in itemFacilityConsumptionForecastFetchResults['objs']]
  itemFacilityConsumptionForecastMeasurementFilter = formatIntersectsFilter('forecast', itemFacilityConsumptionForecastIds)

  numRecordsPerFile = 1000
  dataTypeExports = [
    ['BusinessChangeRule',                         { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': '1 == 1'                                         }],
    ['ConsumptionForecastRun',                     { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': consumptionForecastRunFilter                     }],
    ['ItemFacilityConsumptionForecast',            { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': itemFacilityConsumptionForecastFilter            }],
    ['ItemFacilityConsumptionForecastMeasurement', { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': itemFacilityConsumptionForecastMeasurementFilter }],
    ['Item',                                       { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': '1 == 1'                                         }],
    ['Facility',                                   { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': '1 == 1'                                         }],
    ['NewCustomerRule',                            { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': '1 == 1'                                         }],
    ['NonTypicalOrder',                            { 'downloadData': True, 'refreshCalcFields': True, 'numRecordsPerFile': numRecordsPerFile, 'filter': '1 == 1'                                         }],
  ]

  c3DataTransfer.downloadDataFromC3Env(
    environmentArguments =     environmentArguments,
    dataTypeExports =          dataTypeExports,
    dataDownloadFolder =       dataDownloadFolder,
    errorOutputFolder =        dataDownloadFolder + '_Errors',
    errorSleepTimeSeconds =    5,
    refreshPollTimeSeconds =   1,
    stripMetadataAndDerived =  True,
    masterRefreshDataSwitch =  True,
    masterDownloadDataSwitch = True,
  )
```

### Example PY Data Upload Script
```
# clear && python [PY_FILE_PATH] -env [ENVIRONMENT] -tt [TENANT]:[TAG] -up [USER]:[PASS]
# clear && python [PY_FILE_PATH] -env [ENVIRONMENT] -tt [TENANT]:[TAG] -auth [AUTH_TOKEN]

# pip install c3-data-transfer-tool-jackdedobb

import os
from c3DataMigration import c3DataTransfer


if __name__ == '__main__':
  dataUploadFolder = '/'.join([os.path.dirname(os.path.abspath(__file__)), 'dataUploads'])
  environmentArguments = c3DataTransfer.parseEnvironmentArguments()

  dataTypeImports = [
    ['BusinessChangeRule',     { 'removeData': True,  'uploadData': True,  'refreshCalcFields': True, 'useSQLOnRemove': True, 'disableDownstreamOnRemove': True }],
    ['ConsumptionForecastRun', { 'removeData': True,  'uploadData': True,  'refreshCalcFields': True, 'useSQLOnRemove': True, 'disableDownstreamOnRemove': True }],
    ['Item',                   { 'removeData': True,  'uploadData': True,  'refreshCalcFields': True, 'useSQLOnRemove': True, 'disableDownstreamOnRemove': True }],
    ['Facility',               { 'removeData': True,  'uploadData': True,  'refreshCalcFields': True, 'useSQLOnRemove': True, 'disableDownstreamOnRemove': True }],
    ['NewCustomerRule',        { 'removeData': True,  'uploadData': True,  'refreshCalcFields': True, 'useSQLOnRemove': True, 'disableDownstreamOnRemove': True }],
    ['NonTypicalOrder',        { 'removeData': True,  'uploadData': True,  'refreshCalcFields': True, 'useSQLOnRemove': True, 'disableDownstreamOnRemove': True }],
  ]

  c3DataTransfer.uploadDataToC3Env(
    environmentArguments =    environmentArguments,
    dataTypeImports =         dataTypeImports,
    dataUploadFolder =        dataUploadFolder,
    errorOutputFolder =       dataUploadFolder + '_Errors',
    batchSize =               200,
    errorSleepTimeSeconds =   5,
    refreshPollTimeSeconds =  1,
    masterRemoveDataSwitch =  True,
    masterUploadDataSwitch =  True,
    masterRefreshDataSwitch = True,
  )
```

### Example Command Line Commands
```
clear && python [PY_FILE_PATH] -env [ENVIRONMENT] -tt [TENANT]:[TAG] -up [USER]:[PASS]
clear && python [PY_FILE_PATH] -env [ENVIRONMENT] -tt [TENANT]:[TAG] -auth [AUTH_TOKEN]
```
