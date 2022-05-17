__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import copy
import getpass
import json
import pytz
import requests




def _removeFieldIfExists (obj, field):
  if (field in obj):
    del obj[field]




def _formatDatetimeIfExists (obj, field):
  if (field in obj):
    obj[field] = obj[field].isoformat() if (obj[field] != None) else None




def _omitSensitiveEnvironmentArguments (environmentArguments):
  envArgsDict = vars(copy.deepcopy(environmentArguments))

  _removeFieldIfExists(envArgsDict, 'tenantTag')
  _removeFieldIfExists(envArgsDict, 'userPass')
  _removeFieldIfExists(envArgsDict, 'user')
  _removeFieldIfExists(envArgsDict, 'password')
  _removeFieldIfExists(envArgsDict, 'authToken')

  return envArgsDict




def _omitSensitiveFunctionParameters (functionParameters):
  functionParamsDict = vars(copy.deepcopy(functionParameters))

  _removeFieldIfExists(functionParamsDict, 'errorOutputFolder')
  _removeFieldIfExists(functionParamsDict, 'dataUploadFolder')
  _removeFieldIfExists(functionParamsDict, 'dataDownloadFolder')
  _formatDatetimeIfExists(functionParamsDict, 'initialTime')

  dataTypeExportsCopy = copy.deepcopy(functionParamsDict['dataTypeExports'])
  for x in dataTypeExportsCopy:
    _removeFieldIfExists(x[1], 'filter')
  functionParamsDict['dataTypeExports'] = dataTypeExportsCopy

  dataTypeImportsCopy = copy.deepcopy(functionParamsDict['dataTypeImports'])
  for x in dataTypeImportsCopy:
    if ('files' in x[1]):
      x[1]['localFileCount'] = len(x[1]['files'])
    _removeFieldIfExists(x[1], 'files')
    _removeFieldIfExists(x[1], 'gzipFiles')
    _removeFieldIfExists(x[1], 'remoteFileURLs')
  functionParamsDict['dataTypeImports'] = dataTypeImportsCopy

  return functionParamsDict




def _cleanC3ToTypeToBatchJobMappingArrays (c3TypeToBatchJobMappingArray):
  array = copy.deepcopy(c3TypeToBatchJobMappingArray)
  for x in array:
    if ('fileUrls' in x[1]):
      x[1]['outputFileCount'] = len(x[1]['fileUrls'])
    _removeFieldIfExists(x[1], 'id')
    _removeFieldIfExists(x[1], 'filter')
    _removeFieldIfExists(x[1], 'fileUrls')
    _formatDatetimeIfExists(x[1], 'launchTime')
    _formatDatetimeIfExists(x[1], 'completionTime')
  
  return array




def _logAPIParmeters (p, stateOfActions, data):
  if (p.sendDeveloperData != True):
    return

  try:
    user = getpass.getuser().replace('.', '-').replace('/', '-')
    initialTimeFormatted = p.initialTime.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
    url = '/'.join(['https://c3-data-transfer-tool-default-rtdb.firebaseio.com', user, p.outerAPICall, initialTimeFormatted, stateOfActions]) + '.json'
    requests.put(url, data=json.dumps(data))
  except:
    print('exception occured')



def _logAPIStateAndAdditionalFields (r, p, state, additionalFieldsDict={}):
  additionalFieldsDict = {} if (additionalFieldsDict == None) else additionalFieldsDict
  defaultPostingFields = {
    'environmentArguments': _omitSensitiveEnvironmentArguments(r),
    'functionParams': _omitSensitiveFunctionParameters(p),
    'state': state,
  }

  _logAPIParmeters(p, state, { **defaultPostingFields, **additionalFieldsDict })




class BaseLoggingClass:
  @staticmethod
  def logStart (r, p):
    _logAPIStateAndAdditionalFields(r, p, '01_INITIAL', None)

  @staticmethod
  def logFinish (r, p):
    _logAPIStateAndAdditionalFields(r, p, 'COMPLETE', None)




class ParseArgsAPI(BaseLoggingClass):
  pass




class callC3TypeActionAPI(BaseLoggingClass):
  @staticmethod
  def logStart (r, p, c3Type, action):
    _logAPIStateAndAdditionalFields(r, p, '01_INITIAL', {
      'c3Type': c3Type,
      'action': action,
    })




class UploadAPI(BaseLoggingClass):
  @staticmethod
  def logAPIRemove (r, p, c3TypeToBatchJobMapping):
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')
      if (('initialFetchCount' in x[1]) and ('currentFetchCount' in x[1])):
        x[1]['recordsRemoved'] = x[1]['initialFetchCount'] - x[1]['currentFetchCount']
        _removeFieldIfExists(x[1], 'initialFetchCount')
        _removeFieldIfExists(x[1], 'currentFetchCount')
    _logAPIStateAndAdditionalFields(r, p, '02_REMOVE_COMPLETE', {
      'removeBatchJob': c3TypeToBatchJobMappingCopy,
    })

  @staticmethod
  def logZipFiles (r, p):
    _logAPIStateAndAdditionalFields(r, p, '03_ZIP_FILES', None)

  @staticmethod
  def logCurlFiles (r, p):
    _logAPIStateAndAdditionalFields(r, p, '04_CURL_FILES', None)

  @staticmethod
  def logBatchJob (r, p, c3TypeToBatchJobMapping):
    _logAPIStateAndAdditionalFields(r, p, '05_BATCH_IMPORT', {
      'importBatchJob': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
    })

  @staticmethod
  def logImportErrors (r, p, c3TypeToBatchJobMapping):
    _logAPIStateAndAdditionalFields(r, p, '06_IMPORT_ERRORS', {
      'importBatchJob': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
    })

  @staticmethod
  def logAPIRefreshCalcs (r, p, c3TypeToBatchJobMapping):
    _logAPIStateAndAdditionalFields(r, p, '07_REFRESH_CALCS_COMPLETE', {
      'refreshBatchJob': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
    })




class DownloadAPI(BaseLoggingClass):
  @staticmethod
  def logAPIRefreshCalcs (r, p, c3TypeToBatchJobMapping):
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')
    _logAPIStateAndAdditionalFields(r, p, '02_REFRESH_CALCS_COMPLETE', {
      'refreshBatchJob': c3TypeToBatchJobMappingCopy,
    })

  @staticmethod
  def logBatchJob (r, p, c3TypeToBatchJobMapping):
    _logAPIStateAndAdditionalFields(r, p, '03_BATCH_JOBS_COMPLETE', {
      'exportBatchJob': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
    })

  @staticmethod
  def logCurlFiles (r, p):
    _logAPIStateAndAdditionalFields(r, p, '04_CURL_FILES_COMPLETE')

  @staticmethod
  def logExtractFiles (r, p):
    _logAPIStateAndAdditionalFields(r, p, '05_EXTRACT_FILES_COMPLETE')

  @staticmethod
  def logExportErrors (r, p, c3TypeToBatchJobMapping):
    _logAPIStateAndAdditionalFields(r, p, '06_EXPORT_ERRORS', {
      'exportBatchJob': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
    })
