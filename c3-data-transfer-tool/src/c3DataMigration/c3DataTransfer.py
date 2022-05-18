__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import argparse
from c3DataMigration.c3Helpers import c3FileSystem
from c3DataMigration.c3Helpers import c3PythonClasses
from c3DataMigration.c3Helpers import c3Request
from c3DataMigration.c3Helpers import c3UsageStats
from c3DataMigration.c3Helpers import c3UtilityMethods
from c3DataMigration.c3MigrationMethods import c3DataDownload
from c3DataMigration.c3MigrationMethods import c3DataUpload
from c3DataMigration.c3MigrationMethods import c3DataRefreshCalcFields
from c3DataMigration.c3MigrationMethods import c3DataRemove




def checkMostUpdatedVersion ():
  currentVersion = c3UtilityMethods.getLocalVersionC3DataTransferTool()
  latestVersion = c3UtilityMethods.getLatestVersionC3DataTransferTool()

  if ((currentVersion != None) and (latestVersion != None) and (currentVersion != latestVersion)):
    print('Please upgrade to the latest version to continue using the tool. Please run pip install --upgrade c3-data-transfer-tool-jackdedobb.')
    exit(0)




def parseEnvironmentArguments (sendDeveloperData=True):
  checkMostUpdatedVersion()

  parser = argparse.ArgumentParser(description=
    'Uploads data files to a specific env.\
    You need to specify an environment, tenanttag, and authentication userpassword.'
  )
  parser.add_argument('-env',  '--env',       action='store', dest='env',       help='QA environment to upload data to',            required=True)
  parser.add_argument('-tt',   '--tenanttag', action='store', dest='tenantTag', help='tenant:tag to upload data to',                required=True)
  parser.add_argument('-up',   '--userpass',  action='store', dest='userPass',  help='user:pass to upload data to',                 required=False)
  parser.add_argument('-auth', '--authtoken', action='store', dest='authToken', help='c3 authentication token to upload data with', required=False)
  r = parser.parse_args()
  # Parse tenant:tag to tenant and tag, similar operation with Username password
  if ((not r.userPass) and (not r.authToken)):
    exit('You must specify either a user:pass or an authtoken.')
  try:
    r.tenant, r.tag = r.tenantTag.split(':')
    if (r.userPass):
      r.user, r.password = r.userPass.split(':')
    r.env = r.env[:-1] if (r.env[-1] == '/') else r.env
  except:
    exit('You must separate tenant:tag and user:pass with a colon.')
  print('Env: ' + r.env)

  p = c3PythonClasses.APIParameters(
    sendDeveloperData = sendDeveloperData,
    outerAPICall      = 'parseArgsAPI'
  )
  c3UsageStats.ParseArgsAPI.logStart(r, p)
  c3UsageStats.ParseArgsAPI.logFinish(r, p)

  return r




def callC3TypeAction (environmentArguments, c3Type, action, payload, sendDeveloperData=True):
  checkMostUpdatedVersion()

  url = c3Request.generateTypeActionURL(environmentArguments, c3Type, action)
  requestResponse = c3Request._makeRequestHelper(environmentArguments, url, payload)

  p = c3PythonClasses.APIParameters(
    sendDeveloperData = sendDeveloperData,
    outerAPICall      = 'callC3TypeActionAPI'
  )
  c3UsageStats.callC3TypeActionAPI.logStart(environmentArguments, p, c3Type, action)
  c3UsageStats.callC3TypeActionAPI.logFinish(environmentArguments, p)

  return requestResponse




def uploadDataToC3Env (
    environmentArguments,
    dataTypeImports,
    batchSize,
    errorSleepTimeSeconds,
    refreshPollTimeSeconds,
    dataUploadFolder,
    errorOutputFolder=None,
    masterRemoveDataSwitch=True,
    masterUploadDataSwitch=True,
    masterRefreshDataSwitch=True,
    maxColumnPrintLength=None,
    promptUsersForWarnings=True,
    sendDeveloperData=True,
  ):
  checkMostUpdatedVersion()

  r = environmentArguments
  p = c3PythonClasses.APIParameters(
    dataTypeImports         = dataTypeImports,
    dataUploadFolder        = dataUploadFolder,
    errorOutputFolder       = errorOutputFolder,
    batchSize               = batchSize,
    errorSleepTimeSeconds   = errorSleepTimeSeconds,
    refreshPollTimeSeconds  = refreshPollTimeSeconds,
    maxColumnPrintLength    = maxColumnPrintLength,
    masterRemoveDataSwitch  = masterRemoveDataSwitch,
    masterUploadDataSwitch  = masterUploadDataSwitch,
    masterRefreshDataSwitch = masterRefreshDataSwitch,
    promptUsersForWarnings  = promptUsersForWarnings,
    sendDeveloperData       = sendDeveloperData,
    outerAPICall            = 'uploadAPI'
  )

  c3UsageStats.UploadAPI.logStart(environmentArguments, p)
  c3FileSystem.wipeLocalDirectory(p, errorOutputFolder, p.promptUsersForWarnings)
  c3UtilityMethods.enableQueues(r, p, p.promptUsersForWarnings)
  c3DataRemove.removeDataFromEnv(r, p)
  c3DataUpload.uploadDataToEnv(r, p)
  c3DataRefreshCalcFields.refreshDataOnEnv(r, p, p.dataTypeImports)
  c3UsageStats.UploadAPI.logFinish(r, p)




def downloadDataFromC3Env (
    environmentArguments,
    dataTypeExports,
    errorSleepTimeSeconds,
    refreshPollTimeSeconds,
    dataDownloadFolder,
    errorOutputFolder=None,
    stripMetadataAndDerived=True,
    masterRefreshDataSwitch=True,
    masterDownloadDataSwitch=True,
    maxColumnPrintLength=None,
    promptUsersForWarnings=True,
    sendDeveloperData=True,
  ):
  checkMostUpdatedVersion()

  r = environmentArguments
  p = c3PythonClasses.APIParameters(
    dataTypeExports          = dataTypeExports,
    dataDownloadFolder       = dataDownloadFolder,
    errorOutputFolder        = errorOutputFolder,
    errorSleepTimeSeconds    = errorSleepTimeSeconds,
    refreshPollTimeSeconds   = refreshPollTimeSeconds,
    stripMetadataAndDerived  = stripMetadataAndDerived,
    maxColumnPrintLength     = maxColumnPrintLength,
    masterRefreshDataSwitch  = masterRefreshDataSwitch,
    masterDownloadDataSwitch = masterDownloadDataSwitch,
    promptUsersForWarnings   = promptUsersForWarnings,
    sendDeveloperData        = sendDeveloperData,
    outerAPICall             = 'downloadAPI'
  )

  c3UsageStats.DownloadAPI.logStart(environmentArguments, p)
  c3FileSystem.wipeLocalDirectory(p, dataDownloadFolder, p.promptUsersForWarnings)
  c3FileSystem.wipeLocalDirectory(p, errorOutputFolder, p.promptUsersForWarnings)
  c3UtilityMethods.enableQueues(r, p, p.promptUsersForWarnings)
  c3DataRefreshCalcFields.refreshDataOnEnv(r, p, p.dataTypeExports)
  c3DataDownload.downloadDataFromEnv(r, p)
  c3UsageStats.DownloadAPI.logFinish(r, p)
