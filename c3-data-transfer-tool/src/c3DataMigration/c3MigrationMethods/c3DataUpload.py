__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
from functools import reduce
from progress.bar import IncrementalBar
from c3DataMigration.c3Helpers import c3FileSystem
from c3DataMigration.c3Helpers import c3UsageStats
from c3DataMigration.c3Helpers import c3UtilityMethods
from c3DataMigration.c3Helpers import c3Request




def _zipImportFiles (r, p):
  existingGZipFilePaths = []
  for dataType in p.dataTypeImports:
    c3Type = dataType[0]
    dataTypeUploadFolder = '/'.join([p.dataUploadFolder, c3Type])
    existingGZipFilePaths.extend(c3FileSystem.getLocalFilePathsWithinDirectory(dataTypeUploadFolder, '.gz'))
  c3FileSystem.deleteLocalFiles(existingGZipFilePaths)

  c3FileSystem.zipFilesInDirectory(r, p, p.dataUploadFolder, p.dataTypeImports)
  for dataType in p.dataTypeImports:
    c3Type = dataType[0]
    dataTypeUploadFolder = '/'.join([p.dataUploadFolder, c3Type])
    dataType[1]['gzipFiles'] = c3FileSystem.getLocalFilePathsWithinDirectory(dataTypeUploadFolder, '.gz')




def _postImportFiles (r, p):
  directoryOnEnv = c3FileSystem.getRemoteImportDirectory(r, p)
  c3FileSystem.deleteRemoteDirectory(r, p, directoryOnEnv)

  for dataTypeImport in p.dataTypeImports:
    c3Type = dataTypeImport[0]
    gzipFilePaths = dataTypeImport[1]['gzipFiles']
    dataTypeFilesRemoteFolderPath = '/'.join([directoryOnEnv, c3Type])

    if (dataTypeImport[1]['uploadData'] != True):
      c3UtilityMethods.printFormatExtraPeriods('Posting ' + c3Type, 'UPLOAD FLAG IS FALSE', p.maxColumnPrintLength, True)
      continue

    if (len(gzipFilePaths) == 0):
      c3UtilityMethods.printFormatExtraPeriods('Posting ' + c3Type, 'NO IMPORT FILES', p.maxColumnPrintLength, True)
      continue

    remoteFileUrls = []
    result = c3UtilityMethods.printFormatExtraPeriods('Posting ' + c3Type, ' |████████████████████████████████|', p.maxColumnPrintLength, False)
    progressBar = IncrementalBar(''.join(result[:2]), max=len(gzipFilePaths))
    for idx, gzipFilePath in enumerate(gzipFilePaths):
      remoteUploadFilePath = '/'.join([dataTypeFilesRemoteFolderPath, str(idx) + '.json.gz'])
      fullFileURL = c3Request.generateFileURL(r, remoteUploadFilePath)
      errorCodePrefix = 'Unsuccessful pushing ' + c3Type + ': ' + fullFileURL
      c3Request.uploadFileToURL(r, p.errorSleepTimeSeconds, fullFileURL, gzipFilePath, errorCodePrefix)
      remoteFileUrls.append(remoteUploadFilePath)
      [progressBar.next() for _ in range(1)]
    progressBar.finish()
    dataTypeImport[1]['remoteFileUrls'] = remoteFileUrls




def _startDataUploadToEnv (r, p):
  c3TypeToBatchJobMapping = []
  for dataTypeImport in p.dataTypeImports:
    c3Type = dataTypeImport[0]
    gzipFilePaths = dataTypeImport[1]['gzipFiles']

    if (dataTypeImport[1]['uploadData'] != True):
      c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'UPLOAD FLAG IS FALSE', p.maxColumnPrintLength, True)
      continue

    if (len(gzipFilePaths) == 0):
      c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'NO IMPORT FILES', p.maxColumnPrintLength, True)
      continue

    url = c3Request.generateTypeActionURL(r, 'Import', 'startImport')
    payload = {
      'spec': {
        'targetType': c3Type,
        'fileList': {
          'urls': dataTypeImport[1]['remoteFileUrls']
        }
      }
    }
    errorCodePrefix = 'Unsuccessful kicking off import of type ' + c3Type
    request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)
    batchJobId = c3UtilityMethods.createInitialBatchJobStatusEntry(request, c3Type, c3TypeToBatchJobMapping, None)
    c3TypeToBatchJobMapping[-1][1]['initialFetchCount'] = dataTypeImport[1]['recordCount']
    c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'id=' + str(batchJobId), p.maxColumnPrintLength, True)

  return c3TypeToBatchJobMapping




def _finishDataUploadToEnv (r, p, c3TypeToBatchJobMapping):
  jobType = 'Import'
  c3UtilityMethods.waitForBatchJobsToComplete(r, p, c3TypeToBatchJobMapping, jobType, 'importAction')




def _cleanUpZippedImportFiles (r, p):
  listOfListOfFileUrls = [x[1]['remoteFileUrls'] for x in p.dataTypeImports]
  flattenedListFileUrls = reduce(lambda z, y : z + y, listOfListOfFileUrls)
  c3FileSystem.deleteRemoteFiles(r, p, flattenedListFileUrls)

  remoteImportDir = c3FileSystem.getRemoteImportDirectory(r, p)
  c3FileSystem.deleteRemoteDirectory(r, p, remoteImportDir)

  listOfListOfGZipPaths = [x[1]['gzipFiles'] for x in p.dataTypeImports]
  flattenedListGZipPaths = reduce(lambda z, y : z + y, listOfListOfGZipPaths)
  c3FileSystem.deleteLocalFiles(flattenedListGZipPaths)




def uploadDataToEnv (r, p):
  if (p.masterUploadDataSwitch != True):
    return

  c3UtilityMethods.printFormatExtraDashes('SCANNING DOWNLOAD FOLDER INFO', p.maxColumnPrintLength, True)
  c3FileSystem.scanFilesInDirectory(p, p.dataTypeImports, p.dataUploadFolder, True)

  # c3UtilityMethods.printFormatExtraDashes('ZIPPING IMPORT FILES', p.maxColumnPrintLength, True)
  # _zipImportFiles(r, p)
  # c3UsageStats.UploadAPI.logZipFiles(r, p)

  # c3UtilityMethods.printFormatExtraDashes('CURLING UP IMPORT FILES', p.maxColumnPrintLength, True)
  # _postImportFiles(r, p)
  # c3UsageStats.UploadAPI.logCurlFiles(r, p)

  # c3UtilityMethods.printFormatExtraDashes('UPLOADING DATA TO THE ENV', p.maxColumnPrintLength, True)
  # c3TypeToBatchJobMapping = _startDataUploadToEnv(r, p)
  # _finishDataUploadToEnv(r, p, c3TypeToBatchJobMapping)
  # _cleanUpZippedImportFiles(r, p)
  # c3UsageStats.UploadAPI.logBatchJob(r, p, c3TypeToBatchJobMapping)

  # c3UtilityMethods.printFormatExtraDashes('GENERATING IMPORT QUEUE ERROR FILES', p.maxColumnPrintLength, True)
  # c3UtilityMethods.outputAllQueueErrorsFromMapping(r, p, c3TypeToBatchJobMapping, 'Import')
  # c3UsageStats.UploadAPI.logImportErrors(r, p, c3TypeToBatchJobMapping)
