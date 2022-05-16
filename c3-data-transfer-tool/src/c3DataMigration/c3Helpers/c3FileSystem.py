__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import gzip
import json
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from progress.bar import IncrementalBar
from c3DataMigration.c3Helpers import c3Request
from c3DataMigration.c3Helpers import c3UtilityMethods




def getRemoteFileSystemInstance (r, p):
  url = c3Request.generateTypeActionURL(r, 'FileSystem', 'inst')
  errorCodePrefix = 'Unsuccessful retrieving instance of FileSystem'
  request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, None, errorCodePrefix)
  fileSystemInstance = ET.ElementTree(ET.fromstring(request.text)).getroot().find('./type/name').text

  return fileSystemInstance




def getRemoteImportDirectory (r, p):
  remoteFileSystemRoot = 'c3fs://' # TODO: Find this based on environment
  scriptRunnerUsername = c3UtilityMethods.getc3Context(r, p.errorSleepTimeSeconds)['username']
  directoryOnEnv = '/'.join([remoteFileSystemRoot, 'c3-cp', 'jack-data-transfer', scriptRunnerUsername]) # TODO: May also want to key off timestamp

  return directoryOnEnv




def deleteRemoteDirectory (r, p, folderPath):
  fileSystemInstance = getRemoteFileSystemInstance(r, p)
  url = c3Request.generateTypeActionURL(r, fileSystemInstance, 'deleteFiles')
  payload = {
    'this': {},
    'urlOrEncodedPath': folderPath,
    'confirm': True,
  }
  errorCodePrefix = 'Unsuccessful cleaning up deleting folder on environment'
  c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)




def deleteRemoteFiles (r, p, filePathList):
  fileSystemInstance = getRemoteFileSystemInstance(r, p)
  url = c3Request.generateTypeActionURL(r, fileSystemInstance, 'deleteFilesBatch')
  payload = {
    'this': {},
    'files': filePathList,
  }
  errorCodePrefix = 'Unsuccessful cleaning up generated files on environment'
  c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)




def deleteLocalFiles (filePaths):
  for filePath in filePaths:
    path = Path(filePath)
    if (path.exists() and path.is_file()):
      os.remove(path)




def wipeLocalDirectory (filePath, promptUser=False):
  path = Path(filePath)
  if (path.exists() and path.is_dir()):
    if (promptUser == True):
      print('Type (y/yes) to confirm directory removal: ' + filePath)
      if (not (input().lower() in ['y', 'yes'])):
        print('Exiting script.')
        exit(0)
    shutil.rmtree(path)
  path.mkdir(parents=True, exist_ok=True)




def getLocalFilePathsWithinDirectory (directory, optionalFileExtension=None):
  fullFilePaths = []
  if (os.path.isdir(directory)):
    fileNames = next(os.walk(directory), (None, None, []))[2]
    fullFilePaths = ['/'.join([directory, x]) for x in fileNames]

    if (optionalFileExtension != None):
      fullFilePaths = [x for x in fullFilePaths if (x.endswith(optionalFileExtension))]

  return fullFilePaths




def unzipC3JsonTypeFile (fullFilePath, stripMetadataAndDerived, fieldLabelMap, deleteZipFile=True):
  records = []
  try:
    with gzip.open(fullFilePath, 'rb') as gzipFile:
      gzipFileContents = gzipFile.read()
      records = json.loads(gzipFileContents.decode('utf-8'))['data']
  except:
    with open(fullFilePath, 'rb') as gzipFile:
      gzipFileContents = gzipFile.read()
      records = json.loads(gzipFileContents.decode('utf-8'))['data']

  if (stripMetadataAndDerived == True):
    c3UtilityMethods.stripMetaAndDerivedFieldsFromRecords(records, fieldLabelMap)

  with open(fullFilePath[:-len('.gz')], 'w') as extractedJsonFile:
    json.dump(records, extractedJsonFile, sort_keys=True, indent=2)
  
  if (deleteZipFile == True):
    deleteLocalFiles([fullFilePath])




def zipC3JsonTypeFile (fullFilePath, stripMetadataAndDerived, fieldLabelMap, deleteJSONFile=False):
  records = []
  with open(fullFilePath, 'r') as jsonFile:
    records = json.load(jsonFile)

  if (stripMetadataAndDerived == True):
    c3UtilityMethods.stripMetaAndDerivedFieldsFromRecords(records, fieldLabelMap)

  with gzip.open(fullFilePath + '.gz', 'w') as gzipFile:
    gzipFile.write(json.dumps(records).encode('utf-8'))

  if (deleteJSONFile == True):
    deleteLocalFiles([fullFilePath])




def unzipFilesInDirectory (r, p, downloadsDirectory, c3Types):
  for c3Type in c3Types:
    dataTypeFilesLocationFolder = '/'.join([downloadsDirectory, c3Type])

    if (os.path.isdir(dataTypeFilesLocationFolder)):
      fullFilePaths = getLocalFilePathsWithinDirectory(dataTypeFilesLocationFolder, '.gz')
      if (len(fullFilePaths) == 0):
        c3UtilityMethods.printFormatExtraPeriods('Extracting ' + c3Type, 'NO EXPORT FILES', p.maxColumnPrintLength, True)
        continue

      fieldLabelMap = c3UtilityMethods.retrieveLabeledFields(r, c3Type, p.errorSleepTimeSeconds)

      result = c3UtilityMethods.printFormatExtraPeriods('Extracting ' + c3Type, ' |████████████████████████████████|', p.maxColumnPrintLength, False)
      progressBar = IncrementalBar(''.join(result[:2]), max=len(fullFilePaths))
      for fullFilePath in fullFilePaths:
        unzipC3JsonTypeFile(fullFilePath, p.stripMetadataAndDerived, fieldLabelMap, True)
        [progressBar.next() for _ in range(1)]
      progressBar.finish()
    else:
      c3UtilityMethods.printFormatExtraPeriods('Extracting ' + c3Type, 'EXPORT JOB FAILED', p.maxColumnPrintLength, True)




def zipFilesInDirectory (r, p, uploadsDirectory, dataTypes):
  for dataType in dataTypes:
    c3Type = dataType[0]
    dataTypeFilesLocationFolder = '/'.join([uploadsDirectory, c3Type])

    if (dataType[1]['uploadData'] != True):
      c3UtilityMethods.printFormatExtraPeriods('Zipping ' + c3Type, 'UPLOAD FLAG IS FALSE', p.maxColumnPrintLength, True)
      continue

    if (os.path.isdir(dataTypeFilesLocationFolder)):
      fullFilePaths = getLocalFilePathsWithinDirectory(dataTypeFilesLocationFolder, '.json')
      if (len(fullFilePaths) == 0):
        c3UtilityMethods.printFormatExtraPeriods('Zipping ' + c3Type, 'NO IMPORT FILES', p.maxColumnPrintLength, True)
        continue

      fieldLabelMap = c3UtilityMethods.retrieveLabeledFields(r, c3Type, p.errorSleepTimeSeconds)

      result = c3UtilityMethods.printFormatExtraPeriods('Zipping ' + c3Type, ' |████████████████████████████████|', p.maxColumnPrintLength, False)
      progressBar = IncrementalBar(''.join(result[:2]), max=len(fullFilePaths))
      for fullFilePath in fullFilePaths:
        zipC3JsonTypeFile(fullFilePath, p.stripMetadataAndDerived, fieldLabelMap, False)
        [progressBar.next() for _ in range(1)]
      progressBar.finish()
    else:
      c3UtilityMethods.printFormatExtraPeriods('Zipping ' + c3Type, 'NO IMPORT FOLDER', p.maxColumnPrintLength, True)




def getCountOfRecordsAndDuplicatesAcrossFiles (filePaths):
  seen, dupes = set(), set()
  total = 0
  for filePath in filePaths:
    if (os.path.exists(filePath) and Path(filePath).is_file()):
      with open(filePath) as jsonFile:
        jsonContents = json.load(jsonFile)
        total += len(jsonContents)
        for x in jsonContents:
          if x['id'] in seen:
            dupes.add(x['id'])
          else:
            seen.add(x['id'])

  return total, list(dupes)




def scanFilesInDirectory (p, dataTypes, directory, failScriptIfDuplicates=True):
  for dataType in dataTypes:
    c3Type = dataType[0]

    if (p.outerAPICall == 'uploadAPI'):
      if (dataType[1]['uploadData'] != True):
        c3UtilityMethods.printFormatExtraPeriods('Scanning ' + c3Type, 'UPLOAD FLAG IS FALSE', p.maxColumnPrintLength, True)
        continue
    elif (p.outerAPICall == 'downloadAPI'):
      if (dataType[1]['downloadData'] != True):
        c3UtilityMethods.printFormatExtraPeriods('Scanning ' + c3Type, 'DOWNLOAD FLAG IS FALSE', p.maxColumnPrintLength, True)
        continue

    dataTypeUploadFolder = '/'.join([directory, c3Type])
    fullFilePaths = getLocalFilePathsWithinDirectory(dataTypeUploadFolder, '.json')
    gzipFilePaths = getLocalFilePathsWithinDirectory(dataTypeUploadFolder, '.gz')
    totalRecordCount, duplicateIds = getCountOfRecordsAndDuplicatesAcrossFiles(fullFilePaths)

    if ((len(duplicateIds) > 0) and failScriptIfDuplicates):
      print('Exiting script. ' + c3Type + ' has duplicate ids: ' + str(duplicateIds))
      exit(0)

    dataType[1]['files'] = fullFilePaths
    dataType[1]['gzipFiles'] = gzipFilePaths
    dataType[1]['recordCount'] = totalRecordCount

    fileStringPartsStringArr = c3UtilityMethods.printFormatExtraPeriods('Files:', '{:,}'.format(len(fullFilePaths)), 13, False)
    fileString = fileStringPartsStringArr[0] + (' ' * len(fileStringPartsStringArr[1])) + fileStringPartsStringArr[2]
    totalRecordPartsStringArr = c3UtilityMethods.printFormatExtraPeriods('Records:', '{:,}'.format(totalRecordCount), 19, False)
    totalRecordString = totalRecordPartsStringArr[0] + (' ' * len(totalRecordPartsStringArr[1])) + totalRecordPartsStringArr[2]

    suffixString = fileString + ' / ' +  totalRecordString
    c3UtilityMethods.printFormatExtraPeriods('Scanning ' + c3Type, suffixString, p.maxColumnPrintLength, True)
