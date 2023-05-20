#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import logging
from io import BytesIO
import json

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def getNextAbsoluteFilePath(dir: str):
    for dirpath, _, filenames in os.walk(dir):
        for f in filenames:
            yield {
                'fullPath': os.path.abspath(os.path.join(dirpath, f)),
                'relPath': os.path.relpath(os.path.join(dirpath, f), start=dir),
                'fileName': f
            }

class S3:

    def __init__(self, config: dict):
        self.config = config
        self.auth()

    def auth(self):
        self.session = boto3.session.Session(
            aws_access_key_id=self.config['ACCESS_KEY_ID'],
            aws_secret_access_key=self.config['SECRET_ACCESS_KEY']
        )

        self.client = self.session.client(
            service_name='s3',
            endpoint_url=self.config['ENDPOINT_URL']
        )

        self.resource = self.session.resource(
            service_name='s3',
            endpoint_url= self.config['ENDPOINT_URL']
        )

        return self

    def getAllBuckets(self):
        return self.client.list_buckets()['Buckets']

    def uploadFile(self, fileObj: object, bucketName: str, objectName:str):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        try:
            response = self.client.upload_fileobj(fileObj, bucketName, objectName)

        except ClientError as e:
            logger.error(e)
            raise ValueError('')

        return response

    def clearS3Folder(self, bucketName: str, S3FolderPath: str = None):

        _S3FolderPath = S3FolderPath

        if S3FolderPath is None:
            _S3FolderPath = ''

        self.resource.Bucket(bucketName).objects.filter(Prefix=_S3FolderPath).delete()
        return self

    def uploadFolder(self, folderPath: str, bucketName: str, S3FolderPath: str = None, rmContent: bool = True):

        #если бакет заполнен, удалить его
        if rmContent:
            self.clearS3Folder(bucketName, S3FolderPath)

        for fObj in getNextAbsoluteFilePath(folderPath):
            with open(fObj['fullPath'], 'rb') as f:

                if S3FolderPath is None:
                    fileName = '/'.join(fObj['relPath'].split('\\'))
                else:
                    fileName = S3FolderPath + '/' + '/'.join(fObj['relPath'].split('\\'))

                self.uploadFile(f, bucketName, fileName)
                f.close()

    def getObj(self, bucketName: str, objectName: str, jsonify=True):
        f = BytesIO()
        self.client.download_fileobj(bucketName, objectName, f)
        v = f.getvalue()
        if jsonify:
            return json.loads(v)

        return v