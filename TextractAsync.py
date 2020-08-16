import sys
import time
from pprint import pprint

import boto3
import demjson


class Config:
    credentials = 'credentials'
    bucket = 'bucket'
    output = 'output'
    aws_access_key_id = 'aws_access_key_id'
    aws_secret_access_key = 'aws_secret_access_key'
    region_name = 'region_name'
    s3BucketName = 's3BucketName'
    objectName = 'objectName'
    outputFileName = 'outputFileName'


class Jobstatus:
    INPROGRESS = 'IN_PROGRESS'
    SUCCESS = 'SUCCEEDED'


class Textract(object):
    def __init__(self, fileName):
        self.config = self.read_config()
        self.fileName = fileName
        self.bucket = self.config[Config.bucket][Config.s3BucketName]
        self.region = self.config[Config.credentials][Config.region_name]
        self.aws_access_key = self.config[Config.credentials][Config.aws_access_key_id]
        self.aws_secret_key = self.config[Config.credentials][Config.aws_secret_access_key]
        self.objectName = self.config[Config.bucket].get(Config.objectName, self.fileName)
        self.textract = boto3.client('textract', aws_access_key_id=self.aws_access_key,
                                     aws_secret_access_key=self.aws_secret_key, region_name=self.region)
        self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key,
                               region_name=self.region)
        self.outputFileName = self.config[Config.output][Config.outputFileName]

    @staticmethod
    def sleep(sec=10):
        time.sleep(sec)

    @staticmethod
    def read_config():
        config = {}
        try:
            with open('./config.json') as f:
                config = demjson.decode(f.read())
        except FileNotFoundError as e:
            print(e)
            print("Config Not found")
            exit(0)
        return config

    def upload_file(self):
        try:
            self.s3.upload_file(self.fileName, self.bucket, self.objectName)
        except Exception as e:
            print(e)
            return False
        return True

    def textract_main(self):
        csv_data = self.extract_table()

        with open(self.outputFileName, "wt") as fout:
            fout.write(csv_data)
        print('CSV OUTPUT FILE: ', self.outputFileName)

    def extract_table(self):

        requestresponse = self.textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': self.bucket, 'Name': self.objectName}},
            FeatureTypes=["TABLES"])
        jobid = requestresponse['JobId']
        self.sleep(100)
        getresponse = self.textract.get_document_analysis(JobId=jobid)
        if getresponse['JobStatus'] == Jobstatus.INPROGRESS:
            while True:
                self.sleep()
                getresponse = self.textract.get_document_analysis(JobId=jobid)
                if getresponse['JobStatus'] == Jobstatus.SUCCESS:
                    break
        blocks = getresponse['Blocks']
        pprint(blocks)

        blocks_map = {}
        table_blocks = []
        for block in blocks:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                table_blocks.append(block)

        if len(table_blocks) <= 0:
            return "<b> NO Table FOUND </b>"

        csv = ''
        for index, table in enumerate(table_blocks):
            csv += self.generate_table(table, blocks_map, index + 1)
            csv += '\n\n'

        return csv

    @staticmethod
    def parse_text_from_response(result, blocks_map):
        text = ''
        if 'Relationships' in result:
            for relationship in result['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        word = blocks_map[child_id]
                        if word['BlockType'] == 'WORD':
                            text += word['Text'] + ' '
                        if word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] == 'SELECTED':
                                text += 'X '
        return text

    def generate_table(self, table_result, blocks_map, table_index):
        rows = self.get_table_structure(table_result, blocks_map)

        table_id = 'Table_' + str(table_index)

        csv = 'Table: {0}\n\n'.format(table_id)

        for row_index, cols in rows.items():

            for col_index, text in cols.items():
                csv += '{}'.format(text) + ","
            csv += '\n'

        csv += '\n\n\n'
        return csv

    def get_table_structure(self, table_result, blocks_map):
        rows = {}
        for relationship in table_result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    cell = blocks_map[child_id]
                    if cell['BlockType'] == 'CELL':
                        row_index = cell['RowIndex']
                        col_index = cell['ColumnIndex']
                        if row_index not in rows:
                            rows[row_index] = {}

                        rows[row_index][col_index] = self.parse_text_from_response(cell, blocks_map)
        return rows


if __name__ == "__main__":
    file_name = "/home/ravin/wilson invoice.pdf"
    obj = Textract(file_name)
    obj.textract_main()
