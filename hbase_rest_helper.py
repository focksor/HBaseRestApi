#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

from base64 import b64encode
import requests

'''
@File    :   hbase_rest_helper.py
@Time    :   2020/04/05 13:54:53
@Author  :   focksor
@Version :   1.0
@Contact :   focksor@outlook.com
@Desc    :   functions for hbase rest api.you should first run [hbase rest start] to enable hbase rest api.
'''


class HBaseRest:
    def __init__(self, host="localhost", port=8080):
        self.baseUrl = "http://" + host + ":" + str(port)


    # get cluster version
    # return json like this: {'Version': '2.2.4'}
    def get_cluster_version(self):
        response = requests.get(self.baseUrl+'/version/cluster', headers={"Accept" : "application/json"})
        return response.json()

    # get cluster status    
    def get_cluster_status(self):
        response = requests.get(self.baseUrl+'/status/cluster', headers={"Accept" : "application/json"})
        return response.json()


    # get table list
    # return json like this: {'table': [{'name': 'test1'}, {'name': 'test2'}]}
    def get_table_list(self):
        response = requests.get(self.baseUrl+'/', headers={"Accept" : "application/json"})
        return response.json()


    # create a table if not exists, return status code 201 if successful.
    # add column_family to table if exists, return status code 200 if successful.
    # eg: create_table("test", "cf1", "cf2", "cf3")
    def create_table(self, table_name, *column_family_name):
        column_xml = ""
        for column in column_family_name:
            column_xml += '<ColumnSchema name="%s" />' % column
        table_xml = '<?xml version="1.0" encoding="UTF-8"?><TableSchema name="%s">%s</TableSchema>' % (table_name, column_xml)
        response = requests.post(self.baseUrl+'/%s/schema'%table_name, 
                                data=table_xml,
                                headers = {'content-type': 'text/xml'})
        
        return response.status_code


    # drop a table named table_name
    # return status code 200 if successflu
    # return status code 404 if table does not exist.
    # eg: drop_table("test")
    def drop_table(self, table_name):
        response = requests.delete(self.baseUrl+'/%s/schema' % table_name)
        
        return response.status_code

    # insert data to table
    # create column if not exist
    # change data if exist
    # return status code 200 if successful
    # return status code 404 if table of column_family not found
    def put(self, table_name, column_family, column, row_key, data):
        column_b64 = b64encode(bytes(column_family+":"+column, encoding="utf8")).decode()
        row_key_b64 = b64encode(bytes(row_key, encoding="utf8")).decode()
        data_b64 = b64encode(bytes(data, encoding="utf8")).decode()

        data_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="%s"><Cell column="%s">%s</Cell></Row></CellSet>' % (row_key_b64, column_b64, data_b64)
        response = requests.put(self.baseUrl+'/%s/fakeRow' % table_name, data=data_xml, headers = {'content-type': 'text/xml'})
        print(response.status_code)



if __name__ == "__main__":
    hbase = HBaseRest()
    hbase.put("users", "cf", "es", "row5", "1234567")
