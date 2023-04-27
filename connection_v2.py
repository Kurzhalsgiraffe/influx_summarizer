#!/usr/bin/python
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

class Influx_v2_connection:
    def __init__(self, ip, port, token, org, bucket):        
        self.mInfluxClient= InfluxDBClient(url=f"http://{ip}:{port}", token=token, org=org)
        self.bucket = bucket
        self.org = org

    #----------------------------------------
    def influxSend(self, data):
        self.mInfluxClient.write_api(write_options=SYNCHRONOUS).write(bucket=self.bucket, record=data)

    #----------------------------------------
    def influxRead(self, querystring):
        return self.mInfluxClient.query_api().query(querystring, org=self.org)
