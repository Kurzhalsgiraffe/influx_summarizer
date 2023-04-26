#!/usr/bin/python
from influxdb import InfluxDBClient
import logging

#----------------------------------------
class influxConnection:
    def __init__(self, ip, port, user, passw, db):        
        self.mInfluxClient= InfluxDBClient(ip, port, user, passw)
        self.mDb = db
        self.influxConnect()

    def influxConnect(self):
        try:
            self.mInfluxClient.create_database(self.mDb)
            self.mInfluxConnected = True
            logging.info("Connected to Influx-Server ")
        except Exception:
            logging.error('Error: Influx-Server not connected or wrong credentials',exc_info=True)

    #----------------------------------------
    def influxDisconnect(self):
        if(self.mInfluxConnected):
            self.mInfluxClient.close()
            logging.info('Disconnected from Influx-Server')
            self.mInfluxConnected = False

    #----------------------------------------
    def influxSend(self, data):
        res = False
        if(self.mInfluxConnected):
            try:
                self.mInfluxClient.ping()
                res = self.mInfluxClient.write_points(data, database=self.mDb, time_precision='ms', batch_size=10000)
            except Exception as e:
                self.influxConnect()
                logging.info('Influx not alive')
                logging.info(e)
            res = True

        if not res:
            print('write_points res ', res)

    #----------------------------------------
    def influxRead(self, querystring):
        return self.mInfluxClient.query(querystring)

#----------------------------------------
    def sendData(self, data):
        #print(data)
        self.influxSend(data)
#----------------------------------------
    def connected(self):
        res = False
        if(self.mInfluxConnected):
            try:
                self.mInfluxClient.ping()
                res = True
            except Exception:
                res = False
        return res
