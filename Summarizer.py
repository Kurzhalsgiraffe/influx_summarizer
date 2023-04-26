#!/usr/bin/python
import argparse
import logging
from datetime import datetime, timedelta, timezone
from progress.bar import Bar
from connection import influxConnection
from utils import Machine, convert_string_to_datetime, convert_datetime_to_string

version = "V01.00 01.04.2023"

#----------------------------------------
class Summarizer:
    def __init__(self, influx:influxConnection, interval:int):
        self.influx = influx
        self.interval = interval
        self.starttime = None
        self.observed_machines = {} # {"mid": <machine>}

    def get_data_of_last_summary(self):
        sqlStr = "SELECT \"mid\", \"operid\", \"opertime\", \"last_operval\", \"last_timestamp\" FROM \"m2m\".\"autogen\".\"summary\" GROUP BY * ORDER BY DESC LIMIT 1"
        #logging.info(f'query {sqlStr}')

        data = self.influx.influxRead(sqlStr)
        for d in data:
            for line in d:
                self.starttime = convert_string_to_datetime(line["time"])

                machine = self.get_machine_from_mid(line["mid"])
                operid = line["operid"]
                operation_time = line["opertime"]
                last_operval = line["last_operval"]
                last_timestamp = convert_string_to_datetime(line["last_timestamp"])
                machine.set_operation(operid, operation_time, last_operval, last_timestamp)

    def calculate_summary(self):
        sqlStr  = "SELECT \"mid\", \"operid\", \"operval\" FROM \"m2m\".\"autogen\".\"records\""
        sqlStr += " WHERE time>'" + convert_datetime_to_string(self.starttime) + "'"
        sqlStr += " AND time<'" + convert_datetime_to_string(self.endtime) + "'"
        #logging.info(f'query {sqlStr}')

        data = self.influx.influxRead(sqlStr)
        for d in data:
            self.process_data(d)

        self.write_data()

    def process_data(self, data):
        # Processing influx data and save to self.observed_machines
        for line in data:
            machine = self.get_machine_from_mid(line["mid"])
            operid = line["operid"]
            operation_value = line["operval"]
            timestamp = convert_string_to_datetime(line["time"])
            
            machine.update_operation(operid, operation_value, timestamp)

    def write_data(self):
        data = []
        for mid, machine in self.observed_machines.items():
            for operid, operation in machine.operations.items():
                json_body = {
                    "measurement": "summary",
                    "tags": {
                        "mid": int(mid),
                        "operid": int(operid)
                    },
                    "time": convert_datetime_to_string(self.endtime),
                    "fields": {
                        "opertime": operation.operation_time, 
                        "last_operval": operation.operation_value, 
                        "last_timestamp": convert_datetime_to_string(operation.timestamp)
                    }
                }
                data.append(json_body)
        self.influx.influxSend(data)

    def get_machine_from_mid(self, mid):
        if mid not in self.observed_machines:
            machine = Machine(mid)
            self.observed_machines[mid] = machine
        else:
            machine = self.observed_machines[mid]
        return machine

#----------------------------------------
def printArgs(args):
    logging.info(f'---------- {version} ----------')
    logging.info(f'-ip        {args.ip}')
    logging.info(f'-port      {args.port}')
    logging.info(f'-user      {args.user}')
    logging.info(f'-passwd    {args.passwd}')
    logging.info(f'-db        {args.db}')
    logging.info(f'-interval  {args.interval}')
    logging.info(f'-print  {args.print}')
#----------------------------------------
def main():
    start_time = datetime.now(timezone.utc) # Time of Program Start, to calculate the execution time

    parser = argparse.ArgumentParser()
    parser.add_argument('-ip',       default='')
    parser.add_argument('-port',     default='8086')
    parser.add_argument('-user',     default='')
    parser.add_argument('-passwd',   default='')
    parser.add_argument('-db',       default='m2m')
    parser.add_argument('-interval', default=30)
    parser.add_argument('-print', default=False)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    printArgs(args)

    influxClient = influxConnection(args.ip, args.port, args.user, args.passwd, args.db)
    if(influxClient.connected()):    
        summarizer = Summarizer(influx=influxClient, interval=args.interval)
        
        summarizer.get_data_of_last_summary()

        timenow = datetime.now(timezone.utc)
        if summarizer.starttime == None:
            summarizer.starttime = timenow - timedelta(hours=24)

        loop_counter = 0
        estimated_loops = int((timenow - summarizer.starttime) / timedelta(seconds=int(args.interval)))
        progressbar = Bar('Processing', max=estimated_loops)
        while(True):
            endtime = summarizer.starttime + timedelta(seconds=int(args.interval))
            if endtime <= timenow:
                summarizer.endtime = endtime
                summarizer.calculate_summary()
                summarizer.starttime = summarizer.endtime
                loop_counter += 1
            else:
                break
            progressbar.next()
        progressbar.finish()

        stop_time = datetime.now(timezone.utc)

        if args.print:
            for mid, machine in summarizer.observed_machines.items():
                logging.info(f"Machine ID: {mid}")
                for operid, operation in machine.operations.items():
                    logging.info((
                        f"{f'Operation-ID: {operid}':<17} | "
                        f"{f'Name: {operation.get_operation_name_from_id(operid)}':<23} | "
                        f"{f'Last Value: {operation.operation_value}':<18} | "
                        f"{f'Last Timestamp: {convert_datetime_to_string(operation.timestamp)}':<35} | "
                        f"{f'Operation Time: {operation.operation_time}':<20}"
                    ))

        logging.info(f"Calculated Summary of {loop_counter} Intervals in {stop_time-start_time}")

#----------------------------------------
if __name__ == "__main__":
    main()
