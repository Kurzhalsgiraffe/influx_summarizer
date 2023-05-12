#!/usr/bin/python
import argparse
import logging
from datetime import datetime, timedelta, timezone
from progress.bar import Bar
from connection import Influx_v1_connection, Influx_v2_connection
from utils import Machine, convert_string_to_datetime, convert_datetime_to_string
import secret

READ_LAST_SUMMARY_FROM_VERSION = 2
SAVE_SUMMARY_TO_VERSIONS = [2]

#----------------------------------------
class Summarizer:
    def __init__(self, influx_v1:Influx_v1_connection, influx_v2:Influx_v2_connection, interval:int, name:str):
        self.influx_v1 = influx_v1
        self.influx_v2 = influx_v2
        self.interval = interval
        self.name = name
        self.starttime = None
        self.observed_machines = {} # {"mid": <machine>}

    def get_data_of_last_summary(self):
        # InfluxQL:
        if READ_LAST_SUMMARY_FROM_VERSION == 1:
            sqlStr = f"SELECT \"mid\", \"operid\", \"opertime\", \"last_operval\", \"last_timestamp\" FROM \"m2m\".\"autogen\".\"{self.name}\" GROUP BY * ORDER BY DESC LIMIT 1"
            data = self.influx_v1.influxRead(sqlStr)

            for d in data:
                for line in d:
                    self.starttime = convert_string_to_datetime(line["time"])

                    machine = self.get_machine_from_mid(line["mid"])
                    operid = line["operid"]
                    operation_time = line["opertime"]
                    last_operval = line["last_operval"]
                    last_timestamp = convert_string_to_datetime(line["last_timestamp"])
                    machine.set_operation(operid, operation_time, last_operval, last_timestamp)

        # Flux
        elif READ_LAST_SUMMARY_FROM_VERSION == 2:
            query = f'''from(bucket: "m2m")
            |> range(start: 0)
            |> filter(fn: (r) => r["_measurement"] == "{self.name}")
            |> last()
            '''
            result = {}

            data = self.influx_v2.influxRead(query)
            for table in data:
                for record in table.records:
                    self.starttime = record.values["_time"]

                    mid = record.values["mid"]
                    operid = record.values["operid"]
                    if (mid, operid) in result:
                        result[(mid, operid)][record.values["_field"]] = record.values["_value"]
                    else:
                        result[(mid, operid)] = {record.values["_field"]:record.values["_value"]}

            for mid, operid in result:
                machine = self.get_machine_from_mid(mid)
                operation_time = result[(mid, operid)].get("opertime")
                last_operval = result[(mid, operid)].get("last_operval")
                last_timestamp = convert_string_to_datetime(result[(mid, operid)].get("last_timestamp"))
                machine.set_operation(operid, operation_time, last_operval, last_timestamp)

    def calculate_summary(self):
        sqlStr  = "SELECT \"mid\", \"operid\", \"operval\" FROM \"m2m\".\"autogen\".\"records\""
        sqlStr += " WHERE time>'" + convert_datetime_to_string(self.starttime) + "'"
        sqlStr += " AND time<'" + convert_datetime_to_string(self.endtime) + "'"

        data = self.influx_v1.influxRead(sqlStr)
        for d in data:
            self.process_raw_data(d)

        self.write_data()

    def process_raw_data(self, data):
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
                    "measurement": f"{self.name}",
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

        if 1 in SAVE_SUMMARY_TO_VERSIONS:
            self.influx_v1.influxSend(data)
        if 2 in SAVE_SUMMARY_TO_VERSIONS:
            self.influx_v2.influxSend(data)

    def get_machine_from_mid(self, mid):
        if mid not in self.observed_machines:
            machine = Machine(mid)
            self.observed_machines[mid] = machine
        else:
            machine = self.observed_machines[mid]
        return machine

#----------------------------------------
def printArgs(args):
    logging.info(f'-ip       {args.ip}')
    logging.info(f'-v1port   {args.v1port}')
    logging.info(f'-v2port   {args.v2port}')
    logging.info(f'-user     {args.user}')
    logging.info(f'-org      {args.org}')
    logging.info(f'-v1passwd {args.v1passwd}')
    logging.info(f'-v2token  {args.v2token}')
    logging.info(f'-db       {args.db}')
    logging.info(f'-bucket   {args.bucket}')
    logging.info(f'-interval {args.interval}')
    logging.info(f'-name     {args.name}')
    logging.info(f'-print    {args.print}')
#----------------------------------------
def main():
    start_time = datetime.now(timezone.utc) # Time of Program Start, to calculate the execution time

    parser = argparse.ArgumentParser()
    parser.add_argument('-ip',       default=secret.ip)
    parser.add_argument('-v1port',   default=secret.influx_v1_port)
    parser.add_argument('-v2port',   default=secret.influx_v2_port)
    parser.add_argument('-user',     default=secret.user)
    parser.add_argument('-org',      default=secret.org)
    parser.add_argument('-v1passwd', default=secret.influx_v1_pw)
    parser.add_argument('-v2token',  default=secret.influx_v2_token)
    parser.add_argument('-db',       default='m2m')
    parser.add_argument('-bucket',   default='m2m')
    parser.add_argument('-interval', default=300)
    parser.add_argument('-name',     default="summary_300")
    parser.add_argument('-print',    default=False)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    printArgs(args)

    influx_v1_client = Influx_v1_connection(args.ip, args.v1port, args.user, args.v1passwd, args.db)
    influx_v2_client = Influx_v2_connection(ip=args.ip, port=args.v2port, token=args.v2token, org=args.org, bucket=args.bucket)

    if(influx_v1_client.connected()):
        summarizer = Summarizer(influx_v1=influx_v1_client, influx_v2=influx_v2_client, interval=args.interval, name=args.name)
        summarizer.get_data_of_last_summary()
        timenow = datetime.now(timezone.utc)
        #timenow = convert_string_to_datetime("2023-05-15 00:00:00") # Endzeit für Performance Tests

        # Wenn noch keine Zusammenfassung existiert, dann...
        if summarizer.starttime == None:
            #summarizer.starttime = timenow - timedelta(hours=48)  # ...fasse die letzten 48 Stunden zusammen
            summarizer.starttime = convert_string_to_datetime("2023-04-20 00:00:00") # ...fasse alles seit diesem Datum zusammen

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
