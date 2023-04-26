import pytz
from datetime import datetime

OperInfluxMap = {
    '1' :'sewing          ' ,  #   EMeas_OpSew                  = 1,    // | x | x | x |
    '2' :'starttack       ' ,  #   EMeas_OpStartTack            = 2,    // | x |   | x | 755 Nahtanfangssicherung
    '3' :'backtack        ' ,  #   EMeas_OpEndTack              = 3,    // | x |   | x | 755 Nahtendesicherung
    '4' :'manualtack      ' ,  #   EMeas_OpManualTack           = 4,    // | x |   |   |
    '5' :'trim            ' ,  #   EMeas_OpTrim                 = 5,    // | x |   |   |
    '6' :'footup_sewing   ' ,  #   EMeas_OpFootUpSew            = 6,    // | x |   |   |
    '7' :'footup_idle     ' ,  #   EMeas_OpFootUpIdle           = 7,    // | x |   |   |
    '8' :'cutter          ' ,  #   EMeas_OpCutter               = 8,    // |   | x | x | 755 Eckenmesser
    '9' :'vacuum          ' ,  #   EMeas_OpVacuum               = 9,    // |   |   | x | 755 Einlegen
    '10':'lowertransclamp ' ,  #   EMeas_OpLowerTransportClamp  = 10,   // |   |   | x |
    '11':'movingtransclamp' ,  #   EMeas_OpMovingTransportClamp = 11,   // |   |   | x |
    '12':'closeclamp      ' ,  #   EMeas_OpCloseClamp           = 12,   // |   | x | x | 755 Pattenklemmen schliessen
    '13':'lowerfoldstamp  ' ,  #   EMeas_OpLowerFoldingStamp    = 13,   // |   |   | x |
    '14':'closefoldstamp  ' ,  #   EMeas_OpCloseFoldingStamp    = 14,   // |   |   | x | 755 Faltbleche schliessen
    '15':'poweron         ' ,  #   EMeas_OpPowerOn              = 15,   // | x | x | x |
    '16':'seam            ' ,  #   EMeas_OpSeam                 = 16,   // | x | x | x |
    '21':'stop_sewing     ' ,  #   EMeas_OpStopSew              = 21,   // | x | x | x |
    '22':'stop_idle       ' ,  #   EMeas_OpStopIdle             = 22,   // | x | x | x |
    '23':'stop_maintinance' ,  #   EMeas_OpStopMaint            = 23,   // | x | x | x |
    '24':'stop_error      ' ,  #   EMeas_OpStopError            = 24,   // | x | x | x |
    '25':'stop_break      ' ,  #   EMeas_OpStopBreak            = 25,   // | x | x | x |
}

class Machine:
    def __init__(self, mid):
        self.mid = mid
        self.operations = {operid:Operation() for operid in OperInfluxMap.keys()}

    def set_operation(self, operid, operation_time, last_operval, last_timestamp):
        self.operations[operid].operation_time = operation_time
        self.operations[operid].operation_value = last_operval
        self.operations[operid].timestamp = last_timestamp

    def update_operation(self, operid, operation_value, timestamp):
        self.operations[operid].calculate_operation_time(operation_value, timestamp)

class Operation:
    def __init__(self):
        self.operation_time = 0.0
        self.operation_value = None
        self.timestamp = None

    def calculate_operation_time(self, operation_value, timestamp):
        if self.timestamp:
            if self.operation_value == True and operation_value == False:
                time_diff = (float(timestamp.timestamp()) - float(self.timestamp.timestamp())) # New timestamp minus old timestamp
                if time_diff < 0:
                    raise ValueError('Timedifference is negative. Something went wrong')
                self.operation_time += time_diff
            self.operation_value = operation_value
            self.timestamp = timestamp

        else:
            self.timestamp = timestamp
            self.operation_value = operation_value
    
    def get_operation_name_from_id(self, operid):
        return OperInfluxMap.get(operid)

def convert_string_to_datetime(date_str):
        if date_str:
            # Check if the input string contains a space instead of 'T'
            if ' ' in date_str:
                # Replace the space with 'T'
                date_str = date_str.replace(' ', 'T')

            # Strip off the timezone information if it's present
            if 'Z' in date_str:
                date_str = date_str.rstrip('Z')

            # Determine the format string based on whether the input string contains milliseconds or not
            if '.' in date_str:
                fmt = '%Y-%m-%dT%H:%M:%S.%f'
            else:
                fmt = '%Y-%m-%dT%H:%M:%S'

            # Convert the string to a datetime object
            dt = datetime.strptime(date_str, fmt)

            # Convert the datetime object to a timezone aware datetime object
            dt_aware = pytz.utc.localize(dt)

            return dt_aware
        return None

def convert_datetime_to_string(ts):
        if ts:
            return ts.strftime('%Y-%m-%d %H:%M:%S')
        return ts