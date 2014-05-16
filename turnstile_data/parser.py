import datetime
from collections import namedtuple

from stations import STATION_CODES
from collections import Counter


StationAudit = namedtuple('StationAudit',
                          'station_name, '
                          'station_lines, '
                          'start_ts, '
                          'end_ts, '
                          'type, '
                          'count'
                          )

class Parser(object):

    def __init__(self):
        self._current_station = None
        self._current_lines = None
        self._current_device_addr = None
        self._current_entry_value = -1
        self._current_exit_value = -1
        self._last_timestamp = None

    @staticmethod
    def chunk_audit_entries(seq):
        return (seq[pos:pos + 5] for pos in xrange(0, len(seq), 5))

    @staticmethod
    def parse_timestamp(date, time):
        ts = "%s %s" % (date, time)
        return datetime.datetime.strptime(ts, '%m-%d-%y %H:%M:%S')

    def parse_file(self, file):
        self._current_station = None
        self._current_lines = None
        self._current_device_addr = None
        self._current_entry_value = -1
        self._current_exit_value = -1
        self._last_timestamp = None
        
        turnstile_counts = Counter()

        with open(file, 'r') as f:
            for l in f:
                count_entries = self.parse_line(l)
                for e in count_entries:
                    turnstile_counts[(e.station_name, e.station_lines, e.start_ts.date(), e.type)] += e.count
        return turnstile_counts

    def parse_line(self, line):
        entries = [e.strip() for e in line.split(',')]
        station_code = tuple(reversed(entries[0:2]))
        station_info = STATION_CODES.get(station_code)
        if station_info is not None:
            station_name, station_lines = station_info
        else:
            print "Unable to find station mapping for %s" % str(station_code)
            return []

        device_addr = entries[2]
        register_audits = Parser.chunk_audit_entries(entries[3:])
        
        # if we've encountered a new station or device, update internal state 
        if (station_name != self._current_station or 
                station_lines != self._current_lines or
                device_addr != self._current_device_addr):
            self._current_station = station_name
            self._current_lines = station_lines
            self._current_device_addr = device_addr
            self._current_entry_value = -1
            self._current_exit_value = -1

        audit_counts = []
        for r in register_audits:
            date, time, audit_type, entry_val, exit_val = r
            entry_val = int(entry_val)
            exit_val = int(exit_val)
            ts = Parser.parse_timestamp(date, time)
            # skip non-regular audit events
            if audit_type != "REGULAR" or ts.minute != 0 or ts.second != 0:
                continue
            
            # if this is first audit entry for new device, reset entry/exit vals
            # or if there is a drop in audit values, we have no choice but to reset count
            if (self._current_entry_value == -1 or 
                    self._current_exit_value == -1 or
                    entry_val < self._current_entry_value or 
                    exit_val < self._current_exit_value):
                self._current_entry_value = entry_val
                self._current_exit_value = exit_val
            else:
                assert entry_val >= self._current_entry_value
                assert exit_val >= self._current_exit_value
                
                new_entry_count = StationAudit(
                    self._current_station,
                    self._current_lines,
                    self._last_timestamp,
                    ts, 
                    "Entry", 
                    int(entry_val) - self._current_entry_value,
                    )
                
                new_exit_count = StationAudit(
                    self._current_station,
                    self._current_lines,
                    self._last_timestamp,
                    ts,
                    "Exit",
                    int(exit_val) - self._current_exit_value,
                    )
                self._current_entry_value = entry_val
                self._current_exit_value = exit_val
                audit_counts.extend([new_entry_count, new_exit_count])

            self._last_timestamp = ts
        return audit_counts