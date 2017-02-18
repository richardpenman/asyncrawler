# -*- coding: utf-8 -*-

import csv


class CacheWriter:
    def __init__(self, filename, header):
        self.filename = filename
        self.header = header
        self.writer = None
        self.mode = 'w' # default mode is write

    def writerow(self, record):
        """Write result to CSV
        """
        if self.writer is None:
            # need to create the writer for the first write
            self.writer = csv.writer(open(self.filename, self.mode))
            if 'a' not in self.mode:
                # not append mode so need to write the header
                self.writer.writerow(self.encode(self.header))
        if isinstance(record, dict):
            row = [record.get(field) for field in self.header]
        else:
            row = record
        self.writer.writerow(self.encode(row))

    def encode(self, row):
        return row
        #return [e.encode() for e in row]
