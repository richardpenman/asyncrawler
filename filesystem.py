# -*- coding: utf-8 -*-

import os, csv, sys
from datetime import datetime

DEBUG = '--debug' in sys.argv



def get_hidden_path(filename):
    """Return a hidden path for this filename using the name of current script
    """
    dirname, script = os.path.split(sys.argv[0])
    hidden_dir = os.path.join(dirname, '.' + script.replace('.py', ''))
    if not os.path.exists(hidden_dir):
        try:
            os.mkdir(hidden_dir)
        except OSError as e:
            hidden_dir = ''
    return os.path.join(hidden_dir, filename)



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
            print(self.mode)
            if 'a' not in self.mode:
                # not append mode so need to write the header
                print(self.header)
                self.writer.writerow(self.encode(self.header))
        if isinstance(record, dict):
            row = [record.get(field) for field in self.header]
        else:
            row = record
        self.writer.writerow(self.encode(row))

    def encode(self, row):
        return row
        return [e.encode() for e in row]


class Logger:
    def __init__(self, output_file):
        self.fp = open(output_file, 'a')

    def debug(self, message):
        self._output('Debug', message, DEBUG)

    def info(self, message):
        self._output('Info', message, True)

    def warning(self, message):
        self._output('Warning', message, True)

    def error(self, message):
        self._output('Error', message, True)

    def _output(self, prefix, message, display):
        s = '{}: {}'.format(prefix, message)
        self.fp.write('{}: {}\n'.format(datetime.now(), message))
        #self.fp.flush()
        if display:
            print(s)
logger = Logger(get_hidden_path('asyncrawler.log'))
