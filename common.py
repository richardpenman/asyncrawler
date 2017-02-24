# -*- coding: utf-8 -*-

import sys, os, hashlib, re, html
from datetime import datetime
import unidecode
DEBUG = '--debug' in sys.argv


def hash(s):
    """Produce consistent hash for input - in Python 3 hashes change each runtime
    """
    h = hashlib.md5(s.encode())
    return int(h.hexdigest(), base=16)


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


def regex_get(content, pattern, flag=re.DOTALL|re.IGNORECASE, default=''):
    """Helper method to extract content from regular expression

    >>> regex_get('<div><span>Phone: 029&nbsp;01054609</span><span></span></div>', r'<span>Phone:([^<>]+)')
    '029 01054609'
    >>> regex_get('<div><span>Phone: 029&nbsp;01054609</span><span></span></div>', r'<span>Phone:\s*(\d+)&nbsp;(\d+)')
    ['029', '01054609']
    """
    match = re.compile(pattern, flag).search(content)
    if match:
        result = match.groups()
        return result[0] if len(result) == 1 else result
    return default


def normalize(s, default=''):
    if s:
        if not isinstance(s, str):
            s = str(s)
        return unidecode.unidecode(html.unescape(s)).strip()
    return default


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
