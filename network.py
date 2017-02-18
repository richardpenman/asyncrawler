# -*- coding: utf-8 -*-

import traceback, collections, os, random
from user_agent import generate_user_agent
import common
logger = common.logger



async def fetch(session, transaction, proxy=None, user_agent='asyncrawler', timeout=60, encoding=None):
    """Asynchronously download the URL
    """
    fn = session.get if transaction.data is None else session.post
    headers = transaction.headers or {}
    headers['User-Agent'] = headers.get('User-Agent', user_agent)
    try:
        async with fn(transaction.url, data=transaction.data, headers=headers, proxy=proxy, timeout=timeout) as response:
            transaction.status = response.status
            transaction.body = await response.text(encoding=encoding, errors='ignore')
    except Exception as e:
        logger.error('Fetch error: {}: {}'.format(type(e), transaction.url))
        logger.error(traceback.print_exc())
        transaction.status = transaction.status or 512



class Transaction:
    """Wrapper around a HTTP request and response
    """
    def __init__(self, url, headers=None, data=None, status=0, body=None, callback=None):
        self.url = url
        self.headers = headers
        self.data = data
        self.status = status
        self.num_errors = 0
        self.body = body
        self.callback = callback

    @property
    def callback(self):
        return self._callback

    @callback.setter
    def callback(self, value):
        if value is not None and not isinstance(value, str):
            value = value.__name__
        self._callback = value

    #def __getstate__(self):
    #    d = dict(self.__dict__)
    #    #del d['callback'] # avoid pickling the callback
    #    return d

    def __hash__(self):
        return common.hash('{} {} {}'.format(self.url, self.headers, self.data))

    def __str__(self):
        return '{}: {}'.format(self.url, self.status)

    def made(self):
        """After request is made the status will not be 0
        """
        return self.status > 0

    def can_retry(self, max_retries):
        """Returns True if can retry the request.
        Retries allowed when not a 4XX error and haven't exceeded maximum number of retries
        """
        return self.num_errors < max_retries and not 400 <= self.status < 500

    def is_error(self):
        return self.status >= 400



class ProxyManager:
    def __init__(self, proxy=None, proxies=None, proxy_file=None, max_errors=20):
        """
        max_errors: the maximum number of consecutive download errors before a proxy is discarded
        """
        self.proxies = []
        if proxy:
            self.proxies.append(proxy)
        if proxies:
            self.proxies.extend(proxies)
        if proxy_file:
            if os.path.exists(proxy_file):
                self.proxies.extend(open(proxy_file).read().splitlines())
            else:
                logger.warning('Proxy file "{}" does not exist'.format(proxy_file))
        self.errors = collections.defaultdict(int)
        self.max_errors = max_errors
        self.agents = {}

    def get(self, url):
        """Get proxy for this URL
        """
        # XXX add support to track errors by domain
        if self.proxies:
            return random.choice(self.proxies)

    def success(self, proxy):
        if proxy:
            self.errors[proxy] = 0

    def failure(self, proxy):
        if proxy:
            self.errors[proxy] += 1
            if self.errors[proxy] > this.max_errors:
                self.proxies.remove(proxy)
            
    def agent(self, proxy):
        """Get the user agent used for this proxy
        """
        try:
            agent = self.agents[proxy]
        except KeyError:
            self.agents[proxy] = agent = generate_user_agent()
        return agent
