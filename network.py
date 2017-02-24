# -*- coding: utf-8 -*-

import traceback, collections, os, random
from user_agent import generate_user_agent
import yarl
from . import common, scrape
logger = common.logger



async def fetch(session, transaction, proxy=None, user_agent='asyncrawler', timeout=60, encoding=None):
    """Asynchronously download the URL
    """
    request_fn = session.get if transaction.data is None else session.post
    headers = transaction.headers or {}
    headers['User-Agent'] = headers.get('User-Agent', user_agent)
    try:
        url = str(yarl.URL(transaction.url))
        async with request_fn(url, data=transaction.data, headers=headers, proxy=proxy, timeout=timeout) as response:
            transaction.status = response.status
            content_type = response.headers.get('content-type')
            response_fn = response.json if 'json' in content_type else (response.text if 'text' in content_type else response.read)
            transaction.body = await response_fn(encoding=encoding, errors='ignore')
            #print('Final URL: {}'.format(response.url_obj))
    except Exception as e:
        logger.error('Fetch error: {}: {}'.format(type(e), transaction.url))
        logger.error(traceback.print_exc())
        transaction.status = transaction.status or 512



class Transaction:
    """Wrapper around a HTTP request and response
    """
    def __init__(self, url, headers=None, data=None, status=0, body=None, callback=None, **kwargs):
        self.url = url
        self.headers = headers
        self.data = data
        self.status = status
        self.num_errors = 0
        self.body = body
        self.callback = callback
        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def callback(self):
        return self._callback

    @callback.setter
    def callback(self, value):
        if value is not None and not isinstance(value, str):
            value = value.__name__
        self._callback = value

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

    def merge(self, other):
        """Merge attributes of this Transaction
        """
        for key, value in other.__dict__.items():
            if value:
                setattr(self, key, value)

    def tree(self):
        return scrape.Tree(self.body)



class ProxyManager:
    def __init__(self, proxy=None, proxies=None, proxy_file=None, max_errors=20):
        """
        max_errors: the maximum number of consecutive download errors before a proxy is discarded
        """
        self.proxies = []
        self.add(proxy)
        for proxy in proxies or []:
            self.add(proxy)
        if proxy_file:
            if os.path.exists(proxy_file):
                for proxy in open(proxy_file).read().splitlines():
                    self.add(proxy)
            else:
                logger.warning('Proxy file "{}" does not exist'.format(proxy_file))
        self.errors = collections.defaultdict(int)
        self.max_errors = max_errors
        self.agents = {}


    def add(self, proxy):
        if proxy:
            if not proxy.startswith('http'):
                proxy = 'http://' + proxy
            self.proxies.append(proxy)

    
    def get(self, url):
        """Get proxy for this URL
        """
        # XXX add support to track errors by domain
        # XXX add delay here for domain
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
