# -*- coding: utf-8 -*-

import sys, time, traceback, hashlib, collections, os, random, signal
from urllib.parse import urljoin, urldefrag
import aiohttp
import asyncio
import janus
try:
    import uvloop
except ImportError:
    pass
else:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from user_agent import generate_user_agent
import adt, common, filesystem, pdict, state, xpath
logger = filesystem.logger

SLEEP_TIME = 1 # how many seconds to wait while polling
RUNNING = True # whether crawl is running
CACHE_QUEUE = '--queue' in sys.argv


"""
test redirects
cookies
web interface

json / binary files not work for text()?
form integration:
>>> g = Grab()
>>> g.go('http://example/com/log-in')
>>> g.set_input('username', 'Foo')
>>> g.set_input('password', 'Bar')
>>> g.submit()
"""



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



def crawl_complete(dl_queue, cache_queue, scrape_queue):
    """Crawl is complete when each queue is empty with no pending items
    """
    return dl_queue.empty() and dl_queue._parent._unfinished_tasks == 0 and \
           cache_queue.empty() and cache_queue._parent._unfinished_tasks == 0 and \
           scrape_queue.empty() and scrape_queue._parent._unfinished_tasks == 0



async def crawler(task_id, session, dl_queue, cache_queue, scrape_queue, proxy_manager, max_retries=1, user_agent=None, timeout=60):
    """Asynchronously download transactions from the download queue and send results on to the cache and scrape queues
    """
    logger.debug('Start crawler: {}'.format(task_id))
    while RUNNING:
        if dl_queue.empty():
            if crawl_complete(dl_queue, cache_queue, scrape_queue):
                break
            else:
                # other workers still processing
                await asyncio.sleep(SLEEP_TIME)
                # XXX change to Condition notify?
        else:
            try:
                transaction = await dl_queue.get()
                if not transaction.made() or transaction.can_retry(max_retries):
                    # new request or retrying
                    proxy = proxy_manager.get(transaction.url)
                    user_agent = user_agent or proxy_manager.agent(proxy)
                    await fetch(session, transaction, proxy=proxy, user_agent=user_agent)
                    if transaction.is_error():
                        logger.info('Download error: {}'.format(transaction))
                        # received an error 
                        transaction.num_errors += 1
                        # add back to queue
                        await dl_queue.put(transaction)
                    else:
                        # successfuly download 
                        logger.info('Download: {}'.format(transaction))
                        await cache_queue.put(transaction)
                        await scrape_queue.put(transaction)
                else:
                    # can not retry request so cache the error
                    logger.info('Download fail: {}'.format(transaction))
                    await cache_queue.put(transaction)
            except Exception as e:
                logger.error('Crawl error: {}: {}\n{}'.format(type(e), transaction, traceback.print_exc()))
            finally:
                dl_queue.task_done()
    logger.debug('Done crawler {}'.format(task_id))
    


def threaded_cache(cache, dl_queue, cache_queue, scrape_queue):
    """This thread will load previously cached downloads and cache completed downloads
    """
    logger.debug('Start cache')
    while True:
        logger.debug('dl-size:{} cache-size:{} scrape-size:{}'.format(dl_queue.qsize(), cache_queue.qsize(), scrape_queue.qsize()))
        if cache_queue.empty():
            if not RUNNING or crawl_complete(dl_queue, cache_queue, scrape_queue):
                break
            else:
                time.sleep(SLEEP_TIME)
        else:
            try:
                transaction = cache_queue.get()
                key = hash(transaction)
                if transaction.made():
                    # save complete request to cache
                    logger.debug('Save cache: {}'.format(transaction))
                    cache[key] = transaction
                else:
                    try:
                        cached_transaction = cache[key]
                    except KeyError:
                        # still need to download request
                        logger.debug('Cache miss: {}'.format(transaction))
                        dl_queue.put(transaction)
                    else:
                        logger.debug('Load from cache: {}'.format(cached_transaction))
                        # can process cached transaction
                        # set the correct callback
                        cached_transaction.callback = transaction.callback
                        if not cached_transaction.made() or cached_transaction.is_error():
                            cached_transaction.num_errors = 0
                            dl_queue.put(cached_transaction)
                        else:
                            scrape_queue.put(cached_transaction)
            except Exception as e:
                logger.error('Cache exception: {}: {}\n{}'.format(type(e), transaction, traceback.print_exc()))
            finally:
                cache_queue.task_done() 
    logger.debug('Done cache')




def threaded_scrape(user_crawl, dl_queue, cache_queue, scrape_queue):
    """This thread will call the callback to scrape completed requests and add returned links to the download queue
    """
    logger.debug('Start scrape')
    user_crawl.seen[user_crawl.start] = True
    while RUNNING:
        if scrape_queue.empty():
            if crawl_complete(dl_queue, cache_queue, scrape_queue):
                break
            else:
                time.sleep(SLEEP_TIME)
        else:
            try:
                transaction = scrape_queue.get()
                if transaction.callback is not None:
                    logger.debug('Scrape callback: {}'.format(transaction))
                    child_transactions = getattr(user_crawl, transaction.callback)(transaction)
                    for child_transaction in child_transactions or []:
                        if child_transaction not in user_crawl.seen:
                            user_crawl.seen[child_transaction] = True
                            cache_queue.put(child_transaction)
            except Exception as e:
                logger.error('Scrape exception: {}: {}\n{}'.format(type(e), transaction, traceback.print_exc()))
            finally:
                scrape_queue.task_done()
    logger.debug('Done scrape')




class TestCrawl:
    def __init__(self):
        self.start = HttpTransaction('http://webscraping.com/blog', callback=self.crawl)
        self.seen = adt.HashDict()
        self.writer = filesystem.CacheWriter('results.csv', ['URL', 'Title'])

    def crawl(self, transaction):
        tree = xpath.Tree(transaction.body)
        title = tree.get('//title').tostring()
        self.writer.writerow([transaction.url, title])
        for link in tree.search('//a/@href'):
            link = link.tostring()
            link = urljoin(transaction.url, urldefrag(link)[0])
            if 'http://webscraping.com' in link and link.startswith('http') and not link.endswith('.jpg') and 'blog' in link:
                child_transaction = HttpTransaction(link)
                child_transaction.callback = self.crawl
                yield child_transaction




class HttpTransaction:
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



def signal_handler(signal, frame):
    # XXX need to cache term signal to finish commit
    global RUNNING
    RUNNING = False
    print('Shutting down asyncrawler - press Ctrl+C again to terminate immediately')



def run(user_crawl, num_workers=10, max_connections=10):
    loop = asyncio.get_event_loop()
    dl_queue = janus.Queue(loop=loop)
    scrape_queue = janus.Queue(loop=loop)
    cache_queue = janus.Queue(loop=loop)
    cache_filename = filesystem.get_hidden_path('cache.db')
    cache = pdict.PersistentDict(cache_filename)
    
    if CACHE_QUEUE and state.load_queue(cache, dl_queue.sync_q, scrape_queue.sync_q):
        logger.info('Loaded queue - downloads: {} scrapes: {}'.format(dl_queue.sync_q.qsize(), scrape_queue.sync_q.qsize()))
        user_crawl.writer.mode = 'a'
        pass # successfully loaded the cached queue
    else:
        logger.debug('Default queue')
        cache_queue.sync_q.put(user_crawl.start)

    signal.signal(signal.SIGINT, signal_handler)
    connector = aiohttp.TCPConnector(limit=max_connections)
    # run background thread to load from and save to cache
    proxy_manager = ProxyManager(proxy_file='proxies.txt')
    cache_future = loop.run_in_executor(None, threaded_cache, cache, dl_queue.sync_q, cache_queue.sync_q, scrape_queue.sync_q)
    # run background thread to manage scraping
    scrape_future = loop.run_in_executor(None, threaded_scrape, user_crawl, dl_queue.sync_q, cache_queue.sync_q, scrape_queue.sync_q)
    with aiohttp.ClientSession(loop=loop, connector=connector) as session:
        tasks = [crawler(task_id, session, dl_queue.async_q, cache_queue.async_q, scrape_queue.async_q, proxy_manager) for task_id in range(num_workers)]
        loop.run_until_complete(asyncio.wait(tasks))
    loop.run_until_complete(cache_future)
    loop.run_until_complete(scrape_future)
    if CACHE_QUEUE:
        logger.info('Caching queue state')
        state.save_queue(cache, dl_queue.sync_q, scrape_queue.sync_q)
    else:
        logger.debug('Clearing queue state')
        state.clear_queue(cache)
    loop.close()



def main():
    user_crawl = TestCrawl()
    run(user_crawl)


if __name__ == '__main__':
    main()
