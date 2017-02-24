# -*- coding: utf-8 -*-

import sys, time, traceback, signal, threading
import aiohttp
import asyncio
import janus
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass
from . import common, network, storage, state
logger = common.logger

WAIT_TIME = 1 # how many seconds to wait while polling
RUNNING = True # whether crawl is running
CACHE_QUEUE = '--queue' in sys.argv



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
                c = asyncio.coroutine(lambda: not dl_queue.empty())
                await asyncio.shield(asyncio.wait_for(c(), WAIT_TIME))
        else:
            try:
                transaction = await dl_queue.get()
                if not transaction.made() or transaction.can_retry(max_retries):
                    # new request or retrying
                    proxy = proxy_manager.get(transaction.url)
                    user_agent = user_agent or proxy_manager.agent(proxy)
                    await network.fetch(session, transaction, proxy=proxy, user_agent=user_agent)
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
                logger.error('Crawl error: {}: {}\n{}'.format(type(e), transaction, traceback.print_exc() or ''))
            finally:
                dl_queue.task_done()
    logger.debug('Done crawler {}'.format(task_id))
    


def threaded_cache(cache, dl_queue, cache_queue, scrape_queue):
    """This thread will load previously cached downloads and cache completed downloads
    """
    logger.debug('Start cache')
    while RUNNING:
        logger.debug('dl-size:{} cache-size:{} scrape-size:{}'.format(dl_queue.qsize(), cache_queue.qsize(), scrape_queue.qsize()))
        if cache_queue.empty():
            if crawl_complete(dl_queue, cache_queue, scrape_queue):
                break
            else:
                cond = threading.Condition()
                cond.acquire()
                cond.wait_for(lambda: not cache_queue.empty(), WAIT_TIME)
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
                        cached_transaction.merge(transaction)
                        if not cached_transaction.made() or cached_transaction.is_error():
                            cached_transaction.num_errors = 0
                            dl_queue.put(cached_transaction)
                        else:
                            scrape_queue.put(cached_transaction)
            except Exception as e:
                logger.error('Cache exception: {}: {}\n{}'.format(type(e), transaction, traceback.print_exc() or ''))
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
                cond = threading.Condition()
                cond.acquire()
                cond.wait_for(lambda: not scrape_queue.empty(), WAIT_TIME)
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
                logger.error('Scrape exception: {}: {}\n{}'.format(type(e), transaction, traceback.print_exc() or ''))
            finally:
                scrape_queue.task_done()
    logger.debug('Done scrape')



def signal_handler(signal, frame):
    """SIGINT signal caught so need to shutdown crawl
    """
    global RUNNING
    RUNNING = False
    print('Shutting down asyncrawler - press Ctrl+C again to terminate immediately')



class BaseCrawler:
    def __init__(self):
        self.seen = storage.FakeDict()



def run(user_crawl, cache=None, num_workers=10, max_connections=10):
    """Run the given crawler
    """
    loop = asyncio.get_event_loop()
    dl_queue = janus.LifoQueue(loop=loop) # use a stack for depth first traversal, to spread requests over the website
    scrape_queue = janus.LifoQueue(loop=loop)
    cache_queue = janus.LifoQueue(loop=loop)
    cache = cache or storage.PersistentDict(common.get_hidden_path('cache.db'))
    
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
    proxy_manager = network.ProxyManager(proxy_file='proxies.txt')
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
