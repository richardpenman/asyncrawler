# -*- coding: utf-8 -*-


STATE_KEY = 'queue'


def clear_queue(cache):
    try:
        del cache[STATE_KEY]
    except KeyError:
        pass


def save_queue(cache, dl_queue, scrape_queue):
    dls = []
    scrapes = []
    for queue, storage in [(dl_queue, dls), (scrape_queue, scrapes)]:
        while not queue.empty():
            e = queue.get()
            storage.append(e)
            queue.task_done()
    cache[STATE_KEY] = dls, scrapes


def load_queue(cache, dl_queue, scrape_queue):
    try:
        dls, scrapes = cache[STATE_KEY]
    except KeyError:
        size = 0
    else:
        for dl in dls:
            dl_queue.put(dl)
        for scrape in scrapes:
            scrape_queue.put(scrape)
        size = len(dls) + len(scrapes)
    return size > 0
