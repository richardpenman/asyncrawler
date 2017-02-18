# -*- coding: utf-8 -*-

from urllib.parse import urljoin, urldefrag
import asyncrawler, network, storage, writers, xpath


class TestCrawl:
    def __init__(self):
        self.start = network.Transaction('http://webscraping.com/blog', callback=self.crawl)
        self.seen = storage.HashDict()
        self.writer = writers.CacheWriter('results.csv', ['URL', 'Title'])

    def crawl(self, transaction):
        tree = xpath.Tree(transaction.body)
        title = tree.get('//title').tostring()
        self.writer.writerow([transaction.url, title])
        for link in tree.search('//a/@href'):
            link = link.tostring()
            link = urljoin(transaction.url, urldefrag(link)[0])
            if 'http://webscraping.com' in link and link.startswith('http') and not link.endswith('.jpg') and 'blog' in link:
                child_transaction = network.Transaction(link)
                child_transaction.callback = self.crawl
                yield child_transaction


def main():
    user_crawl = TestCrawl()
    asyncrawler.run(user_crawl)


if __name__ == '__main__':
    main()
