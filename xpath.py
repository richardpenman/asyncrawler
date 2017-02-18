__doc__ = """
"""

import re
import lxml.html


class Tree:
    """Convenience wrapper around lxml
    """
    def __init__(self, e, **kwargs):
        if isinstance(e, lxml.html.HtmlElement):
            # input is already a passed lxml tree
            self.doc = e
        else:
            try:
                # parse the input HTML
                self.doc = lxml.html.fromstring(e)
            except lxml.etree.LxmlError:
                self.doc = None

    def __eq__(self, html):
        return self.orig_html is html


    def xpath(self, path):
        return [] if self.doc is None else self.doc.xpath(path)

    def get(self, path):
        es = self.xpath(path)
        if es:
            return Tree(es[0])
        return ''

    def search(self, path):
        return [Tree(e) for e in self.xpath(path)]

    def tostring(self):
        node = self.doc
        try:
            return ''.join(filter(None, 
                [node.text] + [lxml.html.tostring(e) for e in node]
            ))
        except AttributeError as e:
            return node



class Form:
    """Helper class for filling and submitting forms
    """
    def __init__(self, form):
        self.data = {}
        for input_name, input_value in zip(search(form, '//input/@name'), search(form, '//input/@value')):
            self.data[input_name] = input_value
        for text_name, text_value in zip(search(form, '//textarea/@name'), search(form, '//textarea')):
            self.data[text_name] = text_value
        for select_name, select_contents in zip(search(form, '//select/@name'), search(form, '//select')):
            self.data[select_name] = get(select_contents, '/option[@selected]/@value')
        if '' in self.data:
            del self.data['']

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __str__(self):
        return urllib.urlencode(self.data)

    def submit(self, D, action, **argv):
        return D.get(url=action, data=self.data, **argv)



js_re = re.compile('location.href ?= ?[\'"](.*?)[\'"]')
def get_links(html, url=None, local=True, external=True):
    """Return all links from html and convert relative to absolute if source url is provided

    html:
        HTML to parse
    url:
        optional URL for determining path of relative links
    local:
        whether to include links from same domain
    external:
        whether to include linkes from other domains
    """
    def normalize_link(link):
        if urlparse.urlsplit(link).scheme in ('http', 'https', ''):
            if '#' in link:
                link = link[:link.index('#')]
            if url:
                link = urlparse.urljoin(url, link)
                if not local and common.same_domain(url, link):
                    # local links not included
                    link = None
                if not external and not common.same_domain(url, link):
                    # external links not included
                    link = None
        else:
            link = None # ignore mailto, etc
        return link
    a_links = search(html, '//a/@href')
    i_links = search(html, '//iframe/@src')
    js_links = js_re.findall(html)
    links = []
    for link in a_links + i_links + js_links:
        try:
            link = normalize_link(link)
        except UnicodeError:
            pass
        else:
            if link and link not in links:
                links.append(link)
    return links
