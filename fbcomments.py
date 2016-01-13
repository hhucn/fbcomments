#!/usr/bin/env python3

from __future__ import unicode_literals

import argparse
import collections
import io
import itertools
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree

#
# Data structure
#
# id:           str
# text:         str
# created_time: human-readable str (optional)
# like_count:   int (optional)
# share_count:  int (optional)
# medium:       'video'|'share'|etc. (optional)
# comments:     list of entries
# author_name   str
# author_id     str (optional)


def _read_json(fn):
    with io.open(fn, 'r', encoding='utf-8') as jsonf:
        return json.load(jsonf)


def graph_api(config, path, params={}, filter_func=None):
    url = 'https://graph.facebook.com/v2.2/%s' % path
    if config.get('verbose'):
        print(url)
    params.update({
        'access_token': config['access_token'],
        'limit': '200',
    })
    full_url = url + '?' + urllib.parse.urlencode(params)
    data = []
    while True:
        with urllib.request.urlopen(full_url) as req:
            b = req.read()
        d = json.loads(b.decode('utf-8'))
        if 'data' not in d:
            assert not data
            return d
        this_page = filter(filter_func, d['data'])
        data.extend(this_page)
        if 'paging' in d and 'next' in d['paging']:
            full_url = d['paging']['next']
        else:
            break
    return data


def _download_webpage(url):
    with urllib.request.urlopen(url) as req:
        b = req.read()
        content_type = req.headers.get('Content-Type')
        encoding = 'utf-8'
        if content_type:
            content_type_charset_m = re.search(
                r';\s*charset=(UTF-?8|ISO-[0-9]+-[0-9]+)', content_type)
            if content_type_charset_m:
                encoding = content_type_charset_m.group(1)
        return b.decode(encoding)


def _write_post(d, url, post):
    assert url.startswith('http')

    bname = os.path.basename(
        re.sub(r'[^A-Za-z0-9-]+', '_', re.sub(r'^https?://', '', url)))
    _write_data(d, bname, post)


def _load_post(d, url):
    assert url.startswith('http')

    bname = os.path.basename(
        re.sub(r'[^A-Za-z0-9-]+', '_', re.sub(r'^https?://', '', url)))
    return _load_data(d, bname)


def _write_data(d, name, data):
    fn = os.path.join(d, name)
    with io.open(fn, 'w', encoding='utf-8') as dataf:
        dataf.write(json.dumps(data, indent=2, ensure_ascii=False))


def _load_data(d, name):
    fn = os.path.join(d, name)
    with io.open(fn, 'r', encoding='utf-8') as dataf:
        return json.load(dataf)


def _latest_data(config):
    return sorted(os.listdir(config['download_location']))[-1]


def _node_text(etree_node):
    return (
        ('' if etree_node.text is None else etree_node.text) +
        ''.join(_node_text(c) for c in etree_node.getchildren()) +
        ('' if etree_node.tail is None else etree_node.tail))


def _read_all(d):
    feed_index = _load_data(d, 'feed')
    feed = []
    for post_overview in feed_index:
        post_id = post_overview['id']
        post = _load_data(d, 'post_%s' % post_id)
        post['comments'] = _load_data(d, 'comments_%s' % post_id)
        post['likes'] = _load_data(d, 'likes_%s' % post_id)
        feed.append(post)
    return feed


def _all_users(feed):
    for post in feed:
        yield ('post', post['from'])
        for like_user in post['likes']:
            yield ('like_post', like_user)
        for c in post['comments']:
            yield ('comment', c['from'])


def _duplicate_names(feed):
    """ Returns a sorted list of name -> set of ids """
    id_by_name = collections.defaultdict(set)
    for _, u in _all_users(feed):
        id_by_name[u['name']].add(u['id'])
    dupls = {
        name: sorted(ids) for name, ids in id_by_name.items() if len(ids) > 1}
    return sorted(dupls.items(), key=(lambda t: (-len(t[1]), t[0])))


def _user_stats(feed):
    """ Returns a list of tuples (id, name, {action: count}) """
    user_dict = {}  # Key: Id Contents: (name, {action: count})
    for action, u in _all_users(feed):
        _, adict = user_dict.setdefault(
            u['id'], (u['name'], collections.Counter()))
        adict[action] += 1

    users = [(uid, udata[0], udata[1]) for uid, udata in user_dict.items()]

    users.sort(key=_user_key_actioncount)
    return users


def _user_key_commentcount(u):
    uid, uname, actions = u
    return (-actions.get('comment', 0), uname, uid)


def _user_key_likecount(u):
    uid, uname, actions = u
    return (-actions.get('like_post', 0), uname, uid)


def _user_key_actioncount(u):
    uid, uname, actions = u
    return (
        -sum(actions.values()), -actions.get('comment', 0),
        -actions.get('like_post', 0), uname, uid)


def _comment_tree(comments):
    by_id = {c['id']: c for c in comments}
    root = []
    for c in comments:
        if 'parent' in c:
            by_id[c['parent']['id']].setdefault('__children', []).append(c)
        else:
            root.append(c)
    return root


def _count_entry_users(feed):
    by_action = collections.defaultdict(list)
    for action, u in _all_users(feed):
        by_action[action].append(u['id'])
    return by_action


def _xslsx_write_header(worksheet, columns, row=0, column_offset=0):
    for i, column_name in enumerate(columns, column_offset):
        worksheet.write(row, i, column_name, worksheet._fbc_formats['header'])


def _xslsx_write_heading(worksheet, title, row, col):
    worksheet.write(row, col, title, worksheet._fbc_formats['heading'])


def _xslsx_write_heading_range(worksheet, title, row, col, width, height=1):
    import xlsxwriter.utility
    range_start = xlsxwriter.utility.xl_rowcol_to_cell(row, col)
    range_end = xlsxwriter.utility.xl_rowcol_to_cell(
        row + height - 1, col + width - 1)
    worksheet.merge_range(
        '%s:%s' % (range_start, range_end), title,
        worksheet._fbc_formats['heading_range'])


def _xslsx_write_row(worksheet, row_num, values, column_offset=0):
    max_height = max(
        (v.count('\n') + 1) if isinstance(v, str) else 1
        for v in values)
    worksheet.set_row(row_num, 12 * max_height)
    for i, v in enumerate(values, start=column_offset):
        worksheet.write(row_num, i, v)


def _iterate_comment_tree(comments):
    # Yields (depth, comment) tuples
    def _yield_recursive(depth, c):
        yield (depth, c)
        for child in c.get('comments', []):
            for t in _yield_recursive(depth + 1, child):
                yield t

    for c in comments:
        for t in _yield_recursive(0, c):
            yield t


def _html2xml(html):
    return (
        html.replace('&nbsp;', '&#160;').
        replace('&uuml;', '&#252;').replace('&ouml;', '&#246;'))


def action_download(config, url_groups):
    if not os.path.exists(config['download_location']):
        os.mkdir(config['download_location'])
    d = os.path.join(
        config['download_location'],
        time.strftime('%Y-%m-%dT%H:%M:%S'))
    os.mkdir(d)
    print('Downloading to %s' % d)

    for url_group in url_groups:
        for url in url_group:
            fbpost_m = re.match(
                r'(?x)^https://www\.facebook\.com/' +
                r'[^/]+/(?:posts|videos|photos/[^/]+)/([0-9]+)', url)
            if fbpost_m:
                download_facebook_post(config, d, fbpost_m.group(1), url)
            elif re.match(r'^https?://www\.zeit\.de/', url):
                download_zeit(config, d, url)
            elif re.match(r'^https?://www\.welt\.de/', url):
                download_welt(config, d, url)
            elif re.match(r'^https?://www\.spiegel\.de/', url):
                download_spiegel(config, d, url)
            elif re.match(r'^https?://www\.sueddeutsche\.de/', url):
                download_sz(config, d, url)
            else:
                assert 'http' not in url, 'URL %s is not a facebook page' % url
                download_facebook_page(config, d, url)


def download_zeit(config, d, url):
    webpage = _download_webpage(url)
    m = re.search(r'''(?x)
        <li\s+class="pager__page">\s*
        <a\s+href="(?P<paging_url>.*?\?page=)(?P<pagecount>[0-9]+)\#comments">
        \s*[0-9]+\s*
        </a>\s*</li>\s*</ul>
        ''', webpage)
    pagecount = int(m.group('pagecount'))
    paging_url = m.group('paging_url')

    title = re.search(r'''(?x)
        <span\s+class="article-heading__title">\s*(.*?)\s*</span>
        ''', webpage).group(1)
    if config.get('verbose'):
        print(title)

    comments = []
    last_toplevel = None
    for page in range(1, pagecount + 1):
        if config.get('verbose'):
            print('.. %d/%d' % (page, pagecount))
        p = _download_webpage(paging_url + str(page))
        section_xml = re.search(
            r'''(?sx)(<section\s+class="comment-section"\s+id="comments">
                .*?</section>)''', p).group(1)
        section_xml = re.sub(r'(?s)<script(.*?)</script>', '', section_xml)
        section_xml = re.sub(r'(?s)<svg(.*?)</svg>', '', section_xml)
        section_xml = _html2xml(section_xml)
        section_xml = re.sub(
            r'(<(?:img|br)[^>]*)(?<!.../)>',
            lambda m: m.group(1) + '/>',
            section_xml)
        section = xml.etree.ElementTree.fromstring(section_xml)
        for article in section.findall('.//article'):
            author_node = article.find('.//*[@class="comment-meta__name"]/a')
            author_id = re.match(
                r'.*community\.zeit\.de/user/(?P<user_id>[^/]+)$',
                author_node.attrib['href']).group('user_id')
            body = article.find('.//*[@class="comment__body"]')
            comment = {
                'id': article.attrib['id'],
                'author_id': author_id,
                'author_name': author_node.text,
                'text': _node_text(body).strip(),
                'comments': [],
            }
            is_toplevel = 'js-comment-toplevel' in article.attrib['class']
            if is_toplevel:
                comments.append(comment)
                last_toplevel = comment
            else:
                assert last_toplevel
                last_toplevel['comments'].append(comment)

    post = {
        'text': title,
        'medium': 'article',
        'comments': comments
    }
    _write_post(d, url, post)


def _download_disqus(config, disqus_forum, disqus_identifier):
    disqus_url = (
        'http://disqus.com/embed/comments/?base=default&version=' +
        config['disqus_version'] + '&f=' + disqus_forum +
        '&t_i=' + disqus_identifier + '&t_t=volk')
    disqus_embed = _download_webpage(disqus_url)
    disqus_thread = re.search(r'"thread":"([0-9]+)"', disqus_embed).group(1)

    all_comments = []
    cursor = '0:0:0'
    for page in itertools.count():
        if config.get('verbose'):
            print('.. %d' % (page + 1))
        page_url = (
            'http://disqus.com/api/3.0/threads/listPostsThreaded?' +
            urllib.parse.urlencode({
                'limit': '100',
                'thread': disqus_thread,
                'forum': disqus_forum,
                'cursor': cursor,
                'api_key': config['disqus_api_key'],
                'order': 'asc',
            })
        )
        cpage_json = _download_webpage(page_url)
        cpage = json.loads(cpage_json)
        for cdata in cpage['response']:
            c = {
                'id': cdata['id'],
                'author_name': cdata['author']['name'],
                'created_time': 'createdAt',
                'like_count': cdata['likes'],
                'text': cdata['raw_message'],
                'parent_id': cdata['parent'],
                'comments': [],
            }
            author_id = cdata['author'].get('username')
            if author_id:
                c['author_id'] = author_id
            all_comments.append(c)
        if not cpage['cursor']['hasNext']:
            break
        cursor = cpage['cursor']['next']

    comments_by_id = {int(c['id']): c for c in all_comments}

    comments = []
    for c in all_comments:
        if c['parent_id']:
            parent_id = int(c['parent_id'])
            assert parent_id in comments_by_id
            comments_by_id[parent_id]['comments'].append(c)
        else:
            comments.append(c)

    return comments


def download_welt(config, d, url):
    webpage = _download_webpage(url)
    disqus_forum = re.search(
        r"var disqus_shortname='([^']+)';", webpage).group(1)
    disqus_identifier = re.search(
        r'var\s+disqus_identifier\s*=\s*([0-9]+);', webpage).group(1)

    title = re.search(
        r'<meta property="og:title" content="([^"]+)"/>', webpage).group(1)
    if config.get('verbose'):
        print(title)

    comments = _download_disqus(config, disqus_forum, disqus_identifier)

    post = {
        'text': title,
        'medium': 'article',
        'comments': comments,
    }
    _write_post(d, url, post)


def download_spiegel(config, d, url):
    webpage = _download_webpage(url)
    title_html = re.search(
        r'(?s)<h2 class="article-title(?: lp-article-title)?">.*?</h2>',
        webpage).group(0)
    title_node = xml.etree.ElementTree.fromstring(title_html)
    title = _node_text(title_node).strip()

    if config.get('verbose'):
        print(title)

    comment_count = int(re.search(
        r'<span>\s*insgesamt ([0-9]+) Beiträge</span>', webpage).group(1))
    page_count = (comment_count + 4) // 5
    thread_id = re.search(
        r'<input type="hidden" name="threadid" value="([0-9]+)" />', webpage
    ).group(1)

    comments = []
    for page in range(1, page_count + 1):
        if config.get('verbose'):
            print('.. %d/%d' % (page, page_count))
        page_url = (
            'http://www.spiegel.de/fragments/community/spon-%s-%d.html' % (
                thread_id, page * 5))
        page_html = _download_webpage(page_url)
        page_html = '<page>%s</page>' % page_html
        page_node = xml.etree.ElementTree.fromstring(page_html)

        for c in page_node.findall('./div[@class="article-comment"]'):
            user_node = c.find('.//div[@class="article-comment-user"]/a')
            date = user_node.tail.strip()
            author_id = re.match(
                r'/forum/member-([0-9]+)\.html',
                user_node.attrib['href']).group(1)
            author_name = user_node.text

            text = _node_text(c.find(
                './/*[@class="js-article-post-full-text"]')).strip()
            comments.append({
                'text': text,
                'created_time': date,
                'author_id': author_id,
                'author_name': author_name,
                'comments': []
            })

    post = {
        'text': title,
        'medium': 'article',
        'comments': comments
    }
    _write_post(d, url, post)


def download_sz(config, d, url):
    webpage = _download_webpage(url)
    title_xml = _html2xml(re.search(
        r'<h1 itemprop="headline">.*?</h1>',
        webpage).group(0))
    title_node = xml.etree.ElementTree.fromstring(title_xml)
    title = _node_text(title_node).strip()

    if config.get('verbose'):
        print(title)

    disqus_json = re.search(
        r'class="disqus-container" data-bind=\'(\{"widget.Disqus":.*?)\'>',
        webpage).group(1)
    disqus_data = json.loads(disqus_json)['widget.Disqus']

    comments = _download_disqus(
        config, disqus_data['shortName'], disqus_data['identifier'])

    post = {
        'text': title,
        'medium': 'article',
        'comments': comments
    }
    _write_post(d, url, post)


def download_facebook_page(config, d, page):
    filter_func = None
    if config.get('feedmessage_grep'):
        def filter_func(p):
            if 'message' not in p:
                return False
            res = config['feedmessage_grep'] in p['message']
            return res

    feed = graph_api(config, '%s/feed' % page, params={
        'fields': 'id,message'
    }, filter_func=filter_func)
    _write_data(d, 'feed', feed)

    errors = []
    for post_overview in feed:
        post_id = post_overview['id']
        post_error = download_facebook_post(config, post_id)
        if post_error:
            errors.append(post_error)

    if errors:
        print('The following posts failed to load:')
        for post_id, he in errors:
            print('%s (%d)' % (post_id, he.code))

        sys.exit(1)


def download_facebook_post(config, d, post_id, url=None):
    try:
        raw_post = graph_api(config, '%s' % post_id)
    except urllib.error.HTTPError as he:
        if config.get('abort_on_error', True):
            raise
        else:
            return (post_id, he)
    _write_data(d, 'post_%s' % post_id, raw_post)

    raw_comments = graph_api(
        config, '%s/comments' % post_id,
        {
            'filter': 'stream',
            'fields': 'parent,id,message,created_time,from,like_count'
        })
    _write_data(d, 'comments_%s' % post_id, raw_comments)

    likes = graph_api(
        config, '%s/likes' % post_id, {})
    _write_data(d, 'likes_%s' % post_id, likes)

    if not url:
        return

    comments_by_id = {}
    comments = []

    for rc in raw_comments:
        c = {
            'id': rc['id'],
            'created_time': rc['created_time'],
            'text': rc['message'],
            'like_count': rc['like_count'],
            'author_id': rc['from']['id'],
            'author_name': rc['from']['name'],
            'comments': [],
        }
        comments_by_id[c['id']] = c
        if rc.get('parent'):
            parent_id = rc['parent']['id']
            parent_c = comments_by_id[parent_id]
            parent_c['comments'].append(c)
        else:
            comments.append(c)

    post = {
        'id': post_id,
        'text': raw_post.get('message', raw_post.get('name')),
        'comments': comments,
        'created_time': raw_post['created_time'],
        'like_count': (
            len(raw_post['likes']) if 'likes' in raw_post else ''),
        'share_count': raw_post.get('shares', {'count': ''})['count'],
        'author_id': raw_post['from']['id'],
        'author_name': raw_post['from']['name'],
        'medium': raw_post.get('type', 'unbekannt'),
    }
    _write_post(d, url, post)


def action_comment_stats(config):
    d = os.path.join(config['download_location'], _latest_data(config))
    count = 0
    for fn in os.listdir(d):
        if not fn.startswith('comments_'):
            continue
        comments = _load_data(d, fn)
        count += len(comments)
    print('%d comments' % count)


def action_write_page_x(config):
    import xlsxwriter
    latest_d = _latest_data(config)
    d = os.path.join(config['download_location'], latest_d)
    fn = os.path.join(d, 'stats.xlsx')

    workbook = xlsxwriter.Workbook(
        fn, {'strings_to_urls': False, 'in_memory': True})
    workbook.set_properties({
        'title': 'Facebook-Analyse von %s' % latest_d,
        'author': 'Philipp Hagemeister',
        'company': 'HHU Düsseldorf',
        'comments':
            'Erstellt mit fbcomments (https://github.com/hhucn/fbcomments)',
    })

    fbc_formats = {
        'heading': workbook.add_format({'bold': True}),
        'heading_range': workbook.add_format(
            {'bold': True, 'align': 'center'}),
        'header': workbook.add_format({'bold': True, 'bottom': 1}),
    }

    worksheet = workbook.add_worksheet('Inhalte')
    worksheet._fbc_formats = fbc_formats
    _xslsx_write_header(worksheet, [
        'ID', 'Datum', 'Likes', 'Shares', 'Autor-Id', 'Autor', 'Medium',
        'Beitrag', 'Kommentar', 'Antwort'])

    row = 1
    feed = _read_all(d)
    for post in feed:
        _xslsx_write_row(worksheet, row, [
            post['id'],
            post['created_time'],
            len(post['likes']),
            post.get('shares', {'count': ''})['count'],
            post['from']['id'],
            post['from']['name'],
            post.get('type', 'unbekannt'),
            post['message']
        ])
        for depth, c in _iterate_comment_tree(post['comments']):
            row += 1
            _xslsx_write_row(worksheet, row, [
                post['id'],
                c['created_time'],
                c['like_count']
            ])

            worksheet.write(row, 4, c['from']['id'])
            worksheet.write(row, 5, c['from']['name'])
            worksheet.write(row, 8 + depth, c['message'])
        row += 1

    worksheet = workbook.add_worksheet('Benutzer')
    worksheet._fbc_formats = fbc_formats
    _xslsx_write_header(worksheet, [
        'ID', 'Name', 'Kommentare', 'Likes', 'Aktionen gesamt'])
    user_stats = _user_stats(feed)
    for row, s in enumerate(user_stats, start=1):
        uid, uname, actions = s
        _xslsx_write_row(
            worksheet, row,
            [uid, uname, actions.get('comment'),
             actions.get('like_post'), sum(actions.values())])

    column_offset = 8
    _xslsx_write_header(
        worksheet, row=0, column_offset=column_offset + 1,
        columns=['Anzahl', 'Benutzer'])
    by_action = _count_entry_users(feed)
    all_user_actions = set(itertools.chain(*by_action.values()))
    _xslsx_write_row(
        worksheet, 1, column_offset=column_offset,
        values=[
            'Insgesamt',
            sum(len(v) for v in by_action.values()),
            len(all_user_actions)])
    _xslsx_write_row(
        worksheet, 2, column_offset=column_offset,
        values=[
            'Post likes',
            len(by_action['like_post']),
            len(set(by_action['like_post']))])
    _xslsx_write_row(
        worksheet, 3, column_offset=column_offset,
        values=[
            'Kommentare',
            len(by_action['comment']),
            len(set(by_action['comment']))])

    top_count = 30

    _xslsx_write_heading_range(
        worksheet, 'Top 30 Kommentarschreiber',
        row=5, col=column_offset, width=3)
    _xslsx_write_header(
        worksheet, ['Name', 'Id', 'Kommentare'],
        row=6, column_offset=column_offset)
    top_comments = sorted(user_stats, key=_user_key_commentcount)[:top_count]
    for row, u in enumerate(top_comments, 7):
        _xslsx_write_row(
            worksheet, values=[u[1], u[0], u[2].get('comment', 0)],
            row_num=row, column_offset=column_offset)

    _xslsx_write_heading_range(
        worksheet, 'Top 30 Liker', row=5, col=column_offset + 4, width=3)
    _xslsx_write_header(
        worksheet, ['Name', 'Id', 'Likes'],
        row=6, column_offset=column_offset + 4)
    top_likes = sorted(user_stats, key=_user_key_likecount)[:top_count]
    for row, u in enumerate(top_likes, 7):
        _xslsx_write_row(
            worksheet, values=[u[1], u[0], u[2].get('like_post', 0)],
            row_num=row, column_offset=column_offset + 4)

    _xslsx_write_heading(
        worksheet, 'Mehrfache Namen', row=10 + top_count, col=column_offset)
    for row, (name, ids) in enumerate(
            _duplicate_names(feed), start=10 + top_count + 1):
        _xslsx_write_row(
            worksheet, values=[name] + ids,
            row_num=row, column_offset=column_offset)

    workbook.close()
    print('Wrote %s' % fn)


def action_write_x(config, url_groups):
    import xlsxwriter
    latest_d = _latest_data(config)
    d = os.path.join(config['download_location'], latest_d)
    for url_group in url_groups:
        out_dir = os.path.join(d, 'comments')
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        sample_post = _load_post(d, url_group[-1])
        title = re.sub(r'[^a-zA-ZöäüßÖÄÜ 0-9_-]+', '_', sample_post['text'])
        fn = os.path.join(out_dir, '%s.xlsx' % title)

        workbook = xlsxwriter.Workbook(
            fn, {'strings_to_urls': False, 'in_memory': True})
        workbook.set_properties({
            'title': 'Kommentar-Analyse von %s' % latest_d,
            'author': 'Philipp Hagemeister',
            'company': 'HHU Düsseldorf',
            'comments':
            'Erstellt mit fbcomments (https://github.com/hhucn/fbcomments)',
        })

        fbc_formats = {
            'heading': workbook.add_format({'bold': True}),
            'heading_range': workbook.add_format(
                {'bold': True, 'align': 'center'}),
            'header': workbook.add_format({'bold': True, 'bottom': 1}),
            'cell': workbook.add_format({'text_wrap': True})
        }

        for url in url_group:
            service = re.match(
                r'^https?://(?:www\.)?([a-z0-9]+)\.[a-z]+/', url).group(1)

            worksheet = workbook.add_worksheet(service)
            worksheet._fbc_formats = fbc_formats
            _xslsx_write_header(worksheet, [
                'ID', 'Datum', 'Likes', 'Shares', 'Autor-Id', 'Autor',
                'Medium', 'Beitrag', 'Kommentar', 'Antwort'])

            post = _load_post(d, url)
            row = 1
            _xslsx_write_row(worksheet, row, [
                post.get('id', url),
                post.get('created_time', ''),
                post.get('like_count', ''),
                post.get('share_count', ''),
                post.get('author_id', ''),
                post.get('author_name', ''),
                post.get('type', ''),
                post['text']
            ])
            for depth, c in _iterate_comment_tree(post['comments']):
                row += 1
                _xslsx_write_row(worksheet, row, [
                    c.get('id', ''),
                    c.get('created_time', ''),
                    c.get('like_count', ''),
                    '',
                    c.get('author_id', ''),
                    c.get('author_name', ''),
                    c.get('medium', ''),
                    '',  # Beitrag
                ] + [''] * depth + [c['text']])

        workbook.close()
        print('Wrote %s' % fn)


def action_count_users(config):
    d = os.path.join(config['download_location'], _latest_data(config))
    feed = _read_all(d)
    by_action = _count_entry_users(feed)
    all_users = set(itertools.chain(*by_action.values()))
    print('%d unique users' % len(all_users))
    action_str = ',  '.join(
        '%s: %d entries, %d users' % (action_name, len(users), len(set(users)))
        for action_name, users in by_action.items()
    )
    print('By action: %s' % action_str)


def action_duplicate_names(config):
    d = os.path.join(config['download_location'], _latest_data(config))
    feed = _read_all(d)
    for name, ids in _duplicate_names(feed):
        print("%s: %s" % (name, ', '.join(ids)))


def main():
    action_list = [
        g[len('action_'):] for g in globals() if g.startswith('action_')]
    action_list.sort()

    parser = argparse.ArgumentParser(
        'Download facebook comments of a public page')
    parser.add_argument(
        '-c', '--config', metavar='FILE',
        dest='config_file_location',
        help='Configuration file (in JSON format) to read from',
        default='config.json')
    parser.add_argument(
        'action', metavar='ACTION',
        help='One of ' + ', '.join(action_list)
    )
    args = parser.parse_args()

    config = _read_json(args.config_file_location)
    url_groups = _read_json(config['urls_file'])
    globals()['action_%s' % args.action](config, url_groups)


if __name__ == '__main__':
    main()
