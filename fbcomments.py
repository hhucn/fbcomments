#!/usr/bin/env python3

import argparse
import io
import json
import os
import time
import urllib.parse
import urllib.request


def _read_config(fn):
    with io.open(fn, 'r', encoding='utf-8') as cfgf:
        return json.load(cfgf)


def graph_api(config, path, params={}):
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
        data.extend(d['data'])
        if 'paging' in d and 'next' in d['paging']:
            full_url = d['paging']['next']
        else:
            break
    return data


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


def _comment_tree(comments):
    by_id = {c['id']: c for c in comments}
    root = []
    for c in comments:
        if 'parent' in c:
            by_id[c['parent']['id']].setdefault('__children', []).append(c)
        else:
            root.append(c)
    return root


def _iterate_comment_tree(comments):
    # Yields (depth, comment) tuples
    def _yield_recursive(depth, c):
        yield (depth, c)
        for child in c.get('__children', []):
            for t in _yield_recursive(depth + 1, child):
                yield t

    root = _comment_tree(comments)
    for c in root:
        for t in _yield_recursive(0, c):
            yield t


def action_download(config):
    if not os.path.exists(config['download_location']):
        os.mkdir(config['download_location'])
    d = os.path.join(
        config['download_location'],
        time.strftime('%Y-%m-%dT%H:%M:%S'))
    os.mkdir(d)
    print('Downloading to %s' % d)

    feed = graph_api(config, '%s/feed' % config['page'], params={
        'fields': 'id,message'
    })
    _write_data(d, 'feed', feed)

    for post_overview in feed:
        post_id = post_overview['id']
        post = graph_api(config, '%s' % post_id)
        _write_data(d, 'post_%s' % post_id, post)

        comments = graph_api(
            config, '%s/comments' % post_id,
            {
                'filter': 'stream',
                'fields': 'parent,id,message,created_time,from,like_count'
            })
        _write_data(d, 'comments_%s' % post_id, comments)

        likes = graph_api(
            config, '%s/likes' % post_id, {})
        _write_data(d, 'likes_%s' % post_id, comments)


def action_comment_stats(config):
    d = os.path.join(config['download_location'], _latest_data(config))
    count = 0
    for fn in os.listdir(d):
        if not fn.startswith('comments_'):
            continue
        comments = _load_data(d, fn)
        count += len(comments)
    print('%d comments' % count)


def action_write_x(config):
    import xlsxwriter
    d = os.path.join(config['download_location'], _latest_data(config))
    fn = os.path.join(d, 'comments.xlsx')

    workbook = xlsxwriter.Workbook(fn, {'strings_to_urls': False})
    worksheet = workbook.add_worksheet()
    column_names = [
        'Datum', 'Likes', 'Shares', 'Autor', 'Medium',
        'Beitrag', 'Kommentar', 'Antwort']
    for i, column_name in enumerate(column_names):
        worksheet.write(0, i, column_name)

    row = 1
    feed = _read_all(d)
    for post in feed:
        worksheet.write(row, 0, post['created_time'])
        worksheet.write(row, 1, len(post['likes']))
        worksheet.write(row, 2, post.get('shares', {'count': ''})['count'])
        worksheet.write(row, 3, post['from']['name'])
        worksheet.write(row, 4, post.get('type', 'unbekannt'))
        worksheet.write(row, 5, post['message'])
        for depth, c in _iterate_comment_tree(post['comments']):
            row += 1
            worksheet.write(row, 0, c['created_time'])
            worksheet.write(row, 1, c['like_count'])
            worksheet.write(row, 3, c['from']['name'])
            worksheet.write(row, 6 + depth, c['message'])
        row += 1

    workbook.close()
    print('Wrote %s' % fn)


def main():
    parser = argparse.ArgumentParser(
        'Download facebook comments of a public page')
    parser.add_argument(
        '-c', '--config', metavar='FILE',
        dest='config_file_location',
        help='Configuration file (in JSON format) to read from',
        default='config.json')
    parser.add_argument(
        'action', metavar='ACTION',
        help='One of download'
    )
    args = parser.parse_args()

    config = _read_config(args.config_file_location)
    globals()['action_%s' % args.action](config)


if __name__ == '__main__':
    main()
