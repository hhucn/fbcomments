#!/usr/bin/env python3

import argparse
import collections
import io
import json
import os
import sys
import time
import urllib.error
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


def _all_users(d):
    feed = _read_all(d)
    for post in feed:
        yield ('post', post['from'])
        for like_user in post['likes']:
            yield ('like_post', like_user)
        for c in post['comments']:
            yield ('comment', c['from'])


def _user_stats(d):
    """ Returns a list of tuples (id, name, {action: count}) """
    user_dict = {}  # Key: Id Contents: (name, {action: count})
    for action, u in _all_users(d):
        _, adict = user_dict.setdefault(
            u['id'], (u['name'], collections.Counter()))
        adict[action] += 1

    users = [(uid, udata[0], udata[1]) for uid, udata in user_dict.items()]

    def _key_actioncount(u):
        uid, uname, actions = u
        return (
            -sum(actions.values()), -actions.get('comment', 0),
            -actions.get('like_post', 0), uname, uid)
    users.sort(key=_key_actioncount)
    return users


def _comment_tree(comments):
    by_id = {c['id']: c for c in comments}
    root = []
    for c in comments:
        if 'parent' in c:
            by_id[c['parent']['id']].setdefault('__children', []).append(c)
        else:
            root.append(c)
    return root


def _xslsx_write_header(worksheet, columns, row=0):
    for i, column_name in enumerate(columns):
        worksheet.write(row, i, column_name)


def _xslsx_write_row(worksheet, row_num, row_values, row_start=0):
    for i, v in enumerate(row_values, start=row_start):
        worksheet.write(row_num, i, v)


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

    errors = []
    for post_overview in feed:
        post_id = post_overview['id']
        try:
            post = graph_api(config, '%s' % post_id)
        except urllib.error.HTTPError as he:
            if config.get('abort_on_error', True):
                raise
            else:
                errors.append((post_id, he))
                continue
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
        _write_data(d, 'likes_%s' % post_id, likes)

    if errors:
        print('The following posts failed to load:')
        for post_id, he in errors:
            print('%s (%d)' % (post_id, he.code))

        sys.exit(1)


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
    worksheet = workbook.add_worksheet('Inhalte')
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
    _xslsx_write_header(worksheet, [
        'ID', 'Name', 'Kommentare', 'Likes', 'Aktionen gesamt'])
    for row, s in enumerate(_user_stats(d), start=1):
        uid, uname, actions = s
        _xslsx_write_row(
            worksheet, row,
            [uid, uname, actions.get('comment'),
             actions.get('like_post'), sum(actions.values())])

    workbook.close()
    print('Wrote %s' % fn)


def action_count_users(config):
    by_action = collections.defaultdict(list)
    all_users = set()
    d = os.path.join(config['download_location'], _latest_data(config))
    for action, u in _all_users(d):
        by_action[action].append(u['id'])
        all_users.add(u['id'])
    print('%d unique users' % len(all_users))
    action_str = ',  '.join(
        '%s: %d entries, %d users' % (action_name, len(users), len(set(users)))
        for action_name, users in by_action.items()
    )
    print('By action: %s' % action_str)


def action_duplicate_names(config):
    id_by_name = collections.defaultdict(set)
    d = os.path.join(config['download_location'], _latest_data(config))
    for _, u in _all_users(d):
        id_by_name[u['name']].add(u['id'])
    dupls = {name: ids for name, ids in id_by_name.items() if len(ids) > 1}
    for name, ids in sorted(dupls.items(), key=(lambda t: (-len(t[1]), t[0]))):
        print("%s: %s" % (name, ', '.join(ids)))


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
