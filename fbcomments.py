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
        dataf.write(json.dumps(data, indent=2))
    print('Wrote %s' % fn)


def _load_data(d, name):
    fn = os.path.join(d, name)
    with io.open(fn, 'r', encoding='utf-8') as dataf:
        return json.load(dataf)


def _latest_data(config):
    return sorted(os.listdir(config['download_location']))[-1]


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

        comments = graph_api(config, '%s/comments' % post_id)
        _write_data(d, 'comments_%s' % post_id, comments)


def action_comment_stats(config):
    d = os.path.join(config['download_location'], _latest_data(config))
    count = 0
    for fn in os.listdir(d):
        if not fn.startswith('comments_'):
            continue
        comments = _load_data(d, fn)
        count += len(comments)
    print('%d comments' % count)


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
