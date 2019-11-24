import datetime as dt1
from datetime import datetime as dt2

import json

import os
import os.path

from vk.exceptions import VkAPIError

api = None

MAX_REQUEST = 100

item_dt = lambda i: dt2.fromtimestamp(i['date'])

def get_till_date(domain, bound_dt, use_owner_id=False, owner_id=0):
    global posts
    posts = list()
    for i in range(MAX_REQUEST):
        if not use_owner_id:
            d = api.wall.get(domain=domain, count=100, offset=100*i)
        else:
            d = api.wall.get(owner_id=owner_id, count=100, offset=100*i)
        if not d['items']: break
        dt = item_dt(d['items'][-1])
        if dt >= bound_dt:
            posts.extend(d['items'])
        else:
            posts.extend(filter(lambda i: item_dt(i) >= bound_dt, d['items']))
            break
    else:
        print('Warning! {0} data was not fully dumped. Adjust MAX_REQUEST'.format(domain))
    return posts

def cluster_weeks(posts, start_dt, end_dt=dt2.now()):
    weeks = dict()
    zero_dt = start_dt - dt1.timedelta(days=start_dt.isocalendar()[2]-1)
    for i in range(0, (end_dt-zero_dt).days+1, 7):
        dt = zero_dt + dt1.timedelta(days=i)
        yw = dt.isocalendar()[:2]
        weeks[yw] = {'date': dt.strftime('%Y-%m-%d'),
                     'yw': yw,
                     'items': []}
    for post in posts:
        dt = item_dt(post)
        yw = dt.isocalendar()[:2]
        try:
            weeks[yw]['items'].append(post)
        except KeyError:
            print('Post date out of range', dt)
            print(post['text'])
    return [weeks[w] for w in sorted(weeks.keys())]

def save_posts_by_weeks(weeks, suffix='', outdir='.', dump_reposts=True):
    for w in weeks:
        with open(os.path.join(outdir, '{0}_{1}.txt'.format(w['date'], suffix)), 'w') as f:
            for post in w['items']:
                f.write(post['text']+'\n')
                if dump_reposts and 'copy_history' in post.keys():
                    for repost in post['copy_history']: f.write(repost['text']+'\n')
                f.write('\n')

def dump(domain, date, dirname='', dump_json=True, dump_reposts=True, use_owner_id=False, owner_id=0):
    dt = dt2.strptime(date, '%Y-%m-%d')
    posts = get_till_date(domain, dt, use_owner_id, owner_id)
    weeks = cluster_weeks(posts, dt)
    json_path = './json_{0}'.format(dirname)
    dump_path = './dump_{0}'.format(dirname)
    if dump_json:
        if not os.path.exists(json_path): os.mkdir(json_path)
        with open(json_path+'/{0}_posts.json'.format(domain), 'w', encoding='utf-8') as f:
            f.write(json.dumps(posts, ensure_ascii=False, sort_keys=True, indent=2))
        with open(json_path+'/{0}_weeks.json'.format(domain), 'w', encoding='utf-8') as f:
            f.write(json.dumps(weeks, ensure_ascii=False, sort_keys=True, indent=2))
    if not os.path.exists(dump_path): os.mkdir(dump_path)
    save_posts_by_weeks(weeks, domain, dump_path, dump_reposts)

def dumplist(domainlist, date, dirname='', ignore=list(), dumpjson=True):
    scrapped = list()
    for domain in domainlist:
        if domain in ignore: continue
        try:
            try:
                dump(domain, date, dirname, dumpjson)
            except VkAPIError:
                owner_id = -int(domain.lstrip('clubpublic'))
                dump(domain, date, dirname, dumpjson, use_owner_id=True, owner_id = owner_id)
        except Exception as e:
            print(domain, 'failed :(', type(e), e)
        else:
            scrapped.append(domain)
            print(domain, 'dumped!')
    return scrapped

getfn = lambda fp: '.'.join(fp.split('/')[-1].split('.')[:-1])

def dump_from_links_file(links_filepath, date, ignore=list(), dumpjson=True):
    links_file = open(links_filepath)
    domains = [l.split('/')[-1].strip('\n') for l in \
             filter(lambda l: l.strip(' \n') and l[0].isalpha(),
                    links_file.readlines())]
    return dumplist(domains, date, getfn(links_filepath), ignore)

def main(access_token, links_filepath, date):
    global api
    import vk
    lname = getfn(links_filepath)
    sname = '{0}_scrapped.txt'.format(lname)
    if os.path.exists(sname):
        with open(sname) as scf:
            scrapped = list(map(lambda l: l.rstrip('\n '), scf.readlines()))
    else:
        scrapped = list()
    session = vk.Session(access_token=access_token)
    api = vk.API(session, v='5.35', lang='ru', timeout=10)
    new_scrapped = dump_from_links_file(links_filepath, date, scrapped)
    with open(sname, 'a+') as scf:
        for s in new_scrapped: scf.write(s+'\n')

if __name__ == '__main__':
    token = input('Input your token: ')
    links = input('Enter the name of links file: ')
    data = input('Enter the date you want to scrap till: ')
    main(token, links, data)
