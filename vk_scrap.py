import datetime as dt1
from datetime import datetime as dt2

import os.path

from vk.exceptions import VkAPIError

api = None

MAX_REQUEST = 30

item_dt = lambda i: dt2.fromtimestamp(i['date'])

def get_till_date(domain, bound_dt):
    global posts
    posts = list()
    for i in range(MAX_REQUEST):
        d = api.wall.get(domain=domain, count=100, offset=100*i)
        dt = item_dt(d['items'][-1])
        if dt >= bound_dt:
            posts.extend(d['items'])
        else:
            posts.extend(filter(lambda i: item_dt(i) >= bound_dt, d['items']))
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
        weeks[yw]['items'].append(post)
    return [weeks[w] for w in sorted(weeks.keys())]

def save_posts_by_weeks(weeks, suffix='', outdir='.'):
    for w in weeks:
        with open(os.path.join(outdir, '{0}_{1}.txt'.format(w['date'], suffix)), 'w') as f:
            f.write('\n\n'.join(map(lambda i: i['text'], w['items'])))

def dump(domain, date):
    dt = dt2.strptime(date, '%Y-%m-%d')
    posts = get_till_date(domain, dt)
    weeks = cluster_weeks(posts, dt)
    save_posts_by_weeks(weeks, suffix=domain, outdir='./dump')

def dumplist(domainlist, date):
    for domain in domainlist:
        try:
            dump(domain, date)
        except VkAPIError:
            print(domain, 'failed :(')
        except Exception as e:
            print('unknown error', domain, e)
        else:
            print(domain, 'dumped!')

def dump_from_links_file(links_filepath, date):
    links_file = open(links_filepath)
    domains = [l.split('/')[-1].strip('\n') for l in \
             filter(lambda l: l.startswith('https://vk.com'),
                    links_file.readlines())]
    dumplist(domains, date)

def main(access_token, links_filepath, date):
    global api
    import vk
    session = vk.Session(access_token=access_token)
    api = vk.API(session, v='5.35', lang='ru', timeout=10)
    dump_from_links_file(links_filepath, date)

if __name__ == '__main__':
    token = input('Input your token:')
    links = input('Enter the name of links file:')
    data = input('Enter the date you want to scrap till:')
    main(token, links, data)
