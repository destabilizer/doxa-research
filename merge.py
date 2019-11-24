import os.path
import os

from datetime import datetime

def cluster_group_of_files(source_clusters, time_intervals, fnpath):
    def source_of(fn):
        for s, kws in source_clusters.items():
            for kw in kws:
                if kw in fn: return s
        else:
            raise KeyError
    #source_of = {fn:s for s, fns in source_clusters.items() for fn in fns}.__getitem__
    def time_of(dt):
        for tn, ti in time_intervals.items():
            if ti[0] <= dt < ti[1]: return tn
        else:
            raise KeyError

    clusters = {'{0}_{1}'.format(s,t):[] for s in source_clusters.keys() for t in time_intervals.keys()}
    for fn in os.listdir(fnpath):
        fn = fn.split('.')[0]
        dts = fn.split('_')[0]
        name = '_'.join(fn.split('_')[1:])
        dt = datetime.strptime(dts, '%Y-%m-%d')
        try:
            s = source_of(name)
            t = time_of(dt)
        except KeyError:
            continue
        else:
            clusters['{0}_{1}'.format(s, t)].append(fn)
    return clusters


def merge_group_of_files(name, fnlist, fnpath='.'):
    with open('merge/{0}.txt'.format(name), 'w+') as mergef:
        print('going to merge', len(fnlist))
        for fn in fnlist:
            try:
                txt = open(os.path.join(fnpath, fn+'.txt')).read()
            except FileNotFoundError:
                print('not found', fn+'.txt')
            else:
                mergef.write(txt)


def main(source_clusters, time_intervals, fnpath):
    clusters = cluster_group_of_files(source_clusters, time_intervals, fnpath)
    for cname, fnlist in clusters.items():
        merge_group_of_files(cname, fnlist, fnpath)
        print('merged', cname)
    return 0

if __name__ == '__main__':
    import sys
    cl = open(input('Name of cluster file: ')).readlines()
    clusters = dict()
    for l in cl:
        n, gs = l.split(':')
        clusters[n] = list()
        for g in gs.split():
            g = g.lstrip(' \n').rstrip(' \n')
            if g: clusters[n].append(g)
    sint = input('Intervals: ')
    intervals = dict()
    ss = sint.split()
    for i in range(len(ss)-1):
        a = datetime.strptime(ss[i], '%Y-%m-%d')
        b = datetime.strptime(ss[i+1], '%Y-%m-%d')
        intervals[i] = (a, b)
    sys.exit(main(clusters, intervals, input('Path: ')))

