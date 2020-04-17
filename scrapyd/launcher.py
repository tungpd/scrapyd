import sys
import os
from datetime import datetime
from multiprocessing import cpu_count

from twisted.internet import reactor, defer, protocol, error
from twisted.application.service import Service
from twisted.python import log

from scrapyd.utils import get_crawl_args, native_stringify_dict
from scrapyd import __version__
from .interfaces import IPoller, IEnvironment, ISpiderScheduler
import uuid
from scrapyd.sqlite import JsonSqliteList, JsonSqliteDict

class Launcher(Service):

    name = 'launcher'

    def __init__(self, config, app):
        dbdir = config.get('dbs_dir', 'dbs')
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)
        dbpath = os.path.join(dbdir, 'launcher.db')
        self.processes = {} 
        self.processes_dict = JsonSqliteDict(database=dbpath, table='processes')
        self.finished = JsonSqliteList(database=dbpath, table="finished_job")
        self.finished_to_keep = config.getint('finished_to_keep', 100)
        self.max_proc = self._get_max_proc(config)
        self.runner = config.get('runner', 'scrapyd.runner')
        self.app = app

    def startService(self):

        for slot in range(self.max_proc):
            if slot in self.processes_dict:
                p = self.processes_dict.pop(slot)
                msg = p['msg']
                self._spawn_process(msg, slot)
            else:
                self._wait_for_project(slot)

        log.msg(format='Scrapyd %(version)s started: max_proc=%(max_proc)r, runner=%(runner)r',
                version=__version__, max_proc=self.max_proc,
                runner=self.runner, system='Launcher')

    def _wait_for_project(self, slot):
        poller = self.app.getComponent(IPoller)
        poller.next().addCallback(self._spawn_process, slot)

    def _spawn_process(self, message, slot):
        msg = native_stringify_dict(message, keys_only=False)
        project = msg['_project']
        spider = msg['_spider']
        priority = msg['_priority']
        args = [sys.executable, '-m', self.runner, 'crawl']
        args += get_crawl_args(msg)
        e = self.app.getComponent(IEnvironment)
        env = e.get_environment(msg, slot)
        env = native_stringify_dict(env, keys_only=False)
        pp = ScrapyProcessProtocol(slot, project, spider, priority, \
            msg['_job'], env, msg=msg)
        pp.deferred.addBoth(self._process_finished, slot)
        reactor.spawnProcess(pp, sys.executable, args=args, env=env)
        self.processes[slot] = pp 
        self.processes_dict[slot] = self._get_process_dict(pp)

    def _get_process_dict(self, p):
        return {'project': p.project, 
                'pid': p.pid,
                'slot': p.slot,
                'spider': p.spider,
                'priority': p.priority,
                'job': p.job,
                'start_time': p.start_time,
                'end_time': p.end_time,
                'msg': p.msg,
                'env': p.env}
    
    def _process_finished(self, _, slot):
        scheduler = self.app.getComponent(ISpiderScheduler)
        self.processes_dict.pop(slot)
        process = self.processes.pop(slot)
        process.end_time = datetime.now()
        process_dict = self._get_process_dict(process)        
        self.finished.append(process_dict)

        del self.finished[:-self.finished_to_keep] # keep last 100 finished jobs
        msg = process.msg.copy()
        log.msg(format="process finished: %(msg)r", msg=msg)
        count = int(msg.get('count', 0))
        if count > 1:
            count-=1
            msg['count'] = str(count)
            msg['_job'] = uuid.uuid1().hex
            scheduler.schedule(msg.pop('_project'), msg.pop('_spider'), priority=float(msg.pop('_priority')), **msg)
        self._wait_for_project(slot)

    def _get_max_proc(self, config):
        max_proc = config.getint('max_proc', 0)
        if not max_proc:
            try:
                cpus = cpu_count()
            except NotImplementedError:
                cpus = 1
            max_proc = cpus * config.getint('max_proc_per_cpu', 4)
        return max_proc

class ScrapyProcessProtocol(protocol.ProcessProtocol):

    def __init__(self, slot, project, spider, priority, job, env, msg=None):
        self.slot = slot
        self.pid = None
        self.project = project
        self.spider = spider
        self.priority = priority
        self.job = job
        self.start_time = datetime.now()
        self.end_time = None
        self.env = env
        self.logfile = env.get('SCRAPY_LOG_FILE')
        self.itemsfile = env.get('SCRAPY_FEED_URI')
        self.deferred = defer.Deferred()
        self.msg = msg
    def outReceived(self, data):
        log.msg(data.rstrip(), system="Launcher,%d/stdout" % self.pid)

    def errReceived(self, data):
        log.msg(data.rstrip(), system="Launcher,%d/stderr" % self.pid)

    def connectionMade(self):
        self.pid = self.transport.pid
        self.log("Process started: ")

    def processEnded(self, status):
        if isinstance(status.value, error.ProcessDone):
            self.log("Process finished: ")
        else:
            self.log("Process died: exitstatus=%r " % status.value.exitCode)
        self.deferred.callback(self)

    def log(self, action):
        fmt = '%(action)s project=%(project)r spider=%(spider)r job=%(job)r pid=%(pid)r log=%(log)r items=%(items)r'
        log.msg(format=fmt, action=action, project=self.project, spider=self.spider,
                job=self.job, pid=self.pid, log=self.logfile, items=self.itemsfile)
