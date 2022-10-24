"""
 Profiling including user controlled times.
"""

from time import time
from threading import Timer

# ********************************************************************************
class ExecStats:
    """
    Statistics gathered during mining
    """

    TIMERS={}

    COUNTERS={}

    PARAMS ={}
    
    def __init__(self):
        self.time = {}
        self.numcalls = {}
        self.timer = {}
        self.counters = {}
        self.params = {}
        
        for t in self.TIMERS:
            self.time[t] = 0
            self.numcalls[t] = 0
            self.timer[t] = 0
        for c in self.COUNTERS:
            self.counters[c] = 0

        for p in self.PARAMS:
            self.params[p] = 0

    def formatStats(self):
        res='TIME MEASUREMENTS:\n{:*<40}\n'.format('')
        for t in sorted(self.time):
            res+="{:40}#calls: {:<20d}total-time: {:.8f}\n".format(t,self.numcalls[t],self.time[t])
        res+='\nCOUNTERS:\n{:*<40}\n'.format('')
        for c in sorted(self.counters):
            res+="number of {:20}{:<20d}\n".format(c,self.counters[c])
        return res

    def startTimer(self, name):
        self.timer[name] = time()
        self.numcalls[name]+=1
        #log.debug("numcalls: %d", self.numcalls[name])

    def resetTimer(self, name):
        self.time[name] = 0

    def stopTimer(self, name):
        measuredTime = time() - self.timer[name]
        #log.debug("timer %s ran for %f secs", name, measuredTime)
        self.time[name] += measuredTime

    def incr(self,name):
        self.counters[name] += 1

    def getCounter(self,name):
        return self.counters[name]

class RepeatedTimer():
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = True
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True
    
    def stop(self):
        self._timer.cancel()
        self.is_running = False

