from IPython import get_ipython
import sys


class IpythonDisplay(object):
    def __init__(self):
        self._ipython_shell = get_ipython()

    def stderr_flush(self):
        sys.stderr.flush()

    def error(self, error):
        sys.stderr.write("{}\n".format(error))
        self.stderr_flush()
