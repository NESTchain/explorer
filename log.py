import os
import logging


class LogClass():
    '''
    filepath:path
    filename:file name
    level:log level
    '''
    def __init__(self, filpath, filename,level_info):
        try:
            os.mkdir(filpath)
        except:
            pass
        self.fileName = './' + filpath + '/' + filename
        self.logHandle = logging.getLogger(filename)
        self.fc = logging.FileHandler(self.fileName)
        level = level_info.upper()
        if(level == 'DEBUG'):
            self.logHandle.setLevel(logging.DEBUG)
        if(level == 'INFO'):
            self.logHandle.setLevel(logging.INFO)
        if (level == 'WARNING'):
            self.logHandle.setLevel(logging.WARNING)
        if (level == 'ERROR'):
            self.logHandle.setLevel(logging.WARNING)
        if (level == 'CRITICAL'):
            self.logHandle.setLevel(logging.CRITICAL)
        self.formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:%(message)s')
        self.fc.setFormatter(self.formatter)
        self.logHandle.addHandler(self.fc)

    def getLogging(self):
        return self.logHandle

    def errorLog(self, msg):
        self.logHandle.error(msg)
