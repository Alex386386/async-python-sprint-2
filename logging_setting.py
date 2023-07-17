import logging


logging.basicConfig(filename='scheduler_log.log',
                    filemode='a',
                    format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger('mylog')
