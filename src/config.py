import logging
import colorful
# ********************************************************************
# formatter
stream_formatter = logging.Formatter('{c.white_on_black}%(levelname)s{c.reset} {c.red}%(asctime)s{c.reset} {c.blue}[%(filename)s:%(funcName)s:%(lineno)d]{c.reset} %(message)s'.format(c=colorful))
# file_formatter = logging.Formatter('%(levelname)s %(asctime)s[%(filename)s:%(funcName)s:%(lineno)d]%(message)s')
# ********************************************************************

console_handler = logging.StreamHandler()
console_handler.setFormatter(stream_formatter)

# file_handler=logging.FileHandler('log_general.txt', 'w')
# file_handler.setFormatter(file_formatter)

root = logging.getLogger()
root.addHandler(console_handler)
# root.addHandler(file_handler)
root.setLevel(logging.DEBUG)
