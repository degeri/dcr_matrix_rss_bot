import configparser

config = configparser.ConfigParser()
config.read("config.ini")

def enabled(value):
    return value.lower() == "true"
