"""
version: 1.0, date: 2023-05-19
Class configparser
This class implements functions to manage configuration file
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""
import os
# read configuration file
from configparser import ConfigParser
import random
import string

def get_cred(fileconf: str = './config.ini', section: str = 'dcsweb') -> dict:
    """ This function load the requirement credentials to login into DCS by default from config file
    Args:
        fileconf (string): path to configuration file
        section (string): section to be read from config file
    Raises:
        RuntimeError: raise exception
    Returns:
        conf: object with credentials parameters
    """
    config = ConfigParser()
    config.read(fileconf)
    conf = {}
    if config.has_section(section):
        params = config.items(section)
        for param in params:
            conf[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, fileconf))
    return conf


def set_cred(fileconf: str = './config.ini', section: str = 'dcsweb', key='password',value="empty") -> dict:
    """ This function rewrite sections into de file with new values
    Args:
        fileconf (string): path to configuration file
        section (string): section to be read from config file
    Raises:
        RuntimeError: raise exception
    Returns:
        conf: object with credentials parameters
    """
    config = ConfigParser()
    config.read(fileconf)
    conf = {}
    if config.has_section(section):
        config[section][key] = value
        with open(fileconf, 'w') as configfile:
            config.write(configfile)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, fileconf))
    return conf

if __name__ == '__main__':
    print("manage_conf class")