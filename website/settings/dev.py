# coding: utf-8
# __author__: ""

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True
DJANGO_LOG_LEVEL = DEBUG
ALLOWED_HOSTS = []

HIVE_CONNECTION = {
    "database": "elengjing",
    "host": "192.168.110.122",
    "user": "hive",
    "password": "hive1",
    "port": 10000,
    "authMechanism": "LDAP"
}

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "mpintranettool",
        "USER": "root",
        "PASSWORD": "root",
        "HOST": "localhost",
        "PORT": "3306",
    }
}

