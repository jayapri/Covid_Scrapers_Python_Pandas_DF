import re
import time
import json
import requests
import logging
import os, sys
import errno
from contextlib import contextmanager
import uuid
from dateutil import parser as dateparser
from datetime import datetime
from datetime import date
from datetime import timedelta
from pytz import timezone
import traceback
from os.path import exists as ispath, dirname, basename, join as joinpath, abspath, sep as dirsep, isfile, splitext

dtime = type(datetime.time(datetime.now()))
DTFMT = '%Y-%m-%d %I:%M:%S %p %z'
DFMT = '%Y-%m-%d'
TFMT = "%I:%M:%S %p %z"
UTC = timezone('UTC')
DAYFIRST = (os.environ.get('DAYFIRST', 'False') == 'True')
YEARFIRST = (os.environ.get('YEARFIRST', 'False') == 'True')

DEBUG_LEVEL = logging.getLevelName(os.environ.get('DEBUG_LEVEL', 'INFO'))
TEMP_DIR = os.environ.get('TEMP_DIR', joinpath(dirname(abspath(__file__)), 'data'))
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')


def get_logger(name, level=None, formatter=None):
    logger = logging.getLogger(name)
    if not logger.handlers:
        if os.environ.get('LOG_FILE'):
            LOG_FILE = os.environ.get('LOG_FILE')
            if not ispath(LOG_FILE):
                mkdir_p(dirname(LOG_FILE))
            h = logging.handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30,
                                                          encoding='utf-8')
        else:
            h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            formatter or "%(asctime)s: name-%(name)s: func-%(funcName)s[%(lineno)s]: %(levelname)s:  %(message)s"))
        logger.addHandler(h)
    logger.setLevel(level or DEBUG_LEVEL)
    logger.propagate = False
    return logger


logger = get_logger(__file__)


def mkdir_p(path):
    """ 'mkdir -p' in Python """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


mkdir_p(TEMP_DIR)


@contextmanager
def read_file(*path):
    try:
        with open(joinpath(TEMP_DIR, *path), 'r') as the_file:
            try:
                json_data = json.load(the_file)
                yield json_data
            except (ValueError, json.decoder.JSONDecodeError):
                yield {}
    except Exception as e:
        yield {}


@contextmanager
def update_file(*path, **kwargs):
    path = joinpath(TEMP_DIR, *path)
    with open(path, 'w+') as the_file:
        json_data = {}
        try:
            json_data = json.load(the_file)
            yield json_data
        except (ValueError, json.decoder.JSONDecodeError):
            yield json_data
        finally:
            the_file.seek(0)
            if kwargs.get('log'):
                logger.info("Updating data in path: %s -- %s", path, json_data)
            json.dump(json_data, the_file, ensure_ascii=False, indent=4)
            the_file.truncate()


def to_datetime(value, units=None, dayfirst=None, yearfirst=None, tz=None):
    if value is None:
        return None
    if tz is None: tz = 'Asia/Kolkata'
    if isinstance(tz, str):
        tz = timezone(tz)
    if isinstance(value, datetime):
        if tz:
            if value.tzinfo:
                return value.astimezone(tz)
            return tz.localize(value)
        return value
    elif isinstance(value, (date, dtime)):
        value = value.strftime(DTFMT)
    if dayfirst is None: dayfirst = DAYFIRST
    if yearfirst is None: yearfirst = YEARFIRST
    try:
        value = float(value)
    except ValueError:
        pass
    if isinstance(value, str):
        if value in ['null', 'None', '__NULL__']:
            return None
        if units:
            units = make_list_from_csv(units)
            d = None
            for u in units:
                try:
                    d = datetime.strptime(value, u)
                except ValueError:
                    continue
                else:
                    break
            if not d:
                raise ValueError("Could not convert to datetime with units %s".format(units))
        else:
            try:
                if value.endswith('Z'):
                    # Avoid issue with unparsable style
                    d = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
                else:
                    try:
                        d = datetime.strptime(value, '%d-%m-%Y')
                    except ValueError:
                        try:
                            d = datetime.strptime(value, '%d/%m/%Y')
                        except ValueError:
                            d = dateparser.parse(value, dayfirst=dayfirst, yearfirst=yearfirst)
            except ValueError:
                try:
                    v = float(value)
                    d = datetime.fromtimestamp(v)
                except ValueError as e:
                    raise TypeError("Cannot parse value {} to datetime: {}".format(value, e.message))
    elif isinstance(value, (float)):
        d = datetime.fromtimestamp(value)
    else:
        raise TypeError(
            "Cannot parse value {} to datetime. Accepted types are time, date, datetime, float, str, unicode. Not {}".format(
                value, type(value)))
    if tz:
        if d.tzinfo:
            return d.astimezone(tz)
        return tz.localize(d)
    return d


def now(tz=None, as_datetime=True, units=None):
    d = datetime.now(tz=timezone(tz) if isinstance(tz, str) else timezone('Asia/Kolkata'))
    if as_datetime:
        return to_datetime(d, tz=tz, units=units)
    return d.strftime(units or DTFMT)


def download_file(url, path=None, max_chunks=1024, chunk_size=1024, raise_error_downloading=False, force=False):
    # NOTE max_chunks is 1MB
    # NOTE the stream=True parameter
    try:
        if isinstance(path, str):
            if not ispath(path):
                mkdir_p(dirname(path))
            elif isfile(path) and not force:
                ft = to_datetime(modified_time(path))
                r1 = requests.head(url)
                ut = r1.headers.get('Last-Modified')
                if ut:
                    ut = to_datetime(ut)
                    if ut < ft:
                        logger.info("File at path: %s is newer than at the URL: %s", path, url)
                        return path
            string_file = open(path, 'wb')
        else:
            string_file = StringIO.StringIO()
        r = requests.get(url, stream=True)
        num_chunks = 0
        try:
            if r.status_code >= 400:
                logger.error("Error downloading from url: %s", url)
                logger.error(r.content)
                raise FileNotFound(r.content)
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    string_file.write(chunk)
                    # f.flush() commented by recommendation from J.F.Sebastian
                    num_chunks += 1
                if num_chunks >= max_chunks:
                    raise FileSizeExceeded("File size exceeded {} Kb for URL: {}".format(max_chunks, url))
        except FileNotFound:
            if raise_error_downloading:
                raise
            if not isinstance(string_file, StringIO.StringIO):
                string_file.close()
                return path
            else:
                return string_file.getvalue()
        except:
            raise
        else:
            if not isinstance(string_file, StringIO.StringIO):
                string_file.close()
                return path
            else:
                return string_file.getvalue()
        finally:
            if not isinstance(string_file, StringIO.StringIO):
                string_file.close()
    except requests.exceptions.BaseHTTPError as e:
        if raise_error_downloading:
            raise
        return None


def make_single(inp, iterator=None, force=False, default=None, ignore_dict=False):
    """
    Function returns the first element if length is one, None if length is 0 and the list if len is more than 1
    iterator can be either list or tuple or some other iterator type
    """
    if not isinstance(inp, (list, tuple, dict)):
        return inp
    elif not ignore_dict and isinstance(inp, dict):
        inp = inp.values()
    if len(inp) == 0:
        return default
    elif len(inp) == 1:
        return inp[0]
    elif force:
        return inp[0]
    if iterator:
        return iterator(inp)
    return inp


def make_list(inp, mapper=None):
    """
    Function to convert a single object into a list
    """
    if not isinstance(inp, (list, tuple)):
        inp = [inp]
    if hasattr(mapper, '__call__'):
        return map(mapper, inp)
    return list(inp)


def make_list_from_csv(inp):
    """
    Function to convert a csv into a list
    """
    if isinstance(inp, str):
        return map(lambda x: x.strip(), inp.split(","))
    return make_list(inp)


def phone_number_validator(ph_no):
    """
    Accepts phone number of the type
    9999999999
    09999999999
    +919999999999
    +91-9999999999
    +91 9999999999
    (+91) 9999999999
    0091999999999
    999-999-9999
    (999) 999-9999
    999.999.9999
    +91-999-999-9999
    0091-999-999-9999
    01-888-888888
    011-888-88888
    0111-888-8888
    01111-888888
    01888888888
    01188888888
    01118888888
    """
    if not isinstance(ph_no, str):
        return False
    ph_no = ph_no.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
    if ph_no.startswith('00'):
        if len(ph_no) < 12 or len(ph_no) > 15:
            return False
    elif ph_no.startswith('0'):
        if len(ph_no) < 11 or len(ph_no) > 11:
            return False
    elif ph_no.startswith('+'):
        if len(ph_no) < 12 or len(ph_no) > 14:
            return False
    elif len(ph_no) > 10 or len(ph_no) < 8:
        return False
    ph_no = ph_no.replace('+', '')
    if any(not p.isdigit() for p in ph_no):
        return False
    return True


def print_error(*args):
    tr = traceback.format_exc()
    logger.error(tr)
    if args:
        logger.error(*args)
    tr = tr.strip().splitlines() + list(args)
    return tr


def make_uuid3(*ids):
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, ''.join(ids)))


class CovidIndiaHelpError(Exception):
    pass


def send(data, raise_error=True):
    POST_URL = "https://3tzqfrzicb.execute-api.us-east-1.amazonaws.com/prod-v1/message"
    HEADERS = {'Content-Type': 'application/json'}
    data = make_list(data)
    rl = []
    for i, d in enumerate(data):
        for k in ['description', 'category', 'state', 'phoneNumber']:
            if not k in d:
                if raise_error:
                    a = "Required parameter: {} not found in data row {}".format(k, i)
                    raise CovidIndiaHelpError(a)
                rl.append({"error": a})
                continue
        d['phoneNumber'] = make_list(d['phoneNumber'])
        for k in d['phoneNumber']:
            if not phone_number_validator(k):
                if raise_error:
                    a = "Phone number in request {} is not valid phone number in data row {}".format(k, i)
                    raise CovidIndiaHelpError(a)
                rl.append({"error": a})
                continue
        s, _ = splitext(basename(sys._getframe().f_back.f_code.co_filename))
        d1 = {
            'text': '{}{} available. Call tel: {}, Location:{}{}'.format(
                "{}. ".format(d.get("description", "")),
                d.get("category"),
                " or ".join(d.get("phoneNumber")),
                "{}, ".format(d.get("district")) if d.get("district") else "",
                d.get("state")
            ),
            'created_at': to_datetime(d.get('modifiedOn') or d.get('addedOn') or time.time()).isoformat(),
            'source': s
        }
        logger.info("Sending data to CovidIndiaHelp: %s", json.dumps(d1, indent=2))
        r = requests.post(POST_URL, json=make_list(d1), headers=HEADERS)
        if r.status_code >= 400:
            if raise_error:
                a = "Error in posting to CovidIndiaHelp.Info: {}".format(r.content)
                raise CovidIndiaHelpError(a)
            rl.append({"error": a})
            continue
        ret = "Sending data row: {} successful".format(i)
        logger.info(ret)
        try:
            ret = r.json().get("message", ret)
        except ValueError as e:
            pass
        rl.append({"_id": make_uuid3(d['description'], d['category'], d['state'], ','.join(d['phoneNumber'])),
                   "message": ret})
    return make_single(rl)


def save(id, data):
    if ENVIRONMENT == "dev":
        s, _ = splitext(basename(sys._getframe().f_back.f_code.co_filename))
        with update_file("{}.json".format(s)) as j:
            j[id] = data
        return data


def get(id, default=None):
    if ENVIRONMENT == "dev":
        s, _ = splitext(basename(sys._getframe().f_back.f_code.co_filename))
        with read_file("{}.json".format(s)) as j:
            return j.get(id, default)
        return default
