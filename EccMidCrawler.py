#!/usr/bin/env python
import json
import requests
import os
import sys
import threading
import logging
import shutil
import argparse

reload(sys)
sys.setdefaultencoding('utf8')

MAX_TITLE_LENGTH = 50

def truncate(title):
    return title[:MAX_TITLE_LENGTH]

def fetchJson(url):
    headers = {'Authorization': 'Basic m-events:kims',
               'Accept': 'application/json, text/javascript, */*; q=0.01'}
    page = requests.get(url, headers = headers)
    if (page.status_code != requests.codes.ok):
        logging.warning("%s is not reachable: %d", url, page.status_code)
        return None
    content = json.loads(page.content)
    return content

SlideDownloadHost = "http://edge.conference2web.com/"
class SlideDownloadHelper:
    import urllib3
    SlideDownloadConn = urllib3.connection_from_url(SlideDownloadHost, maxsize=4)
    ReadChunkSize = 512

    @classmethod
    def getResponseFrom(cls, path_id, filename):
        url = '{0}data/eccmid/data/talks/{1}/{2}'.format(SlideDownloadHost, path_id, filename)
        logging.info("Downloading file: %s", url)
        from urllib3 import Timeout, Retry
        r = cls.SlideDownloadConn.request('GET', url, timeout=Timeout(30),retries=Retry(4))
        return r

    @classmethod
    def downloadSildeJPG(cls, path_id, filename, local_path):
        local_file = os.path.join(local_path, filename)
        if os.path.exists(local_file):
            statinfo = os.stat(local_file)
            if (statinfo.st_size > 0):
                logging.info("%s exists, skip download", local_file)
                return
            else:
                os.remove(local_file)

        r = cls.getResponseFrom(path_id, filename)
        f = open(local_file, 'wb', 0)
        f.write(r.data)
        f.close()
        '''
        Following would block, not show why
        with open(local_file, 'wb', 0) as out:
            while True:
                data = r.read(cls.ReadChunkSize)
                if data is None:
                    break
                out.write(data)
        r.release_conn()
        '''

    @classmethod
    def parseStructureXML(cls, path_id):
        r = cls.getResponseFrom(path_id, 'structure.xml')
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.data)
        return root

def CrawlByDay(day, index):
    try:
        logging.info("Crawling day: %s" % day)
        if os.path.exists(day):
            if checkIfDone(day) and not index:
                logging.info("%s is already done.", day)
                return
        else:
            os.makedirs(day)
        url = "http://api-lb.virtual-meeting.net/v1/rooms.json?per_page=24&event_id=25&include_contentsessions=1&starts_at_from={0}&page=1".\
                format(day)
        session_in_day = fetchJson(url)
        allGood = True
        indexFile = None
        if index:
            indexFile = open(os.path.join(day, 'menu.txt'), 'w')
            indexFile.write("Menu for %s\n" % day)
        for hall in session_in_day:
            for session in hall['contentsessions']:
                title = session['title']
                title = title.decode('utf-8')
                id = session['id']
                try:
                    logging.info("session title: %s, id: %d\n", title, id)
                    if not index:
                        CrawlSession(day, id, title)
                    else:
                        indexFile.write("%i............................%s\n" % (id, title))
                except Exception as e:
                    logging.exception("(%s, %d(%s)) failed", day, id, title)
                    allGood = False
    except Exception as e:
        logging.exception("Error crawling for %s", day)
        pass
    else:
        if allGood:
            markDone(day)

def CrawlSession(day, sessionId, title):
    local_dir = os.path.join(day, str(sessionId))
    if os.path.exists(local_dir):
        if checkIfDone(local_dir):
            logging.info("(%s, %d, %s) is already done.", day, sessionId, title.decode('utf-8'))
            return
    else:
        os.makedirs(local_dir)
    url = "http://api-lb.virtual-meeting.net/v1/contentsessions/{0}.json?include_contents=1".\
            format(sessionId)
    session = fetchJson(url)
    lectures = session['contents']
    allGood = True
    for lect in lectures:
        title = lect['slug']
        try:
            CrawlResource(day, sessionId, title)
        except Exception as e:
            logging.exception("Error Crawling resource %s on %s of %d", title, day, sessionId)
            allGood = False
    if allGood:
        markDone(local_dir)
    else:
        raise Exception("%s %d(%s) met issues in one of its resource download." % (day, sessionId, title))

def CrawlResource(day, sessionId, title):
    local_dir = os.path.join(day, str(sessionId), truncate(title))
    if os.path.exists(local_dir):
        if checkIfDone(local_dir):
            logging.info("%s has already been downloaded.", title)
            return
    else:
        os.makedirs(local_dir)
    resource_url = "http://api-lb.virtual-meeting.net/v1/resources/{0}.json".format(title)
    logging.info("resource url: {0}\n".format(resource_url))
    resource = fetchJson(resource_url)
    if not resource:
        logging.warning("%s is not accessible" % title)
    title = resource['title']
    title = title.decode('utf-8')
    path_id = resource['path']
    resource_type = resource['resourcetype']['title']
    if not path_id:
        noteAndLeave(local_dir, "'%s' is not authorized publicly.\n" % title)
        return
    if resource_type == 'eposter':
        noteAndLeave(local_dir, '"%s" is of type %s, not a ppt, we skipped it.' % (title, resource_type))
        return
    root = SlideDownloadHelper.parseStructureXML(path_id)
    import base64
    import urllib
    allGood = True
    for slide in root:
        try:
            slideNum = slide.findtext("SlideNumber")
            if not slideNum:
                logging.warning("%s, %d, %s has empty slide number.", day, sessionId, title)
                continue
            slideNum  = base64.b64decode(slideNum)
            slidePath = slide.findtext("SlidePath")
            if not slidePath:
                logging.warning("%s, %d, %s has empty slide path.", day, sessionId, title)
                continue
            slidePath = base64.b64decode(slidePath)
            if not slidePath:
                logging.warning("met an empty slide path after base64, for slide %s", slideNum)
                continue
            SlideDownloadHelper.downloadSildeJPG(path_id, slidePath, local_dir)
            for animationShape in slide.findall("AnimationShape"):
                try:
                    shape_name = animationShape.findtext("Url")
                    if not shape_name:
                        logging.warning("met an empty shape name, for slide %s", slideNum)
                        continue
                    shape_name  = base64.b64decode(shape_name)
                    if not shape_name:
                        logging.warning("met an empty slide path, for %s's animation", slideNum)
                        continue
                    SlideDownloadHelper.downloadSildeJPG(path_id, shape_name, local_dir)
                except Exception as e:
                    logging.exception("Not able to fetch animation for slideNum %s", slideNum)
                    allGood = False
        except Exception as e:
            logging.exception("Not able to fetch slideNum %s for resource (%s, %d, %s)",
                              slideNum, day, sessionId, title)
            allGood = False
    logging.info("Done with %s, allGood=%s\n", title, allGood)
    if allGood:
        markDone(local_dir)
    else:
        raise Exception("Resource %s, %d, %s met issues, may need redo" % (day, sessionId, title))

def noteAndLeave(local_dir, note):
    logging.info(note)
    f = open(os.path.join(local_dir, "readme.txt"), 'w')
    f.write(note.decode('utf-8'))
    f.close()
    markDone(local_dir)

def markDone(local_dir):
    import datetime
    current_time = datetime.datetime.now().time()
    f = open(os.path.join(local_dir, "done.txt"), 'w')
    f.write("Done at %s." % current_time.isoformat())
    f.close()

def checkIfDone(local_dir):
    return os.path.exists(os.path.join(local_dir, "done.txt"))

def Crawling():
    parser = argparse.ArgumentParser(description='Download all ppts from eccmid website for GTT')
    parser.add_argument('date', metavar='date', nargs='+', help='The date to process, e.g. "2016-04-09"')
    parser.add_argument('--index', dest='index', action='store_true', help='Index the folder, generating a txt file, in which each line is like 5807....<title> udner  the date folder')
    args = parser.parse_args()
    threads = []
    for day in args.date:
        threads.append(threading.Thread(target=CrawlByDay, args=(day, args.index)))
    [t.start() for t in threads]
    [t.join() for t in threads]

if __name__ == '__main__':
    import calendar
    import time
    logging.basicConfig(filename=('crawl.%s.log' % calendar.timegm(time.gmtime())),
                        level=logging.DEBUG,
                        format='%(asctime)s %(thread)d %(levelname)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    Crawling()
