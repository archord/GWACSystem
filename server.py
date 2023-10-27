from socket import socket
# from simple_socket_server import SimpleSocketServer
import simple_socket_server as sss
from datetime import datetime
import logging
import time

verbose = True
log = logging.getLogger() #create logger
log.setLevel(logging.DEBUG) #set level of logger
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s") #set format of logger
logging.Formatter.converter = time.gmtime #convert time in logger to UCT
filehandler = logging.FileHandler("serverCommunication.log", 'a+')
filehandler.setFormatter(formatter) #add format to log file
log.addHandler(filehandler) #link log file to logger
if verbose:
    streamhandler = logging.StreamHandler() #create print to screen logging
    streamhandler.setFormatter(formatter) #add format to screen logging
    log.addHandler(streamhandler) #link logger to screen logging

socket_server = sss.SimpleSocketServer()

def getUtcTimeStr():
    timeStr = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
    timeStr = timeStr.replace(" ", "T")
    return timeStr

@socket_server.on('connect')
def on_connect(sock: socket, peer):
    ip, port=peer
    timeStr = getUtcTimeStr()
    returnStr = "connect server success: %s\r\n"%(timeStr)
    logStr = "%s, new connection from %s:%s"%(timeStr, ip, port)
    log.info(logStr)
    # socket_server.send(sock, bytes(returnStr, 'utf-8'))

@socket_server.on('disconnect')
def on_disconnect(_sock, peer):
    ip, port=peer
    timeStr = getUtcTimeStr()
    logStr = "%s, connection closed %s:%s"%(timeStr, ip, port)
    log.info(logStr)

@socket_server.on('message')
def on_message(sock: socket, peer, message: bytes):

    ip, port=peer
    timeStr = getUtcTimeStr()
    logStr = "%s, receive data from %s:%s, %s"%(timeStr, ip, port, message)
    log.debug(logStr)

    # returnStr = "receive data success: %s\r\n"%(timeStr)
    # socket_server.send(sock, bytes(returnStr, 'utf-8'))

@socket_server.on('onfocus')
def on_focus(sock: socket, peer, dbrows: list):

    ip, port=peer
    timeStr = getUtcTimeStr()
    print("onfocus: get %d rows"%(len(dbrows)))

    for trow in dbrows:
        unit_id = trow[0]
        group_id = trow[1]
        camname = trow[2]
        time_obs_ut = trow[3]
        fwhm = trow[4]
        astro_flag = trow[5]
        obj_num = trow[6]
        bg_bright = trow[7]
        s2n = trow[8]
        avg_limit = trow[9]
        isp_id = trow[10]
        # print(isp_id, unit_id,group_id,camname,time_obs_ut,fwhm,astro_flag,obj_num,bg_bright,s2n,avg_limit)
        # logStr = "%s, send fwhm to %s:%s, %s"%(timeStr, ip, port, fwhm)
        # log.debug(logStr)
        if fwhm>=100:
            fwhm=99
        tstrs = time_obs_ut.split(" ")
        dateStr1 = tstrs[0]
        timeStr1 = tstrs[1]
        returnStr = "g#%s%sfwhm%s%04.0f%%%s%%%s%%\r\n"%(group_id, unit_id, camname, fwhm*100, dateStr1, timeStr1)
        # returnStr = "send fwhm: %s, %.2f\r\n"%(timeStr, fwhm)
        log.debug(returnStr)
        socket_server.send(sock, bytes(returnStr, 'utf-8'))

socket_server.run(host='0.0.0.0', port=15332)