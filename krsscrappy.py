from ast import parse
import os
import requests
import time
import json
import time
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
#from threading import Thread

#start 102, end 1013552
firstnum = 102
lastnum  = 1013552

# R - resolved, F - failed
def failureLog(krs, isResolved, fl):
    if(isResolved):
        fl.write("R" + str(krs) + "\n")
    else:
        fl.write("F" + str(krs) + "\n")

def timeNow(): #Get time now, for logs, y'know
    return(str(time.strftime("[%H:%M:%S] ", time.localtime())))


def plog(log, silent = True): #Function printing logs. Might be modified to print to a file
    if not silent:
        print(timeNow()+str(log))


def resolveKRS(nr): #Check if KRS nr is assigned to anything
    krs = str(nr).zfill(10)
    plog("Resolving "+str(krs), True)
    resolveFails = 0
    while True:
        try:
            response = requests.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/"+krs+"?rejestr=P&format=json")
        except:
            resolveFails += 1
            time.sleep(1)
            plog(str(krs) + " has failed while resolving. Fail count: " + str(resolveFails))
        else:
            break
        if resolveFails>5:
            plog("Failed to resolve " + str(krs), False)

    if(response.status_code==200):
        output = ""
        fails = 0
        while True:
            try:
                output = parseKRS(krs, response.json())
            except:
                fails += 1
                time.sleep(1)
                plog(str(krs) + " has failed while parsing. Fail count: " + str(fails) + ", response received:\n" + str(response.json()), True)
            else:
                break
            if fails>10:
                plog("Fails exceeded while parsing " + krs, False)
                break
        #light sanitisation
        plog("Successfully resolved " + krs + "!", True)
        return output.replace('\n', ' ').replace('\r', '')
    else:
        plog(krs+" | status code "+str(response.status_code), True)


def parseKRS(krs, data): #If KRS resolved, process its data
    pkd = ""
    for przew in range(0,len(data['odpis']['dane']['dzial3']['przedmiotDzialalnosci']['przedmiotPrzewazajacejDzialalnosci'])):
        pkd += str(data['odpis']['dane']['dzial3']['przedmiotDzialalnosci']['przedmiotPrzewazajacejDzialalnosci'][przew]['kodDzial'])+", "
    try:
        for pozost in range(0,len(data['odpis']['dane']['dzial3']['przedmiotDzialalnosci']['przedmiotPozostalejDzialalnosci'])):
            pkd += str(data['odpis']['dane']['dzial3']['przedmiotDzialalnosci']['przedmiotPozostalejDzialalnosci'][pozost]['kodDzial'])+", "
    except:
        plog(str(krs)+" THIS error again...")
    if len(pkd)>1:
        pkd = pkd[:-1]
    return(str(krs) + " | " + str(data['odpis']['dane']['dzial1']['danePodmiotu']['nazwa']) + " | " + pkd)

def saveKRS(x, file, failLogFile): #
    resolved = resolveKRS(x)
    if resolved:
        file.write(resolved + "\n")
        plog("[" + str(int((x/(lastnum-firstnum)*100))) + "%] Resolved " + str(x).zfill(10), False)
        failureLog(x, True, failLogFile)
    else:
        failureLog(x, False, failLogFile)

def fileSaver(file, failLogFile): #Save file roughly every second
    plog("Saved!", False)
    file.flush()
    failLogFile.flush()
    os.fsync(file.fileno())
    os.fsync(failLogFile.fileno())
    time.sleep(5)


def Harvester(): #Main function
    plog("Starting new session", False)
    extension = ".txt"
    filename = "nlp2"
    failLog = filename + "-fails.txt" + extension

    filename = filename+extension
    f = open(filename, "a", encoding="utf-8")
    fl = open(failLog, "a", encoding="utf-8")
    threads = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        with ThreadPoolExecutor(max_workers=1) as timeExecutor:
            try:
                for x in range(firstnum,lastnum+1):
                    threads.append(executor.submit(saveKRS, x, f, fl))
                    threads.append(timeExecutor.submit(fileSaver, f, fl))
            except:
                plog("Exception occured!", False)
                f.close()
    f.close()

Harvester()