#!/usr/bin/env python3
from kafka import KafkaConsumer
from pywebio.input import input, FLOAT
from pywebio.output import put_text, put_markdown, put_table, put_button
import json
import os
import time


#+=========================================process_data_fun_STARTS==========================================+
def process_data_fun():
    put_text("+====================================+process_data+====================================+")
    tempList = []
    tempList = [['USER', 'PID', '%CPU', '%MEM', 'VSZ', 'RSS', 'TTY', 'STAT', 'START', 'TIME', 'COMMAND']]
    for i in a['process_data']:
        tempList1 = []
        tempList1.append(i['USER'])
        tempList1.append(i['PID'])
        tempList1.append(i['%CPU'])
        tempList1.append(i['%MEM'])
        tempList1.append(i['VSZ'])
        tempList1.append(i['RSS'])
        tempList1.append(i['TTY'])
        tempList1.append(i['STAT'])
        tempList1.append(i['START'])
        tempList1.append(i['TIME'])
        tempList1.append(i['COMMAND'])
        tempList.append(tempList1)
    put_table(tempList)
    put_text("+====================================+process_data+====================================+")
    put_button('Quit', onclick=killApplication)
#+=========================================process_data_fun_ENDS==========================================+

#+=========================================lsof_data_fun_STARTS==========================================+
def lsof_data_fun():
    put_text("+====================================+lsof_data+====================================+")
    tempList = []
    tempList = [['COMMAND', 'PID', 'USER', 'FD', 'TYPE', 'DEVICE', 'SIZE/OFF', 'NODE', 'NAME']]
    for i in a['lsof_data']:
        tempList1 = []
        tempList1.append(i["COMMAND"])
        tempList1.append(i["PID"])
        tempList1.append(i["USER"])
        tempList1.append(i["FD"])
        tempList1.append(i["TYPE"])
        tempList1.append(i["DEVICE"])
        tempList1.append(i["SIZE/OFF"])
        tempList1.append(i["NODE"])
        tempList1.append(i["NAME"])
        tempList.append(tempList1) 
    put_table(tempList)
    put_text("+====================================+lsof_data+====================================+")
    put_button('Quit', onclick=killApplication)
#+=========================================lsof_data_fun_ENDS==========================================+
#+=========================================ps_data_fun_STARTS==========================================+
def show_IP_Status_Detail():
    tempStr=""
    for i in a['psad_data']["IP Status Detail"]:
        tempStr+=i
        tempStr+="\n"
    put_text(tempStr)
    put_button('Quit', onclick=killApplication)

def show_iptables_log_prefix_counters():
    tempStr=""
    for i in a['psad_data']["iptables log prefix counters"]:
        tempStr+=i
        tempStr+="\n"
    put_text(tempStr)
    put_button('Quit', onclick=killApplication)


def show_Top_25_attackers():
    tempStr=""
    for i in a['psad_data']["Top 25 attackers"]:
        tempStr+=i
        tempStr+="\n"
    put_text(tempStr)
    put_button('Quit', onclick=killApplication)

def show_Top_20_scanned_ports():
    
    tempStr = ""
    for i in a['psad_data']["Top 20 scanned ports"]:
        tempStr+=i
        tempStr+="\n"
    put_text(tempStr)
    put_button('Quit', onclick=killApplication)

def show_Top_50_signature_matches():
    tempStr = ""
    for i in a['psad_data']['Top 50 signature matches']:
        tempStr+=i
        tempStr+="\n"
    put_text(tempStr)
    put_button('Quit', onclick=killApplication)
    
def show_psad_data_fun():
    put_text("+++============================+++++show_psad_data+++++============================+++")
    tempStr = ""
    for i in a['psad_data']['psad']:
        tempStr+=i
        tempStr+="\n"
    for i in a['psad_data']["Version"]:
        tempStr+=i
    put_text(tempStr)
    put_text("+++============================+++++show_psad_data+++++============================+++")
    put_button('Quit', onclick=killApplication)

def psad_data_fun():
    put_text("+====================================+psad_data+====================================+")
    put_button('Show psad data', onclick=show_psad_data_fun)
    put_button('Show Top 50 signature matches', onclick=show_Top_50_signature_matches)
    put_button("Show Top 25 attackers", onclick=show_Top_25_attackers)
    put_button("Show Top 20 scanned ports", onclick=show_Top_20_scanned_ports)
    put_button("Show iptables log prefix counters", onclick=show_iptables_log_prefix_counters)
    put_button("Show IP Status Detail", onclick=show_IP_Status_Detail)
    put_text("+====================================+psad_data+====================================+")
    put_button('Quit', onclick=killApplication)
#+=========================================ps_data_fun_ENDS==========================================+
def killApplication():
    print("Application Exited Thank you!")
    put_markdown("<h1>Application Exited Thank you!</h1>")
    time.sleep(2)
    os._exit(os.EX_OK)

if __name__ == "__main__":
    address = "172.16.154.138:9092"    #Address of the Topic (Kafka Server)
    consumer = KafkaConsumer(
        "EDR",                  #Topic
        bootstrap_servers = address, 
        auto_offset_reset="latest",   #Printing method 
        group_id= "consumer-grpA"     #Group
    )
    print("Starting Consumer...\nA webpage will be opened autmomatically...")
    
    for msg in consumer:
        a = json.loads(msg.value)   #Getting data from the messages received
        print("Receiving messages from",address)
        break
    
    put_markdown('<h1><b>EDR</b></h1>')
    put_text("The data of "+a["timestamp"])
    put_button('psad_data', onclick=psad_data_fun)
    put_button('lsof_data', onclick=lsof_data_fun)
    put_button('process_data', onclick=process_data_fun)
    put_button('Quit', onclick=killApplication)
