#!/usr/bin/env python3
import os
import time
from datetime import datetime
from kafka import KafkaProducer
import json

log_dir="/var/log"
open_files_log_file="lsof.log"
ppid = str(os.getppid())
data_dict = {}

#JSON serializer configuration part
def json_serializer(data):
	return json.dumps(data).encode("utf-8")

#bootstrap server's address and dedicated port
producer = KafkaProducer(bootstrap_servers = ["172.16.154.138:9092"],  value_serializer=json_serializer)

while True:

    ## ==== Add timestamp ====
    data_dict['timestamp'] = str(datetime.now())


    ## ==== PROCESS DATA ====
    # Contains all the currently running processes in a list of dictionaries for each process
    ps_list = []
    # list to store all pids
    pid_list = []
    # list all process command
    ps_cmd = '/usr/bin/ps uw -g' + ppid + ',1 --deselect'
    # Fetching all running process using ps
    ps_output = os.popen(ps_cmd).read().rstrip('\n')
    # All processes raw data in a list
    ps_output_list = ps_output.split('\n')
    # Count of number of processes
    ps_count = len(ps_output_list)
    # if there exists any new process (count>1, as the columns are the first element)
    if ps_count > 1:
        # USER PID %CPU %MEM VSZ RSS TTY STAT START TIME COMMAND
        ps_cols=ps_output_list[0].split()
        # Contains values of a single process in dict
        ps_dict={}
        # loop for all processes in a single timeframe
        for i in range(1,ps_count):
            # loop for all columns for a single process
            for j in ps_cols:
            # if the column is COMMAND(last column) get all the remaining elements for full cmd
                if j == "COMMAND":
                    full_cmd = ' '.join(ps_output_list[i].split()[ps_cols.index(j):])
                    ps_dict[j]=full_cmd
                # else get the corresponding column data for process dict
                elif j=="PID":
                    ps_dict[j]=ps_output_list[i].split()[ps_cols.index(j)]
                    pid_list.append(ps_dict[j])
                else:
                    ps_dict[j]=ps_output_list[i].split()[ps_cols.index(j)]
            # Add the process data dict in process list
            ps_list.append(ps_dict.copy())
    data_dict['process_data']=ps_list


    ## ==== OPEN FILES DATA ====
    lsof_list = []
    for pid in pid_list:
        lsof_cmd = "/usr/bin/lsof -p " + pid + "| /usr/bin/grep -vE '/usr/bin/bash$|path dev=|/dev/pts/|cwd|rtd|libc.so.6|libtinfo.so.6.3|ld-linux-x86-64.so.2|^psad'"
        lsof_output = os.popen(lsof_cmd).read().rstrip('\n')
        lsof_output_list = lsof_output.split('\n')
        lsof_count = len(lsof_output_list)
        if lsof_count > 1:
            lsof_cols=lsof_output_list[0].split()
            lsof_dict = {}
            for i in range(1,lsof_count):
                for j in lsof_cols:
                    if j == "NAME":
                        if len(lsof_output_list[i].split()) == len(lsof_cols):
                            full_name = ' '.join(lsof_output_list[i].split()[lsof_cols.index(j):])
                            lsof_dict[j] = full_name
                        else:                                                                                                     # special case kworker which doesn't have all fields
                            lsof_dict[j] = lsof_output_list[i].split()[-1]
                    else:
                        if len(lsof_output_list[i].split()) == len(lsof_cols):
                            lsof_dict[j] = lsof_output_list[i].split()[lsof_cols.index(j)]
                        else:                                                                                                     # special case kworker which doesn't have all fields
                            if lsof_cols.index(j) < (len(lsof_output_list[i].split())-1):
                                lsof_dict[j] = lsof_output_list[i].split()[lsof_cols.index(j)]
                            else:
                                lsof_dict[j] = ""
                lsof_list.append(lsof_dict.copy())
    data_dict['lsof_data']=lsof_list


        ## === PSAD data fetch ===
    content = [] #Dubugged
    title = ""  #Dubugged
    psad_dict = {}  
    psad_cmd = 'psad -S'
    psad_output = os.popen(psad_cmd).read().rstrip('\n')
    psad_output_lines = psad_output.split('\n')
    for line in psad_output_lines:
        if line.startswith('[+]'):
            title = line.split(':')[0].replace('[+] ','')
            if title == "psad_fw_read (pid":
                title = "psad_fw_read"
            elif title == "psad (pid":
                title = "psad"
            content = []
        if line != '':
            content += line.rstrip('\n').replace('[+] '+ title + ' ','').replace('[+] '+ title + ':','').lstrip(' ').split('\n')
        if '' in content:
            content.remove('')
        if title not in ['These results are available in','psad_fw_read']:
            psad_dict[title]=content
    data_dict['psad_data']=psad_dict


    ## === Final data dictionary ===
    print(data_dict)
    ##Sending data via Kafka Producer
    producer.send("EDR", data_dict)
    print("data sent")
    # Get processes and open files every 10 seconds
    time.sleep(10)

