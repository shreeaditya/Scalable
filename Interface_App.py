from subprocess import Popen as start
import os
# cwd = os.getcwd()
cwd = 'D:\Django_project-2\projects\learndjango_telusko\dist'
print(cwd)
# processes = []
#
# script_list = ["Scalable Assignment.exe"]
#
# p1 = start(os.path.join(cwd, "Scalable Assignment.exe"))
# p1.wait()

# for i in script_list:
#     process = start(os.path.join(cwd, i))
#     processes.append(process)
#
# for process in processes:
#     process.wait()

os.startfile(os.path.join(cwd, "Scalable Assignment.exe"))

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ExampleHandler(FileSystemEventHandler):
    def on_created(self, event): # when file is created
        service_list = ["Scalable Assignment.exe", "Function_1-Pick Data & Compare.exe", "Service_2-Perform Data Analysis.exe", "Service_3-Insert Data to DB.exe"]
        for i in service_list:
            p1 = start(os.path.join(cwd, i ))
            p1.wait()
        # do something, eg. call your function to process the image
        print("Got event for file %s" % event.src_path)

observer = Observer()
event_handler = ExampleHandler() # create event handler
# set observer to use created handler in directory
observer.schedule(event_handler, path='D:\Django_project-2\projects\learndjango_telusko\dist\Data')
observer.start()

# sleep until keyboard interrupt, then stop + rejoin the observer
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()