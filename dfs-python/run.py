import subprocess
import time
import os

# Windows: use start cmd to open new terminals
def run_in_new_terminal(cmd):
    subprocess.Popen(f'start cmd /k {cmd}', shell=True)

# start namenode
run_in_new_terminal("python namenode/server.py")
time.sleep(1)

# start datanodes
run_in_new_terminal("python datanode/server.py 6000")
run_in_new_terminal("python datanode/server.py 6001")
time.sleep(1)

# run GUI in current terminal
os.system("python gui/gui.py")
