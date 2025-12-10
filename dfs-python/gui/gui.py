import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import tkinter as tk
from tkinter import filedialog
from client.client import upload_file, get_locations, download_file


def upload_action():
    file = filedialog.askopenfilename()
    if file:
        upload_file(file)


def check_action():
    filename = filename_entry.get().strip()
    get_locations(filename)


def download_action():
    filename = filename_entry.get().strip()
    if not filename:
        print("Enter filename first")
        return
    save_path = filedialog.asksaveasfilename(initialfile=filename)
    if not save_path:
        return
    download_file(filename, save_path)


root = tk.Tk()
root.title("DFS Prototype")

upload_btn = tk.Button(root, text="Upload File", command=upload_action)
upload_btn.pack(pady=5)

filename_entry = tk.Entry(root)
filename_entry.pack(pady=5)

check_btn = tk.Button(root, text="Check File Locations", command=check_action)
check_btn.pack(pady=5)

download_btn = tk.Button(root, text="Download File", command=download_action)
download_btn.pack(pady=5)

root.mainloop()
