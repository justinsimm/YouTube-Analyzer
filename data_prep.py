import os
import shutil
import pathlib

count = 0
prep = os.path.join(f"{os.getcwd()}/prep")

#Rename files so there are no duplicate names and no logs
for folder in os.listdir(prep):
    folder = os.path.join(prep, folder)
    for file in os.listdir(folder):
        file = os.path.join(folder, file)

        if file == "log.txt":
            os.remove(file)
        else:
            name, ext = os.path.splitext(file)
            os.rename(f"{name}{ext}", f"{name} ({count}){ext}")

    count += 1

#Merge all files for use in input
for folder in os.listdir(prep):
    folder = os.path.join(prep, folder)
    for file in os.listdir(folder):
        src = os.path.join(folder, file)
        dst = os.path.join(f"{os.getcwd()}/input", file)
        shutil.move(src, dst)