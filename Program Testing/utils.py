import os
from multiprocessing import freeze_support

def IsWinOS():
    if os.name == "nt":
        freeze_support()
        print("Freeze Support Enabled Because of Windows Operating System.")
        return True