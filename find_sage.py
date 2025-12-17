"""
Quick script to find Sage 50 installation on this PC.
Run: python find_sage.py
"""

import os
from pathlib import Path

print("🔍 Searching for Sage 50 data...\n")

# Common locations to check
locations = [
    r"C:\ProgramData\Sage\Accounts",
    r"C:\Program Files\Sage\Accounts",
    r"C:\Program Files (x86)\Sage\Accounts",
    os.path.expanduser(r"~\Documents\Sage"),
    os.path.expanduser(r"~\Sage"),
]

found = []

for base in locations:
    if os.path.exists(base):
        print(f"📁 Found: {base}")
        
        # Look for company folders
        for item in os.listdir(base):
            item_path = os.path.join(base, item)
            
            if os.path.isdir(item_path):
                # Check for ACCDATA (indicates company data)
                accdata = os.path.join(item_path, "ACCDATA")
                if os.path.exists(accdata):
                    print(f"   ✅ Company found: {item_path}")
                    found.append(item_path)
                else:
                    # Check subdirectories (year folders)
                    for sub in os.listdir(item_path):
                        sub_path = os.path.join(item_path, sub)
                        if os.path.isdir(sub_path):
                            accdata = os.path.join(sub_path, "ACCDATA")
                            if os.path.exists(accdata):
                                print(f"   ✅ Company found: {sub_path}")
                                found.append(sub_path)

print("\n" + "="*60)

if found:
    print("\n✅ SAGE 50 DATA FOUND!\n")
    print("Add this to your config.env:\n")
    for path in found:
        print(f"SAGE_COMPANY_PATH={path}")
    print("\n(Use the one that matches your active company)")
else:
    print("\n❌ No Sage 50 data found in common locations.")
    print("\nTry:")
    print("1. Open Sage 50 → Help → About → Look for Data Path")
    print("2. Search Windows for 'ACCDATA' folder")

