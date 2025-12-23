"""
Quick script to check what customers and GL accounts exist in Sage 50.
Run this to see what you need to set up before importing orders.
"""

from pathlib import Path
import tempfile
from loguru import logger

def check_sage_setup():
    """Check customers and GL accounts in Sage."""
    
    try:
        import clr
        import sys
        
        # Add reference to Sage SDK
        sage_path = r"C:\Program Files (x86)\Sage\Peachtree\Interop.PeachwServer.dll"
        if Path(sage_path).exists():
            clr.AddReference(sage_path)
        else:
            print(f"ERROR: Sage SDK not found at {sage_path}")
            return
        
        from Interop.PeachwServer import Login, Application, Export, PeachwIEObj, PeachwIEFileType
        from Interop.PeachwServer import PeachwIEObjCustomerListField, PeachwIEObjChartOfAccountsField
        
        # Connect to already-open Sage (no login required)
        print("Connecting to already-open Sage 50 session...")
        login = Login()
        # Use "Peachtree Software" as username - connects to existing session
        obj = login.GetApplication("Peachtree Software", "")
        app = Application(obj)
        
        if app.get_CompanyIsOpen():
            company_name = app.get_CompanyName()
            print(f"Connected to: {company_name}")
        else:
            print("ERROR: No company is open in Sage. Please open a company first.")
            return
        
        # Export customers
        print("\n" + "="*60)
        print("CUSTOMERS IN SAGE:")
        print("="*60)
        
        try:
            exporter = Export(app.CreateExporter(PeachwIEObj.peachwIEObjCustomerList))
            exporter.ClearExportFieldList()
            exporter.AddToExportFieldList(int(PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerId))
            exporter.AddToExportFieldList(int(PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerName))
            
            temp_path = Path(tempfile.gettempdir()) / "sage_customers.xml"
            exporter.SetFilename(str(temp_path))
            exporter.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
            exporter.Export()
            
            if temp_path.exists():
                content = temp_path.read_text()
                # Parse and display customer IDs
                import re
                ids = re.findall(r'<Customer_ID[^>]*>([^<]+)</Customer_ID>', content)
                names = re.findall(r'<Customer_Name[^>]*>([^<]+)</Customer_Name>', content)
                
                if ids:
                    for i, (cid, name) in enumerate(zip(ids, names)):
                        print(f"  {i+1}. ID: {cid:20} Name: {name}")
                else:
                    print("  No customers found!")
                
                # Check for required customers
                required = ['Amazon', 'eBay', 'Shopify']
                ids_lower = [x.lower() for x in ids]
                print("\nRequired customers for import:")
                for req in required:
                    if req.lower() in ids_lower:
                        print(f"  [OK] {req}")
                    else:
                        print(f"  [MISSING] {req} - YOU NEED TO CREATE THIS!")
                
                temp_path.unlink()
        except Exception as e:
            print(f"  Error exporting customers: {e}")
        
        # Export GL Accounts
        print("\n" + "="*60)
        print("G/L ACCOUNTS IN SAGE:")
        print("="*60)
        
        try:
            exporter = Export(app.CreateExporter(PeachwIEObj.peachwIEObjChartOfAccounts))
            exporter.ClearExportFieldList()
            exporter.AddToExportFieldList(int(PeachwIEObjChartOfAccountsField.peachwIEObjChartOfAccountsField_AccountId))
            exporter.AddToExportFieldList(int(PeachwIEObjChartOfAccountsField.peachwIEObjChartOfAccountsField_AccountDescription))
            exporter.AddToExportFieldList(int(PeachwIEObjChartOfAccountsField.peachwIEObjChartOfAccountsField_AccountType))
            
            temp_path = Path(tempfile.gettempdir()) / "sage_accounts.xml"
            exporter.SetFilename(str(temp_path))
            exporter.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
            exporter.Export()
            
            if temp_path.exists():
                content = temp_path.read_text()
                # Parse and display account IDs
                import re
                ids = re.findall(r'<Account_ID[^>]*>([^<]+)</Account_ID>', content)
                descs = re.findall(r'<Account_Description[^>]*>([^<]*)</Account_Description>', content)
                types = re.findall(r'<Account_Type[^>]*>([^<]*)</Account_Type>', content)
                
                # Show accounts (limit to first 30)
                if ids:
                    # Group by type
                    ar_accounts = []
                    income_accounts = []
                    
                    for i, (aid, desc, atype) in enumerate(zip(ids, descs, types)):
                        if 'receivable' in atype.lower() or 'receivable' in desc.lower():
                            ar_accounts.append((aid, desc))
                        if 'income' in atype.lower() or 'revenue' in atype.lower() or 'sales' in desc.lower():
                            income_accounts.append((aid, desc))
                    
                    print("\nAccounts Receivable (AR) accounts:")
                    for aid, desc in ar_accounts[:10]:
                        print(f"  {aid:15} {desc}")
                    
                    print("\nIncome/Sales accounts:")
                    for aid, desc in income_accounts[:10]:
                        print(f"  {aid:15} {desc}")
                    
                    print("\nAll accounts (first 20):")
                    for i, (aid, desc, atype) in enumerate(zip(ids[:20], descs[:20], types[:20])):
                        print(f"  {aid:15} {desc:30} ({atype})")
                    
                    if len(ids) > 20:
                        print(f"  ... and {len(ids) - 20} more accounts")
                    
                    # Check for required accounts
                    print("\nLooking for accounts 1100 and 4100:")
                    if '1100' in ids:
                        print(f"  [OK] 1100 found")
                    else:
                        print(f"  [MISSING] 1100 - Check what AR account to use!")
                    
                    if '4100' in ids:
                        print(f"  [OK] 4100 found")
                    else:
                        print(f"  [MISSING] 4100 - Check what Sales account to use!")
                else:
                    print("  No accounts found!")
                
                temp_path.unlink()
        except Exception as e:
            print(f"  Error exporting accounts: {e}")
        
        print("\n" + "="*60)
        print("SETUP INSTRUCTIONS:")
        print("="*60)
        print("""
To fix the import errors, you need to:

1. CREATE CUSTOMERS in Sage (Maintain > Customers):
   - Customer ID: Amazon,  Name: Amazon
   - Customer ID: eBay,    Name: eBay
   - Customer ID: Shopify, Name: Shopify

2. CHECK G/L ACCOUNTS:
   - Note your Accounts Receivable account ID (e.g., 1100)
   - Note your Sales/Income account ID (e.g., 4100)
   
   If they are different from 1100/4100, set environment variables:
   SET SAGE_AR_ACCOUNT=your_ar_account
   SET SAGE_SALES_ACCOUNT=your_sales_account
""")
        
    except ImportError as e:
        print(f"ERROR: pythonnet not installed. Run: pip install pythonnet")
        print(f"Details: {e}")
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    check_sage_setup()

