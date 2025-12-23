"""
Import inventory items from InventoryExport.xlsx into Sage 50.
"""

import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
import tempfile
from datetime import datetime
from loguru import logger

from agent.config import init_config
from agent.sage50.sdk_operations import SageSDK, SageSDKError

# Batch size - Sage SDK may have limits on import size
BATCH_SIZE = 100


def create_inventory_xml(items_df: pd.DataFrame, batch_num: int = 0) -> str:
    """
    Create XML file for inventory import.
    
    Sage 50 Inventory Item fields:
    - Item_ID (required)
    - Description
    - Item_Class (Stock item, Non-stock item, Service, etc.)
    - Sales_Price (Package Price)
    - Last_Unit_Cost
    - GL_Sales_Account (required - defaults to 4100)
    - GL_Inventory_Account (for stock items - defaults to 1200)
    - GL_Cost_Of_Sales_Account (for stock items - defaults to 5000)
    """
    
    # Create XML structure
    root = ET.Element("PAW_Inventory_Items")
    root.set("xmlns:paw", "urn:schemas-peachtree-com/paw8.02-datatypes")
    root.set("xmlns:xsi", "http://www.w3.org/2000/10/XMLSchema-instance")
    
    for idx, row in items_df.iterrows():
        item = ET.SubElement(root, "PAW_Inventory_Item")
        
        # Item ID (required) - max 20 chars
        item_id = str(row.get('Item ID', '')).strip()[:20]
        if not item_id:
            continue
            
        id_elem = ET.SubElement(item, "Item_ID")
        id_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        id_elem.text = item_id
        
        # Description - max 160 chars
        description = str(row.get('Description', item_id)).strip()[:160]
        ET.SubElement(item, "Description").text = description
        
        # Item Class
        item_class = str(row.get('Item Class', 'Stock item')).strip()
        # Map to Sage item types
        if 'non-stock' in item_class.lower() or 'non stock' in item_class.lower():
            ET.SubElement(item, "Item_Class").text = "Non-stock item"
        elif 'service' in item_class.lower():
            ET.SubElement(item, "Item_Class").text = "Service"
        elif 'assembly' in item_class.lower():
            ET.SubElement(item, "Item_Class").text = "Assembly"
        else:
            ET.SubElement(item, "Item_Class").text = "Stock item"
        
        # Sales Price (Package Price)
        try:
            sales_price = float(row.get('Package Price', 0) or 0)
        except:
            sales_price = 0.0
        ET.SubElement(item, "Sales_Price_1").text = f"{sales_price:.2f}"
        
        # Last Unit Cost
        try:
            unit_cost = float(row.get('Last Unit Cost', 0) or 0)
        except:
            unit_cost = 0.0
        ET.SubElement(item, "Last_Unit_Cost").text = f"{unit_cost:.2f}"
        
        # GL Accounts - required for import
        # Sales Account
        gl_sales = ET.SubElement(item, "GL_Sales_Account")
        gl_sales.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        gl_sales.text = "4100"  # Income account
        
        # Inventory Account (for stock items)
        gl_inv = ET.SubElement(item, "GL_Inventory_Account")
        gl_inv.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        gl_inv.text = "1200"  # Inventory asset account
        
        # Cost of Sales Account (for stock items)
        gl_cos = ET.SubElement(item, "GL_Cost_Of_Sales_Account")
        gl_cos.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        gl_cos.text = "5000"  # Cost of goods sold
        
        # Item Type (for Sage categorization)
        ET.SubElement(item, "Item_Type").text = "Inventory"
        
        # Inactive flag
        ET.SubElement(item, "Inactive").text = "False"
    
    # Write to temp file
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    xml_path = Path(tempfile.gettempdir()) / f"inventory_import_{batch_num}_{timestamp}.xml"
    
    tree = ET.ElementTree(root)
    tree.write(str(xml_path), encoding="utf-8", xml_declaration=True)
    
    return str(xml_path)


def import_inventory_batch(xml_path: str, app) -> dict:
    """Import a batch of inventory items using Sage SDK."""
    
    from Interop.PeachwServer import (
        PeachwIEObj, 
        PeachwIEObjInventoryItemField,
        PeachwIEFileType,
        Import
    )
    
    try:
        # Create importer for Inventory Items
        importer_obj = app.CreateImporter(PeachwIEObj.peachwIEObjInventoryItem)
        importer = Import(importer_obj)
        
        if not importer:
            return {"success": False, "error": "Failed to create importer"}
        
        # Clear and set up field list
        importer.ClearImportFieldList()
        
        # Add fields to import
        fields = [
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_ItemId,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_Description,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_ItemClass,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_SalesPrice1,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_LastUnitCost,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_GLSalesAccountId,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_GLInventoryAccountId,
            PeachwIEObjInventoryItemField.peachwIEObjInventoryItemField_GLCostOfSalesAccountId,
        ]
        
        for field in fields:
            importer.AddToImportFieldList(int(field))
        
        # Set file info
        importer.SetFilename(xml_path)
        importer.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
        
        # Perform import
        importer.Import()
        
        return {"success": True}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def main():
    """Main function to import inventory."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Import inventory items to Sage 50")
    parser.add_argument("--file", "-f", type=str, default="InventoryExport.xlsx",
                        help="Excel file to import (default: InventoryExport.xlsx)")
    parser.add_argument("--batch-size", "-b", type=int, default=BATCH_SIZE,
                        help=f"Items per batch (default: {BATCH_SIZE})")
    parser.add_argument("--limit", "-l", type=int, default=None,
                        help="Limit total items to import (for testing)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Generate XML only, don't import")
    
    args = parser.parse_args()
    
    # Read Excel file
    excel_path = Path(args.file)
    if not excel_path.exists():
        print(f"ERROR: File not found: {excel_path}")
        return
    
    print(f"Reading {excel_path}...")
    df = pd.read_excel(excel_path)
    
    total_items = len(df)
    if args.limit:
        df = df.head(args.limit)
        total_items = len(df)
    
    print(f"Total items to import: {total_items}")
    print(f"Batch size: {args.batch_size}")
    print(f"Number of batches: {(total_items + args.batch_size - 1) // args.batch_size}")
    
    if args.dry_run:
        print("\n[DRY RUN] Generating XML only...")
        xml_path = create_inventory_xml(df, 0)
        print(f"Created: {xml_path}")
        
        # Show sample
        with open(xml_path, 'r') as f:
            content = f.read()
        print(f"\nFirst 2000 chars of XML:\n{content[:2000]}")
        return
    
    # Connect using the already-working SageSDK
    try:
        print("\nConnecting to Sage 50...")
        config = init_config()
        sdk = SageSDK(config)
        sdk.connect()
        
        app = sdk._company  # Get the Application object for direct SDK calls
        
        print(f"Connected to Sage 50!")
        print("(Using existing session - Sage will stay open)")
        
    except SageSDKError as e:
        print(f"ERROR: {e}")
        return
    except Exception as e:
        print(f"ERROR connecting to Sage: {e}")
        return
    
    # Import in batches
    print("\n" + "="*60)
    print("IMPORTING INVENTORY")
    print("="*60)
    
    total_success = 0
    total_failed = 0
    errors = []
    
    num_batches = (total_items + args.batch_size - 1) // args.batch_size
    
    try:
        for batch_num in range(num_batches):
            start_idx = batch_num * args.batch_size
            end_idx = min(start_idx + args.batch_size, total_items)
            batch_df = df.iloc[start_idx:end_idx]
            
            print(f"\nBatch {batch_num + 1}/{num_batches}: Items {start_idx + 1} to {end_idx}...")
            
            # Create XML for this batch
            xml_path = create_inventory_xml(batch_df, batch_num)
            
            # Import
            result = import_inventory_batch(xml_path, app)
            
            if result["success"]:
                total_success += len(batch_df)
                print(f"  [OK] Imported {len(batch_df)} items")
            else:
                total_failed += len(batch_df)
                error_msg = result.get("error", "Unknown error")
                errors.append(f"Batch {batch_num + 1}: {error_msg}")
                print(f"  [FAILED] {error_msg}")
            
            # Clean up temp file
            try:
                Path(xml_path).unlink()
            except:
                pass
    finally:
        # Disconnect (won't close Sage if it was already open)
        sdk.disconnect()
    
    # Summary
    print("\n" + "="*60)
    print("IMPORT COMPLETE")
    print("="*60)
    print(f"Total items: {total_items}")
    print(f"Successful:  {total_success}")
    print(f"Failed:      {total_failed}")
    
    if errors:
        print("\nErrors:")
        for err in errors[:10]:
            print(f"  - {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")


if __name__ == "__main__":
    main()

