import os
import datetime
import requests
import json
import logging
import ftplib
from threading import Thread
from threading import RLock
import openpyxl as XL
import xml.etree.ElementTree as ET

logger = logging.getLogger()
logger.setLevel(logging.INFO)

url = 'https://api.ebay.com/ws/api.dll'

userid = os.environ['userid']
key = os.environ['key']
ftp_ip = os.environ['ftp_ip']
ftp_user = os.environ['ftp_user']
ftp_pass = os.environ['ftp_pass']

getAllItemIdsParams = {
	'Content-Type' : 'text/xml',
	'X-EBAY-API-COMPATIBILITY-LEVEL' : '1081',
	'X-EBAY-API-CALL-NAME' : 'GetSellerEvents',
	'X-EBAY-API-SITEID' : '0'
	}
	
getItemParams = {
	'Content-Type' : 'text/xml',
	'X-EBAY-API-COMPATIBILITY-LEVEL' : '1081',
	'X-EBAY-API-CALL-NAME' : 'GetItem',
	'X-EBAY-API-SITEID' : '0'
	}

today = datetime.datetime.now() - datetime.timedelta(hours=7)
yesterday = (today - datetime.timedelta(days=1)).replace(hour=9, minute=0)

shortnames = {
	'suspensionspecialists' : 'TSS',
	'wulfsuspension' : 'WLF'
	}

report_headers = [
	'itemid',
	'Listing URL',
	'current-*Category',
	'previous-*Category',
	'current-StoreCategory',
	'previous-StoreCategory',
	'current-*Title',
	'previous-*Title',
	'current-*ConditionID',
	'previous-*ConditionID',
	'current-*C:Brand',
	'previous-*C:Brand',
	'current-C:Part Type',
	'previous-C:Part Type',
	'current-*C:Manufacturer Part Number',
	'previous-*C:Manufacturer Part Number',
	'current-C:Interchange Part Number',
	'previous-C:Interchange Part Number',
	'current-C:Other Part Number',
	'previous-C:Other Part Number',
	'current-C:Placement on Vehicle',
	'previous-C:Placement on Vehicle',
	'current-C:Warranty',
	'previous-C:Warranty',
	'current-C:Custom Bundle',
	'previous-C:Custom Bundle',
	'current-C:Fitment Type',
	'previous-C:Fitment Type',
	'current-C:Included Hardware',
	'previous-C:Included Hardware',
	'current-C:Greasable or Sealed',
	'previous-C:Greasable or Sealed',
	'current-C:Modified Item',
	'previous-C:Modified Item',
	'current-C:Adjustable',
	'previous-C:Adjustable',
	'current-C:Non-Domestic Product',
	'previous-C:Non-Domestic Product',
	'current-C:Country/Region of Manufacture',
	'previous-C:Country/Region of Manufacture',
	'current-PicURL',
	'previous-PicURL',
	'current-GalleryType',
	'previous-GalleryType',
	'current-*Description',
	'previous-*Description',
	'current-*Format',
	'previous-*Format',
	'current-*Duration',
	'previous-*Duration',
	'current-*StartPrice',
	'previous-*StartPrice',
	'current-BuyItNowPrice',
	'previous-BuyItNowPrice',
	'previous-*Quantity',
	'current-*Quantity',
	'current-ShippingType',
	'previous-ShippingType',
	'current-ShippingService-1:Option',
	'previous-ShippingService-1:Option',
	'current-ShippingService-1:Cost',
	'previous-ShippingService-1:Cost',
	'current-*DispatchTimeMax',
	'previous-*DispatchTimeMax',
	'current-*ReturnsAcceptedOption',
	'previous-*ReturnsAcceptedOption',
	'current-ReturnsWithinOption',
	'previous-ReturnsWithinOption']
	
def P(str):
	return f"{{urn:ebay:apis:eBLBaseComponents}}{str}"
	
def getValueString(name, item):
	itemspecifics = item.find(P('ItemSpecifics'))
	returnString = ''
	
	for each in itemspecifics:
		if (each.find(P('Name')).text == name):
			allValues = each.findall(P('Value'))
			numValues = len(allValues)
			
			if (numValues > 1):
				i = 0
				while (i < numValues):
					if (i != (numValues - 1)):
						returnString = allValues[i].text + '|' + returnString
					else:
						returnString = returnString + allValues[i].text
					i = i + 1
			else:
				returnString = allValues[0].text
				break
	
	return returnString

def getAllItemIdsXML():
	return f"""
<?xml version="1.0" encoding="utf-8"?>
<GetSellerEventsRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials>  
  <eBayAuthToken>{key}</eBayAuthToken>
  </RequesterCredentials>
  <ModTimeFrom>{yesterday}</ModTimeFrom>
  <ModTimeTo>{today}</ModTimeTo>
  <OutputSelector>ItemID</OutputSelector>
</GetSellerEventsRequest>
"""

def getItemXML(itemid):
	return f"""
<?xml version="1.0" encoding="utf-8"?>
<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials>
    <eBayAuthToken>{key}</eBayAuthToken>
  </RequesterCredentials>
  <ItemID>{itemid}</ItemID>
  <IncludeItemSpecifics>True</IncludeItemSpecifics>
  <DetailLevel>ReturnAll</DetailLevel>
</GetItemRequest>
"""

def getAllItemIds():
	itemids = []
	
	logger.info(f"[{userid}] Starting getAllItemIds...")
	r = requests.post(url, data=getAllItemIdsXML(), headers=getAllItemIdsParams)
		
	if (r.status_code != 200):
		logger.error(f"[{userid}] Response: {r.text}")
		
	root = ET.fromstring(r.content)
	itemArr = root.find(P('ItemArray'))

	if not itemArr:
		return itemids

	for eachItem in itemArr:
		itemid = eachItem.find(P('ItemID')).text
		itemids.append(itemid)
	
	return itemids

def getItem(itemid):
	logger.info(f"[{userid}] Calling GetItem for {itemid}")
	r = requests.post(url, data=getItemXML(itemid), headers=getItemParams)
	
	if (r.status_code != 200):
		logger.error(f"[{userid}] Response: {r.text}")
	
	root = ET.fromstring(r.content)
	item = root.find(P('Item'))

	CategoryID = ''
	try:
		CategoryID = item.find(P('PrimaryCategory')).find(P('CategoryID')).text
	except:
		pass
	
	StoreCategoryID = ''
	try:
		StoreCategoryID = item.find(P('Storefront')).find(P('StoreCategoryID')).text
	except:
		pass
	
	Title = ''
	try:
		Title = item.find(P('Title')).text
	except:
		pass

	ConditionID = ''
	try:
		ConditionID = item.find(P('ConditionID')).text
	except:
		pass

	Brand = getValueString('Brand', item)	
	PartType = getValueString('Part Type', item)
	ManufacturerPartNumber = getValueString('Manufacturer Part Number', item)
	InterchangePartNumber = getValueString('Interchange Part Number', item)
	OtherPartNumber = getValueString('Other Part Number', item)
	PlacementOnVehicle = getValueString('Placement on Vehicle', item)
	Warranty = getValueString('Warranty', item)
	CustomBundle = getValueString('Custom Bundle', item)
	FitmentType = getValueString('Fitment Type', item)
	IncludedHardware = getValueString('Included Hardware', item)
	Greasable = getValueString('Greasable', item)
	ModifiedItem = getValueString('Modified Item', item)
	Adjustable = getValueString('Adjustable', item)
	NonDomesticProduct = getValueString('Non-Domestic Product', item)
	CountryRegionOfManufacture = getValueString('Country/Region of Manufacture', item)
	
	PicURL = ''
	try:
		PicURL = item.find(P('PictureDetails')).find(P('GalleryURL')).text
	except:
		pass

	GalleryType = ''
	try:
		GalleryType = item.find(P('PictureDetails')).find(P('GalleryType')).text
	except:
		pass
		
	Description = ''
	try:
		Description = item.find(P('Description')).text
	except:
		pass

	Format = ''

	Duration = ''
	try:
		Duration = item.find(P('ListingDuration')).text
	except:
		pass
		
	StartPrice = ''
	try:
		StartPrice = item.find(P('StartPrice')).text
	except:
		pass
		
	BuyItNowPrice = ''
	try:
		BuyItNowPrice = item.find(P('BuyItNowPrice')).text
	except:
		pass
		
	Quantity = ''
	try:
		Quantity = item.find(P('Quantity')).text
	except:
		pass
	
	ShippingType = ''
	try:
		ShippingType = item.find(P('ShippingDetails')).find(P('ShippingType')).text
	except:
		pass
	
	ShippingService1Option = ''
	try:
		ShippingService1Option = item.find(P('ShippingDetails')).find(P('ShippingServiceOptions')).find(P('ShippingService')).text
	except:
		pass
		
	ShippingService1Cost = ''	
	try:
		ShippingService1Cost = item.find(P('ShippingDetails')).find(P('ShippingServiceOptions')).find(P('ShippingServiceCost')).text
	except:
		pass

	DispatchTimeMax = ''
	try:
		DispatchTimeMax = item.find(P('DispatchTimeMax')).text
	except:
		pass

	ReturnsAcceptedOption = ''
	try:
		ReturnsAcceptedOption = item.find(P('ReturnPolicy')).find(P('ReturnsAcceptedOption')).text
	except:
		pass
		
	ReturnsWithinOption = ''
	try:
		ReturnsWithinOption = item.find(P('ReturnPolicy')).find(P('ReturnsWithinOption')).text
	except:
		pass

	return {
		'itemid' : itemid,
		'Listing URL' : f'https://www.ebay.com/itm/{itemid}',
		'*Category' : CategoryID,
		'StoreCategory' : StoreCategoryID,
		'*Title' : Title,
		'*ConditionID' : ConditionID,
		'*C:Brand' : Brand,
		'C:Part Type' : PartType,
		'*C:Manufacturer Part Number' : ManufacturerPartNumber,
		'C:Interchange Part Number' : InterchangePartNumber,
		'C:Other Part Number' : OtherPartNumber,
		'C:Placement on Vehicle' : PlacementOnVehicle,
		'C:Warranty' : Warranty,
		'C:Custom Bundle' : CustomBundle,
		'C:Fitment Type' : FitmentType,
		'C:Included Hardware' : IncludedHardware,
		'C:Greasable or Sealed' : Greasable,
		'C:Modified Item' : ModifiedItem,
		'C:Adjustable' : Adjustable,
		'C:Non-Domestic Product' : NonDomesticProduct,
		'C:Country/Region of Manufacture' : CountryRegionOfManufacture,
		'PicURL' : PicURL,
		'GalleryType' : GalleryType,
		'*Description' : Description,
		'*Format' : Format,
		'*Duration' : Duration,
		'*StartPrice' : StartPrice,
		'BuyItNowPrice' : BuyItNowPrice,
		'*Quantity' : Quantity,
		'ShippingType' : ShippingType,
		'ShippingService-1:Option' : ShippingService1Option,
		'ShippingService-1:Cost' : ShippingService1Cost,
		'*DispatchTimeMax' : DispatchTimeMax,
		'*ReturnsAcceptedOption' : ReturnsAcceptedOption,
		'ReturnsWithinOption' : ReturnsWithinOption}
		
def getOldItems():
	try:
		ftp = ftplib.FTP()
		ftp.connect(ftp_ip, 21, timeout=120)
		ftp.set_debuglevel(1)
		ftp.set_pasv(True)
		ftp.login(ftp_user, ftp_pass)
		
		f = open('data.xlsx', 'wb')
		
		ftp.retrbinary(f'RETR {userid}-data.xlsx', f.write)
		
		f.close()
		ftp.quit()
		
		lastData_wb = XL.load_workbook(filename = f"data.xlsx", read_only=True)
		lastData_ws = lastData_wb['Sheet']
		
		items = {}
		
		for row in lastData_ws.rows:
			items[row[0].value] = {
				'*Category' : row[2].value,
				'StoreCategory' : row[3].value,
				'*Title' : row[4].value,
				'*ConditionID' : row[5].value,
				'*C:Brand' : row[6].value,
				'C:Part Type' : row[7].value,
				'*C:Manufacturer Part Number' : row[8].value,
				'C:Interchange Part Number' : row[9].value,
				'C:Other Part Number' : row[10].value,
				'C:Placement on Vehicle' : row[11].value,
				'C:Warranty' : row[12].value,
				'C:Custom Bundle' : row[13].value,
				'C:Fitment Type' : row[14].value,
				'C:Included Hardware' : row[15].value,
				'C:Greasable or Sealed' : row[16].value,
				'C:Modified Item' : row[17].value,
				'C:Adjustable' : row[18].value,
				'C:Non-Domestic Product' : row[19].value,
				'C:Country/Region of Manufacture' : row[20].value,
				'PicURL' : row[21].value,
				'GalleryType' : row[22].value,
				'*Description' : row[23].value,
				'*Format' : row[24].value,
				'*Duration' : row[25].value,
				'*StartPrice' : row[26].value,
				'BuyItNowPrice' : row[27].value,
				'*Quantity' : row[28].value,
				'ShippingType' : row[29].value,
				'ShippingService-1:Option' : row[30].value,
				'ShippingService-1:Cost' : row[31].value,
				'*DispatchTimeMax' : row[32].value,
				'*ReturnsAcceptedOption' : row[33].value,
				'ReturnsWithinOption' : row[34].value}
			
		return items
	except Exception as err:
		return None
		
def log(str):
	print(f"[{userid}] {str}", flush=True)

def main(event, context):
	# Get all modified itemids
	log(f"Getting itemids of items modified between {yesterday} and {today}")
	itemids = getAllItemIds()
	
	log(f"Found {len(itemids)}...")
	
	if (len(itemids) == 0):
		log("No modified items found, no report to generate. Exiting.")
		return
	
	newItems = []
	
	log("Retrieving full details for each modified item...")
	
	# Get all fields for each item and add them to the list
	for eachId in itemids:
		newItems.append(getItem(eachId))
		
	log("Retrieving data file from FTP...")
		
	# Read in the data file from the FTP
	oldItems = getOldItems()
	
	wb_report = XL.Workbook()
	ws_report = wb_report.active
	
	ws_report.append(report_headers)
	
	numSkipped = 0
	log("Generating report...")
	# Add the fields from both versions of the item to the report
	for newItem in newItems:
		itemid = newItem['itemid']
		url = f'https://www.ebay.com/itm/{itemid}'
		
		oldItem = {}

		# if itemid not found in data, skip to the next one
		try:
			oldItem = oldItems[itemid]
			log(f"{itemid} found in data, adding to report...")
		except:
			log(f'{itemid} not found in data, skipping...')
			numSkipped = numSkipped + 1
			continue
			
		ws_report.append(
		[
			itemid,
			url,
			newItem['*Category'],
			oldItem['*Category'],
			newItem['StoreCategory'],
			oldItem['StoreCategory'],
			newItem['*Title'],
			oldItem['*Title'],
			newItem['*ConditionID'],
			oldItem['*ConditionID'],
			newItem['*C:Brand'],
			oldItem['*C:Brand'],
			newItem['C:Part Type'],
			oldItem['C:Part Type'],
			newItem['*C:Manufacturer Part Number'],
			oldItem['*C:Manufacturer Part Number'],
			newItem['C:Interchange Part Number'],
			oldItem['C:Interchange Part Number'],
			newItem['C:Other Part Number'],
			oldItem['C:Other Part Number'],
			newItem['C:Placement on Vehicle'],
			oldItem['C:Placement on Vehicle'],
			newItem['C:Warranty'],
			oldItem['C:Warranty'],
			newItem['C:Custom Bundle'],
			oldItem['C:Custom Bundle'],
			newItem['C:Fitment Type'],
			oldItem['C:Fitment Type'],
			newItem['C:Included Hardware'],
			oldItem['C:Included Hardware'],
			newItem['C:Greasable or Sealed'],
			oldItem['C:Greasable or Sealed'],
			newItem['C:Modified Item'],
			oldItem['C:Modified Item'],
			newItem['C:Adjustable'],
			oldItem['C:Adjustable'],
			newItem['C:Non-Domestic Product'],
			oldItem['C:Non-Domestic Product'],
			newItem['C:Country/Region of Manufacture'],
			oldItem['C:Country/Region of Manufacture'],
			newItem['PicURL'],
			oldItem['PicURL'],
			newItem['GalleryType'],
			oldItem['GalleryType'],
			newItem['*Description'],
			oldItem['*Description'],
			newItem['*Format'],
			oldItem['*Format'],
			newItem['*Duration'],
			oldItem['*Duration'],
			newItem['*StartPrice'],
			oldItem['*StartPrice'],
			newItem['BuyItNowPrice'],
			oldItem['BuyItNowPrice'],
			newItem['*Quantity'],
			oldItem['*Quantity'],
			newItem['ShippingType'],
			oldItem['ShippingType'],
			newItem['ShippingService-1:Option'],
			oldItem['ShippingService-1:Option'],
			newItem['ShippingService-1:Cost'],
			oldItem['ShippingService-1:Cost'],
			newItem['*DispatchTimeMax'],
			oldItem['*DispatchTimeMax'],
			newItem['*ReturnsAcceptedOption'],
			oldItem['*ReturnsAcceptedOption'],
			newItem['ReturnsWithinOption'],
			oldItem['ReturnsWithinOption']
		])
	
	# If there were no modified items or we skipped all of them because they weren't in the data, don't put out a report
	if (len(newItems) == 0 or numSkipped == len(newItems)):
		log(f"Number of modified items: {len(newItems)}. Number of items not found in the data: {numSkipped}. No report to generate.")
		return
	
	log("Writing report to local file...")
	# Put the report on the FTP
	wb_report.save('/tmp/out.xlsx')
	
	log("Putting report on FTP...")
	ftp = ftplib.FTP()
	ftp.connect(ftp_ip, 21, timeout=120)
	ftp.set_debuglevel(1)
	ftp.set_pasv(True)
	ftp.login(ftp_user, ftp_pass)
	
	f = open('out.xlsx', 'rb')
	
	ftp.storbinary(f"STOR {shortnames[userid]}_{today.strftime('%Y-%m-%d')}.xlsx", f)
	
	f.close()
	ftp.quit()
	
	log("Done.")