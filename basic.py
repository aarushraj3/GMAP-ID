import csv, sys, glob, os, time
from API.gmap_placeid_api import GMAP_ID

csv.field_size_limit(sys.maxint)

def safe_dec_enc(data,basic=False):
    if data:
        if isinstance(data, unicode):
            if basic:
                return data.encode('ascii','ignore')
            return data.encode('utf-8','ignore')
        else:
            if basic:
                # REMOVE NON-STANDARD UNICODE-POINTS FROM BYTE-STRING
                return data.decode('ascii','ignore').encode('ascii')
            return data.decode('utf-8','ignore').encode('utf-8')
    return ''

def filter(row):
        Name=(row['Name'].strip()).lower()
        Village=(row['VILLAGE_NAME'].strip()).lower()
        Street=(row['Street Address'].strip()).lower()
        if  Village in Name:
            if Name[:Name.index(Village)].endswith('('):
                Name=Name[:Name.index(Village)-1].strip()
            else:
                Name=Name[:Name.index(Village)]
            #print "VILLAGE :",Name#[:Name.index(Village)]
            return Name#[:Name.index(Village)]#,row['VILLAGE_NAME']
        #elif  in row['Name']:
        #    return row['Name'][:row['Name'].index(row['Street Address'])]#,row['Street Address']
        else:
            #print "NO FILTER :",(row['Name'].strip()).lower()
            return (row['Name'].strip()).lower()

def read_csv(file_path,new_cols,fields,rows):
    with open(file_path, 'rb') as csvFile:
        reader = csv.DictReader(csvFile, dialect=csv.excel)
        reader.fieldnames.extend(new_cols)
        fields.extend(reader.fieldnames)
        rows.extend(reader)

def write_csv(file_path,fields,rows):
    with open(file_path, 'wb') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=fields)
        writer.writerow(dict(zip(fields,fields)))
        for row in rows:
            writer.writerow(row)

path = []

directory = './input'#change path
for filename in os.listdir(directory):
	if filename.endswith(".csv"):
		x = os.path.join(directory, filename)
		path.append(x)

for element in path:
	print element
	file_path = glob.glob(element)[0]
	file_name = os.path.basename(file_path)
	obj = GMAP_ID()

	new_cols = ['Place Id','Place Id Filter']
	fields = []
	rows = []

	read_csv(file_path, new_cols, fields, rows)
	separators = [',', '(']     # BY PRIORITY

	start = 0
	end = len(rows)

	for idx, row in enumerate(rows[start:end]):

			for k in row.keys():
				row[k] = safe_dec_enc(row[k])

			for i in separators:
				parts = row['Name'].split(i)
				if len(parts) == 1:
					continue
					cleaned = parts[0].strip()
					row['Name'] = cleaned
					break
			resp = obj.get_id(row)
			while (resp['status_code'] == 501 or resp['status_code'] == 502):
				time.sleep(3600)
				resp = obj.get_id(row)

			print(start+idx, resp)
			if resp['status_code'] == 201:
				row['Place Id'] = resp['place_id']

			row['Name']=(str(filter(row))).upper()
			resp = obj.get_id(row)
			while (resp['status_code'] == 501 or resp['status_code'] == 502):
				time.sleep(3600)
				resp = obj.get_id(row)

			print(start+idx, resp)
			if resp['status_code'] == 201:
				row['Place Id Filter'] = resp['place_id']

	write_csv('output/{0}_{1}_'.format(start,end)+file_name, fields, rows)
