import json

with open("dataMay-31-2017.json") as jsonfile:
    js = json.load(jsonfile)

with open("dataMay-31-2017_corrected.json", 'w') as outfile:
    for d in js['data']:
		outfile.write('{')
		for index in xrange(len(js['cols'])):
			if isinstance(d[index], int):
				outfile.write('"' + js['cols'][index].encode('utf-8') + '": "' + str(d[index]) + '"')
			else:
				outfile.write('"' + js['cols'][index].encode('utf-8') + '": "' + d[index].encode('utf-8') + '"')
			if index != len(js['cols']) - 1:
				outfile.write(', ')
			
		outfile.write('}')
		outfile.write('\n')