import requests
import os

siteUrl = "https://api.cartolafc.globo.com/"
siteFolder = "./cartola/2021/"
endpoints = [
	{'url': 'clubes', 'name_file': 'clubes'},
	{'url': 'ligas', 'name_file': 'ligas'},
	{'url': 'rodadas', 'name_file': 'rodadas'},
	{'url': 'partidas', 'name_file': 'partidas'},
	{'url': 'atletas/mercado', 'name_file': 'atletas'}
]

if not os.path.exists(siteFolder):
	os.makedirs(siteFolder)

for item in endpoints:
	response = requests.get(siteUrl + item['url'])
	print(response.json())
	
	file = siteFolder + '2021_' + item['name_file'] + '.json'
	
	with open(file, 'w') as f:
		f.write(response.text)