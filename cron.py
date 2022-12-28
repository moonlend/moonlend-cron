import json
import requests
from bs4 import BeautifulSoup as bs
import time
import pymongo
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

load_dotenv()

data = requests.get("https://raw.githubusercontent.com/moonlend/moonlend-nft-list/master/nft-list.json").json()
PASSWORD = os.getenv('MONGODBPASSWORD')
client = pymongo.MongoClient(f"mongodb+srv://ninja:{PASSWORD}@oracle-atlas.2mwhyc5.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))


def moonsama_marketplace_price(address, link):

	query = f"""{{ 
		latestOrders: 
			orders( where: {{
				active: true, buyAsset: \"0x0000000000000000000000000000000000000000-0\", sellAsset_starts_with: \"{address}\"
				}} 
			orderBy: pricePerUnit orderDirection: asc skip: 0 first: 1 ) {{ 
				id orderType createdAt active pricePerUnit 
				}}
			}}"""
	
	resp = (requests.post(link, json={"query": query})).json()
	floor = int(resp["data"]["latestOrders"][0]["pricePerUnit"])
	return floor


def moonbeans_price(address, link):
	query = f"""{{ 
		allAsks(condition: {{collectionId: \"{address}\"}}, 
		orderBy: VALUE_ASC, first: 1) {{ 
			nodes {{ 
				id timestamp value __typename }} 
				__typename 
				}}
			}}"""
	
	resp = (requests.post(link, json={"query": query})).json()
	floor = int(resp["data"]["allAsks"]["nodes"][0]["value"])
	return floor


def raregems_price(link):
	resp = requests.get(link)
	soup = bs(resp.content, features="html.parser")
	parent_element = soup.find("div", text="Min Price").parent
	floor = int(parent_element.find("img").next_sibling.strip()) * 10**18
	return floor


def update_db():

    for collection in data["tokens"]:
        collection["address"] = collection["address"].lower()
        prices = []

        for marketplace in collection["marketplaces"]:
            if marketplace["name"] == "Moonsama Marketplace":
                try:
                    price = moonsama_marketplace_price(collection["address"], marketplace["link"])
                    obj = {
                        "timestamp" : int(time.time()),
                        "marketplace" : marketplace["name"],
                        "price" : str(price)
                    }
                    prices.append(obj)
                except:
                    pass

            elif marketplace["name"] == "Moonbeans":
                try:
                    price = moonbeans_price(collection["address"], marketplace["link"])
                    obj = {
                        "timestamp" : int(time.time()),
                        "marketplace" : marketplace["name"],
                        "price" : str(price)
                    }
                    prices.append(obj)
                except:
                    pass
            elif marketplace["name"] == "Raregems":
                try:
                    price = raregems_price(marketplace["link"])
                    obj = {
                        "timestamp" : int(time.time()),
                        "marketplace" : marketplace["name"],
                        "price" : str(price)
                    }
                    prices.append(obj)
                except:
                    pass
            
        if len(prices) == 0:
            continue
        
        table = client["nft_collections_moonriver"][collection["address"]]
        table.insert_many(prices)
        table.delete_many({"timestamp": { "$lt": int(time.time()) - 30*24*3600 }}) #delete data that is older than a month

update_db()