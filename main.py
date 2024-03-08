import asyncio
import aiohttp
import csv
import json

async def fetch_data(session, url, headers):
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f'Error fetching data: {response.status}')
            return None

async def fetch_additional_data(session, listing_id, headers):
    additional_url = f'https://partner-directory.adobe.io/v1/spp/listing/{listing_id}'
    return await fetch_data(session, additional_url, headers)

async def get_data():
    api_key = 'partner_directory'
    headers = {'x-api-key': api_key}
    filename = "listings.csv"

    async with aiohttp.ClientSession() as session:
        with open(filename, mode='w', newline='', encoding='utf-8') as csv_file:
            fieldnames = ['id', 'name', 'level', 'summary', 'Industries_Served']  # Adjust fieldnames as needed
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(25):
                primary_url = f'https://partner-directory.adobe.io/v1/spp/listings?geo=north-america&country=us&solution=commerce&page={i}'
                primary_data = await fetch_data(session, primary_url, headers)
                if primary_data and 'listings' in primary_data:
                    for listing in primary_data['listings']:
                        
                        #Fetch additional data for each listing
                        Industries_Served = await fetch_additional_data(session, listing.get('name'), headers)

                        # Prepare the listing data
                        simplified_listing = {
                            'id': listing.get('id'),
                            'name': listing.get('name'),
                            'level': listing.get('level'),
                            'summary': listing.get('summary'),
                            'Industries_Served': json.dumps(Industries_Served.get('industries'))  # Convert additional data to JSON string
                        }
                        # Write the listing data to CSV
                        writer.writerow(simplified_listing)

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_data())

if __name__ == "__main__":
    main()
