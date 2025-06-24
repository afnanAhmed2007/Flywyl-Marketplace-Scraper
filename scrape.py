# <-------- AFNAN's CODE ---------->
# CODE OBJECTIVE: To scrape top listings of products from all marketplaces and matching the best listing based on product & vendor name

#importing libraries 
import nest_asyncio
nest_asyncio.apply()

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

CONCURRENCY_LIMIT = 9 # concurrently scraping from 9 URLS -> can be increased or decreased based on CPU & RAM
BATCH_SIZE = 3 # 3 product & vendor matches occur at a time

results_list = []

# determines which listing has the highest matching score 
def fuzz_filter(results, product_name, vendor_name, product_weight = 0.75, vendor_weight = 0.25, threshold=85): 
     if results: 
        scores = []

        for item in results: 
            name_score = fuzz.token_set_ratio(product_name, item.get("PRODUCT", "")) # obtaining score based on name
            vendor_score = fuzz.token_set_ratio(vendor_name, item.get("VENDOR", "")) # obtaining score based on vendor
            total_score = (product_weight * name_score) + (vendor_weight * vendor_score) # getting total score based on weights
            scores.append((item, total_score))

        scores.sort(key=lambda x: x[1], reverse=True)

        #only get listings with the scores above the threshold (85)
        best_matches = [(item, score) for item, score in scores if score >= threshold]

        if best_matches: 
            #return listing with the highest score (if there is one)
            return best_matches[0][0]
        
     return None

# filters webpage and obtains product name, vendor, and link
async def extract_products(semaphore, browser, url, css_selector, marketplace):
    async with semaphore: 
        page = await browser.new_page()

        if marketplace=="GCP": 
            # conditions to wait for GCP pg to load 
            await page.goto(url)
            await page.wait_for_selector("div.cfc-panel-body-scroll-content")
        else: 
            # conditions to wait for AWS & AZURE pgs to load 
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle")

        # scrape all the content based on css_selector 
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        elements = soup.select(css_selector)

        results = []

        # if page contains wanted content
        if elements: 
            for el in elements:
                name, link, vendor =  None, None, None

                if marketplace=="AWS":

                    # access a tag element for AWS
                    a_tag = el.find(
                    "a",
                    href=True,
                    string=lambda s: s and s.strip()        
                    )

                    # get name and link through a_tag
                    name = a_tag.get_text(strip=True) if a_tag else None
                    link = a_tag['href'] if a_tag else None

                    # get vendor based on vendorName
                    vendor_tag = el.select_one('[data-semantic="vendorName"]')
                    vendor = vendor_tag.get_text(strip=True) if vendor_tag else None

                elif marketplace=="AZURE":

                    # get link for AZURE
                    link_tag = el.find("a", href=True)['href']
                    link = f"https://azuremarketplace.microsoft.com{link_tag}" if link_tag else None

                    # get name through titleContent & vendor through providerSection
                    name_tag = el.select_one(".tileContent")
                    vendor_tag = el.select_one(".providerSection")
                    name = name_tag.get_text(strip=True).partition("By")[0] if name_tag else None
                    vendor = vendor_tag.get_text(strip=True).replace("By", "", 1).strip() if vendor_tag else None

                elif marketplace=="GCP": 

                    # get link for GCP
                    link_tag = el.get('href')
                    link = f"https://console.cloud.google.com{link_tag}" if link_tag else None
                
                    # get name through h3 & vendor through h4
                    name_tag = el.select_one("h3.cfc-truncated-text")
                    vendor_tag = el.select_one("h4.cfc-truncated-text")
                    name = name_tag.get_text(strip=True) if name_tag else None
                    vendor = vendor_tag.get_text(strip=True).replace("By", "", 1).strip() if vendor_tag else None

                #append results
                results.append({
                    "PRODUCT": name,
                    "VENDOR": vendor,
                    "LINK": link,
                })
        else: 
            #empty results 
            results = []

        await page.close()
        return results

#get each URL, CSS_SELECTOR based on each marketplace
def AWSURL(vendor_name):
    query = f"{vendor_name}".replace(" ", "+")
    base = "https://aws.amazon.com/marketplace/search/results?searchTerms="
    css_selector = "[class^='awsui_body-cell-content']"
    return  f"{base}{query}", css_selector, "AWS"

def AZUREURL(vendor_name):
    query = f"{vendor_name}".replace(" ", "%20")
    base = "https://azuremarketplace.microsoft.com/en-us/marketplace/apps?search="
    css_selector = "[class^='spza_tileWrapper']"
    return  f"{base}{query}", css_selector, "AZURE"

def GCPURL(vendor_name):
    query = f"{vendor_name}".replace(" ", "%20")
    base = "https://console.cloud.google.com/marketplace/browse?hl=en&inv=1&invt=Ab0uyg&q="
    css_selector = "[class^='mp-search-results-list-item-link']"
    return  f"{base}{query}", css_selector, "GCP"


counter = 1

# combines all functions together to append results 
async def get_final_listings(semaphore, browser, Product, Vendor):

    # counter in terminal to see if results are being appended
    global counter 

    # calling URL functions 
    urls = [
        AWSURL(Vendor),
        AZUREURL(Vendor),
        GCPURL(Vendor)
    ]

    # holding extract_products function to run concurrently 
    tasks = [
        extract_products(semaphore, browser, url, css_selector, marketplace)
        for url, css_selector, marketplace in urls
    ]

    # get scraped listings  concurrently 
    aws, azure, gcp = await asyncio.gather(*tasks)

    # passing listings through fuzz_filter concurrently
    aws_list, azure_list, gcp_list = await asyncio.gather(
    asyncio.to_thread(fuzz_filter, aws, Product, Vendor),
    asyncio.to_thread(fuzz_filter, azure, Product, Vendor),
    asyncio.to_thread(fuzz_filter, gcp, Product, Vendor)
    )

    # printing counter 
    print(counter)
    counter+=1

    result = {
        "PRODUCT": Product,
        "VENDOR": Vendor,
        "AWS": aws_list,
        "AZURE": azure_list,
        "GCP": gcp_list
    }

    # appending result (3 marketplace listings) to a final list
    results_list.append(result)

# maintains scraping from the web runs smoothly 
async def run_batch(product_vendor_list):

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with async_playwright() as p:

        # creating a browser
        browser = await p.chromium.launch(headless=True)

        tasks = []
        for i in range(0, len(product_vendor_list), BATCH_SIZE):


            batch = product_vendor_list[i:i + BATCH_SIZE]

            # creating batch task for products and running final_listings concurrently 
            batch_tasks = [
                get_final_listings(semaphore, browser, product, vendor)
                for product, vendor in batch
            ]

            tasks.append(asyncio.gather(*batch_tasks, return_exceptions=True))

        # closing browser when task is finished
        await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

# converting file to a list
def load_product_list(file):
    df = pd.read_excel(file)
    product_vendor_list = list(zip(df['solution_name'], df['vendor']))
    return product_vendor_list

# main function being called 
def process_file(file_obj):
    global results_list
    results_list = []
    product_vendor_list = load_product_list(file_obj)
    asyncio.run(run_batch(product_vendor_list))

    # returning dataframe of listings of products from all 3 marketplaces
    return pd.DataFrame(results_list)

