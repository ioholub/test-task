import asyncio
import csv
import re

import aiohttp
from aiohttp import ClientResponseError
from bs4 import BeautifulSoup

product_mask = re.compile(r"products/(?P<product_slug>[A-Za-z0-9-]+)\"")
PRODUCT_URL_TEMPLATE = "https://{shop}/products/{handle}.json"
CONTACT_PAGES = (
    "/",
    "pages/about",
    "pages/about-us",
    "pages/contact",
    "pages/contact-us"
)


async def request(method, url, load=False, **kwargs):
    if not url.startswith("http"):
        url = "https://" + url
    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, raise_for_status=True, **kwargs) as response:
            if load:
                response = await response.json()
            else:
                response = await response.text()
            return response


def prepare_urls(url):
    result = [url]
    for contact_page in CONTACT_PAGES:
        result.append(url + "/" + contact_page)
    return result


async def search_contacts_page(url):
    for u in prepare_urls(url):
        try:
            return await request("GET", u)
        except ClientResponseError as e:
            # no contact page found, proceed
            if e.status >= 400:
                continue
            raise


async def get_contacts(url):
    contacts = dict.fromkeys(["email", "facebook", "twitter"])
    contacts_page = await search_contacts_page(url)
    if not contacts_page:
        return contacts
    soup = BeautifulSoup(contacts_page, "lxml")
    for link in soup.find_all("a"):
        if link.get("href") is None:
            continue
        if "facebook" in link.get("href").lower():
            contacts["facebook"] = link.get("href").lower()
        elif "twitter" in link.get("href").lower():
            contacts["twitter"] = link.get("href").lower()
        elif link.get("href").startswith("mailto:"):
            contacts["email"] = link.get("href").replace("mailto:", "")
        await asyncio.sleep(0)
    return contacts


async def get_product_handles(url, count=5):
    result = set()
    try:
        products = await request("GET", url + "/collections/all")
    except ClientResponseError as exc:
        if exc.status >= 400:
            return result
        raise
    matches = product_mask.finditer(products)
    while len(result) < count:
        match = next(matches, None)
        if not match:
            break
        result.add(match.group("product_slug"))
    return result


async def get_products(url, product_handles):
    result = {}
    product_urls = [
        PRODUCT_URL_TEMPLATE.format(shop=url, handle=product_handle)
        for product_handle in product_handles
    ]
    products_data = await asyncio.gather(
        *[request("GET", product_url, load=True) for product_url in product_urls],
        return_exceptions=True
    )
    for idx, product_data in enumerate(products_data, start=1):
        product = product_data["product"]
        result[f"title_{idx}"] = product["title"]
        product_images = product["images"]
        if product_images:
            result[f"image_{idx}"] = product_images[0]["src"]
    return result


async def parse(url):
    product_handles = await get_product_handles(url)
    contacts = await get_contacts(url)
    products = await get_products(url, product_handles)
    return {**contacts, **products, "url": url}


def save_csv(data, path):
    # filter out request failures (got blocked as a result of multiple requests)
    data = [item for item in data if isinstance(item, dict)]
    columns = max(data, key=len).keys()
    with open(path, 'w', newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def read_csv(path):
    stores_list = []
    with open(path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            stores_list.append(row["url"])
    return stores_list


async def main(urls):
    results = await asyncio.gather(
        *[parse(url) for url in urls],
        return_exceptions=True
    )
    return results


if __name__ == '__main__':
    shop_urls = read_csv("stores.csv")
    parsed_data = asyncio.run(main(shop_urls))
    save_csv(parsed_data, "parsed_stores_data.csv")
