import httpx
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
import json

# Constants
CEFI_THREDDS_BASE = "https://psl.noaa.gov/thredds/catalog/Projects/CEFI/regional_mom6/cefi_portal/"


def find_all_files_thredds(base_catalog_url):
    """
    Recursively crawls a THREDDS catalog and returns a dict of catalogs that contain .nc files.
    Keys are catalog HTML URLs; values are lists of NetCDF file access URLs (OPeNDAP or HTTPServer).
    """
    visited = set()
    result = {}

    def recurse(catalog_xml_url):
        if catalog_xml_url in visited:
            return
        visited.add(catalog_xml_url)

        try:
            with httpx.Client(timeout=10.0) as client:
                res = client.get(catalog_xml_url)
                res.raise_for_status()
        except Exception as e:
            print(f"Failed to fetch: {catalog_xml_url} — {e}")
            return

        try:
            root = ET.fromstring(res.content)
        except ET.ParseError as e:
            print(f"Failed to parse XML from: {catalog_xml_url} — {e}")
            return

        ns = {
            'x': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0',
            'xlink': 'http://www.w3.org/1999/xlink'
        }
        datasets = root.findall(".//x:dataset", ns)
        nc_urls = []

        for ds in datasets:
            url_path = ds.attrib.get('urlPath', '')

            if url_path.endswith('.nc'):
                # Build access URL (assuming OPeNDAP or HTTPServer base)
                access_url = urljoin("https://psl.noaa.gov/thredds/dodsC/", url_path)
                nc_urls.append(access_url)

        if nc_urls:
            html_url = catalog_xml_url.replace('/catalog.xml', '/catalog.html')
            result[html_url] = nc_urls

        # Recurse into sub-catalogs
        for cat_ref in root.findall(".//x:catalogRef", ns):
            href = cat_ref.attrib.get('{http://www.w3.org/1999/xlink}href')
            if href:
                subcat_xml_url = urljoin(catalog_xml_url, href)
                recurse(subcat_xml_url)

    # Ensure the base catalog URL ends with 'catalog.xml'
    if not base_catalog_url.endswith("catalog.xml"):
        base_catalog_url = base_catalog_url.rstrip("/") + "/catalog.xml"

    recurse(base_catalog_url)

    return result

if __name__ == "__main__":

    catalog_dict = find_all_files_thredds(CEFI_THREDDS_BASE)

    # output to json format
    print("Found catalogs and files:")
    print(json.dumps(catalog_dict, indent=2))
    # Optionally, you can save the output to a file
    with open('cefi_thredds_catalog.json', 'w') as f:
        json.dump(catalog_dict, f, indent=2)
    print("Catalog information saved to 'cefi_thredds_catalog.json'")

