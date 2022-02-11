import hashlib
import io
import sqlite3
import time
import aiohttp
import zlib
from dateutil.parser import parse as parsedate
import os
import json
import UnityPy
import asyncio
import urllib.request
import threading 
import ssl

db = sqlite3.connect("hashes.db")
cursor = db.cursor()
base_url = "https://d2z2594camlaoa.cloudfront.net/v100"
pathlist = ['beta2/PC/cg/tcg/853b6b484ddd2fb1326f6e4cab8979cfc97259c1069ecf99d74147e42dd78da4',
'beta2/PC/ncg/en/9293306eaa9fa70fa756f0325bd080e87616bbbc99dd07370bde7cbebde9057c',
'beta2/PC/ncg/tcg/b3b6e36cc19d2ace603d60c9caeab82b351ba3fb8eef8fb299336dcc36953834',
'beta2/PC/cg/common/d498b045da477552ded5d37cd3defb07a827222e3793f192d67ce6ca13fd5503',
'beta2/PC/ncg/common/96d47ced60708c6941143eba731969d95911cce699ab1c9e5fc2ff47a96f2989']
# cursor.execute("""CREATE TABLE IF NOT EXISTS "beta2/PC/cg/tcg" (
#                                         etag string PRIMARY KEY,
#                                         modified_date DATE NOT NULL
#                                     )""")
# cursor.execute("""CREATE TABLE IF NOT EXISTS "beta2/PC/ncg/en" (
#                                         etag string PRIMARY KEY,
#                                         modified_date DATE NOT NULL
#                                     )""")
# cursor.execute("""CREATE TABLE IF NOT EXISTS "beta2/PC/ncg/tcg" (
#                                         etag string PRIMARY KEY,
#                                         modified_date DATE NOT NULL
#                                     )""")
# cursor.execute("""CREATE TABLE IF NOT EXISTS "beta2/PC/cg/common" (
#                                         etag string PRIMARY KEY,
#                                         modified_date DATE NOT NULL
#                                     )""")
# cursor.execute("""CREATE TABLE IF NOT EXISTS "beta2/PC/ncg/common" (
#                                         etag string PRIMARY KEY,
#                                         modified_date DATE NOT NULL
#                                     )""")                           
# db.commit()

def main():
    for i in pathlist:
        loop = asyncio.get_event_loop()
        url = GetUrlFromPath(i)
        etag, date = GetUrlHeaders(url)
        etag = etag.strip('"')
        #get path without hash at the end
        path, sha256 = os.path.split(i)
        #fetch etag
        rows = cursor.execute(
            f"""SELECT etag, modified_date FROM "{path}" WHERE etag = ?""",
            (etag,),
        ).fetchall()
        #insert new value
        if not rows:
            newest_file = cursor.execute(f"""SELECT * FROM "{path}" ORDER BY modified_date DESC LIMIT 1""")
            old_etag = newest_file.fetchone()
            #add the new etag to the db
            sqlite_insert_with_param = f"""INSERT INTO "{path}"
                            (etag, modified_date) 
                            VALUES (?, ?);"""

            data_tuple = (etag, date)
            cursor.execute(sqlite_insert_with_param, data_tuple)
            # db.commit()
            print(f"new etag found: {etag}\npath: {path}")
            response = urllib.request.urlopen(url).read()
            path = path.replace('/', '_')
            #get the real path
            realpath = os.path.dirname(os.path.realpath(__file__))

            dir = os.path.join(realpath, path)
            version_folder = os.path.join(dir, etag)
            extracted_folder = os.path.join(version_folder, "extracted")
            file = os.path.join(version_folder, "catalog.json")
            os.makedirs(version_folder, exist_ok=True)
            os.makedirs(extracted_folder, exist_ok=True)
            with open(file, "wb") as f:
                f.write(response)

            #check for file dif
            new = json.load(open(file))
            if old_etag:
                old = json.load(open(os.path.join(dir, old_etag[0], "catalog.json")))
            else:
                old = {'informations':[]}
            new_list = new['informations']
            old_list = old['informations']
            #get file urls
            downloads = []
            old_downloads = []
            # old_list =[]
            url_path, sha256 = os.path.split(url)
            # downloads.append(f"{url_path}/202111181555/cc/cc3059c3")
            for i in [i for i in new_list if i not in old_list]:
                downloads.append(f"{url_path}/{i['version']}/{i['assetName']}")
                #get the old asset version
                old_asset = next(item for item in old_list if item['assetName'] == i['assetName'])
                old_downloads.append(f"{url_path}/{old_asset['version']}/{old_asset['assetName']}")
            #save to folder, in the future add this to an async function instead of a for loop
    files = loop.run_until_complete(download_list(downloads, loop))
    assets = loop.run_until_complete(extract_asset_list(files, extracted_folder, old_downloads))

async def extract_asset(filebytes, filename, folder, old_downloads):
    env = UnityPy.load(filebytes)
    for obj in env.objects:
        if obj.type.name in ["Sprite"]: #, "Sprite"
            data = obj.read()
            dest = os.path.join(folder, data.name)
            # make sure that the extension is correct
            # you probably only want to do so with images/textures
            dest, ext = os.path.splitext(dest)
            dest = dest + ".png"
            img = data.image
            img.save(dest)
        elif obj.type == "TextAsset":
            data = obj.read()
            bytedata = bytes(data.script)
            if bytedata[0:8] == b"\x59\x44\x4c\x5a\x01\x00\x00\x00":
                print(f"magic number detected, decompressing '{data.name}'")
                #decompress data minus header
                decompressed = zlib.decompress(bytedata[8:len(bytedata)])
                #find index of len before gpr
                start = io.BytesIO(decompressed[decompressed.find(b'\xa5'):])
                deserialized = DeserializeText(start)
                #list comprehension to get the old url
                old_url = [item for item in old_downloads if filename in item]
                old_deserialized = []
                if old_url:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(old_url[0]) as response:
                            data = await response.read()
                            old_env = UnityPy.load(data)
                            for obj in [i for i in old_env.objects if i.type == "TextAsset"]:
                                data = obj.read()
                                bytedata = bytes(data.script)
                                decompressed = zlib.decompress(bytedata[8:len(bytedata)]) 
                                start = io.BytesIO(decompressed[decompressed.find(b'\xa5'):])
                                old_deserialized = DeserializeText(start)

                with open(os.path.join(folder, f"{data.name}.txt"), "w", encoding='utf8') as f:
                    #diff the old version and write new content
                    for i in [i for i in deserialized if i not in old_deserialized]:
                        f.write(i[0]+" : " + i[1] + "\n")
                print(f"Wrote to {data.name}.txt")

async def extract_asset_list(files, extracted_folder, old_downloads):
    results = await asyncio.gather(*[extract_asset(file, filename, extracted_folder, old_downloads) for file, filename in files], return_exceptions=True)  
    return results    
        
async def download(session, url):
    async with session.get(url) as response:
        return await response.read(), os.path.split(url)[1]

async def download_list(urls, loop):
    async with aiohttp.ClientSession(loop=loop) as session:
        results = await asyncio.gather(*[download(session, url) for url in urls], return_exceptions=True)
        return results

def DeserializeText(bytes):
    deserialized = []
    while 1:
        id_len = bytes.read(1)
        if not id_len:
            break
        identifier = int.from_bytes(id_len, byteorder='big')
        if identifier > 216:
            id_len = bytes.read(identifier-216)
            id_len = int.from_bytes(id_len, byteorder='big')
        else:
            id_len = identifier-160
        id = bytes.read(id_len)
        
        #same thing for txt
        txt_len = bytes.read(1)
        if not txt_len:
            break
        identifier = int.from_bytes(txt_len, byteorder='big')
        if identifier > 216:
            txt_len = bytes.read(identifier-216)
            txt_len = int.from_bytes(txt_len, byteorder='big')
        else:
            txt_len = identifier-160
        txt = bytes.read(txt_len)
        deserialized.append((id.decode(), txt.decode()))
    return deserialized

def GetUrlHeaders(url : str):
    response = urllib.request.urlopen(url)
    etag, date = response.headers['ETag'], response.headers['Last-Modified']
    date = parsedate(date)
    return(etag, date)

def GetUrlFromPath(path : str):
    names = []
    while 1:
        path, name = os.path.split(path)

        if name != "":
            names.append(name)
        else:
            break
    
    names.reverse()
    url = base_url
    for i in names:
        #if less than the sha256 hash
        if len(i) < 64:
            url += "/" + hashlib.sha1(bytes(i, "utf-8")).hexdigest()
        elif len(i) == 64: 
            url += "/" + i
    return url

#need to seperate download url from header function and download new file
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Finished in %s seconds" % (time.time() - start_time))