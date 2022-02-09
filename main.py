import hashlib
import sqlite3
import urllib.request
import zlib
from dateutil.parser import parse as parsedate
import os
import json
import UnityPy

from importlib_metadata import version

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
            sqlite_insert_with_param = f"""INSERT INTO "{path}"
                            (etag, modified_date) 
                            VALUES (?, ?);"""

            data_tuple = (etag, date)
            cursor.execute(sqlite_insert_with_param, data_tuple)
            db.commit()
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
            # diff = json.dumps([i for i in new_list if i not in old_list], indent=4)
            # print(diff)
            #get file urls
            downloads = []
            url_path, sha256 = os.path.split(url)
            for i in [i for i in new_list if i not in old_list]:
                downloads.append(f"{url_path}/{i['version']}/{i['assetName']}")
            #save to folder, in the future add this to an async function instead of a for loop
            for i in downloads:
                #save to url end text + version folder
                env = UnityPy.load(urllib.request.urlopen(i).read())
                #make new folder for extracted assets
                for obj in env.objects:
                    if obj.type.name in ["Sprite"]: #, "Sprite"
                        data = obj.read()
                        dest = os.path.join(extracted_folder, data.name)
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
                            with open(os.path.join(extracted_folder, data.name), "wb") as f:
                                f.write(decompressed)
            print("extracted all assets!")
                        # if obj.serialized_type.nodes:
                        #     # save decoded data
                        #     tree = obj.read_typetree()
                        #     data = obj.read()
                        #     fp = os.path.join(extracted_folder, f"{data.name}.json")
                        #     with open(fp, "wt", encoding = "utf8") as f:
                        #         json.dump(tree, f, ensure_ascii = False, indent = 4)
                        # else:
                        #     # save raw relevant data (without Unity MonoBehaviour header)
                        #     data = obj.read()
                        #     fp = os.path.join(extracted_folder, f"{data.name}.bin")
                        #     with open(fp, "wb") as f:
                        #         f.write(data.raw_data)

                        # with open(os.path.join(version_folder, os.path.split(i)[1]), "wb") as f:
                        #     f.write(urllib.request.urlopen(i).read())

                
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
    main()