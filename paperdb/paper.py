import os
import asyncio
import os.path as _p
import aiofiles as _af
import ujson as _j

class PaperDatabase(object):
    def __init__(self,root):
        self.root = root
        self.init_database()

    def init_database(self):
        if not self.has_collection("_system"):
            self._init_database()

    def _init_database(self):
        self.create_collection("_system")
        self.write_document_sync("_system","config",{
            "config":{
                "version":1
                }
            })

    def get_coll_path(self,name):
        return _p.join(self.root,name)

    def get_doc_path(self,collname,docname):
        return _p.join(self.get_coll_path(collname),docname+".json")

    def has_collection(self,name):
        return _p.exists(self.get_coll_path(name))

    def has_document(self,collname,docname):
        return _p.exists(self.get_doc_path(collname,docname))

    def create_collection(self,name):
        if self.has_collection(name):
            return None
        os.mkdir(self.get_coll_path(name))

    async def read_document(self,coll,name):
        async with _af.open(self.get_doc_path(coll,name)) as f:
            return await _j.loads(f.read())

    def read_document_sync(self,coll,name):
        with open(self.get_doc_path(coll,name)) as f:
            return _j.loads(f.read())

    async def write_document(self,coll,name,doc):
        async with _af.open(self.get_doc_path(coll,name),"w+") as f:
            await f.write(_j.dumps(doc))

    def write_document_sync(self,coll,name,doc):
        with open(self.get_doc_path(coll,name),"w+") as f:
            f.write(_j.dumps(doc))

    def list_collection(self):
        return os.listdir(self.root)

    def list_document(self,coll):
        l = os.listdir(self.get_coll_path(coll))
        return map((lambda e: e.split('.',maxsplit=2)[0]), l)

