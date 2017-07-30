
class CachePool(object):
    def __init__(self,limit=120,ifnone=None,sync=True):
        self.limit = limit
        self.pool = {}
        self.ifnone_callback = ifnone
        self.is_sync = sync

    def put(self,k,v):
        count = len(self.pool)
        if count >= self.limit:
            self.pool.popitem()
        self.pool[k] = v

    def get(self,k,default=None):
        r = self.pool.get(k,None)
        if not r:
            if default:
                return default
            elif self.ifnone_callback:
                r = None
                if self.is_sync:
                    r = self.ifnone_callback(k)
                else:
                    r = await self.ifnone_callback(k)
                self.put(k,r)
                return r
        return r


class PaperClient(object):
    def __init__(self,db,cache_limit=150,preload=False,sync=True):
        self.source = db
        necb = self.no_el_callback if sync else self.no_el_callback_sync
        self.cachemap = CachePool(
                limit=cache_limit,
                ifnone=necb,
                sync=sync
                )
        if preload:
            self._preload()

    @staticmethod
    def cname2docname(name):
        return tuple(name.split('.',maxsplit=2))

    @staticmethod
    def docname2cname(coll,doc):
        return '.'.join((coll,doc))

    def no_el_callback(self,cname):
        coll, doc = self.cname2docname(cname)
        return self.db.read_docment_sync(coll,doc)

    async def no_el_callback_async(self,cname):
        coll, doc = self.cname2docname(cname)
        return await self.db.read_document(coll,doc)

    def get_document(self,name):
        return self.cachemap.get(name)

    def each(self,coll):
        for n in self.db.list_document(coll):
            yield self.get_document(self.docname2cname(coll,n))

    def foreach(self,coll,f):
        for d in self.each(coll):
            f(d)

    @staticmethod
    def match_doc(rule,d2):
        for k in rule:
            if k not in d2:
                return False
            elif rule[k] != d2[k]:
                return False
        return True

    def find(self,coll,rule=None,filter_=None,limit=None,skip=None):
        count = -1
        if limit:
            limit = limit -1
        for d in self.each(coll):
            count += 1
            isMatch = False
            if rule:
                if self.match_doc(rule,d):
                    isMatch = True
                else:
                    isMatch = False
            else:
                isMatch = True
            if filter_:
                if filter_(d):
                    isMatch = isMatch and True
                else:
                    isMatch = False
            if limit:
                if count > limit:
                    break
            if skip:
                if count <= skip:
                    isMatch = False
            if isMatch:
                yield d

    def find_one(self,*args,**kargs):
        return self.find(*args,**kargs,limit=1)

    def _preload(self):
        for c in self.db.list_collection():
            self.foreach(c,(lambda x: x))

