
class CachePool(object):
    def __init__(self,limit=120,ifnone=None):
        self.limit = limit
        self.pool = {}
        self.ifnone_callback = ifnone

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
                r = self.ifnone_callback(k)
                self.put(k,r)
                return r
        return r


class PaperClient(object):
    def __init__(self,db,cache_limit=150):
        self.source = db
        self.cachemap = CachePool(
                limit=cache_limit,
                ifnone=self.no_el_callback
                )

    @staticmethod
    def cname2docname(name):
        return tuple(name.split('.',maxsplit=2))

    @staticmethod
    def docname2cname(coll,doc):
        return '.'.join((coll,doc))

    def no_el_callback(self,cname):
        coll, doc = self.cname2docname(cname)
        return self.db.read_docment_sync(coll,doc)

    def get_document(self,name):
        return self.cachemap.get(name)

