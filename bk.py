import time
from paperdb import PaperDatabase

db = PaperDatabase("./testdata")
db.create_collection("test")

def test_write_speed():
    t1 = time.time()
    for x in range(1000*20+1):
        db.write_document_sync("test","doc"+str(x),{ "int":x })
        print(x)
    t = time.time() - t1
    print("write: "+str(t))

def __main__():
    test_write_speed()

if __name__ == "__main__":
    __main__()
