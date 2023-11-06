from base import Base,engine
from tables import PprRawAll
if __name__=="__main__":
    Base.metadata.create_all(engine)