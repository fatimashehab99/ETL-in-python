from scripts.common.base import Base
from sqlalchemy import cast,Column,Integer,String,Date
from sqlalchemy.orm import column_property

class PprRawAll(Base):
    __tablename__ = "ppr_raw_all"  # Define the table name
    id = Column(Integer, primary_key=True)
    date_of_sale = Column(String(55))
    address = Column(String(255))
    postal_code = Column(String(55))
    county = Column(String(55))
    price = Column(String(55))
    description = Column(String(255))
    transaction_id=column_property(
        date_of_sale + "_" + address + "_" + county + "_" + price
    )
    
    
    