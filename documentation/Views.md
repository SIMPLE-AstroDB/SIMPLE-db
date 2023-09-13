# Views

SIMPLE offers several database views for ease of searching. 
A database view is a virtual table that is defined by a query. 
It does not store any data itself, but rather provides a way to access data from one or more tables in a database.

Because views are not part of the standard SIMPLE schema, they must be activated when using `astrodbkit2`. 

```python
import sqlalchemy as sa
from astrodbkit2.astrodb import Database, and_, or_

# Establish connection to database; your connection string may be different
connection_string = 'sqlite:///SIMPLE.db'
db = Database(connection_string)

# Inspect views in the database
insp = sa.inspect(db.engine)

# Examine available views
print(insp.get_view_names())  # list of available views
print(insp.get_view_definition('ParallaxView'))  # view definition

# Create object reflecting the view
# DO NOT USE db.metadata or you will treat the view as a real table 
# and break other astrodbkit2 functionality
PhotometryView = sa.Table('PhotometryView', sa.MetaData())  
insp.reflect_table(PhotometryView, include_columns=None)

ParallaxView = sa.Table('ParallaxView', sa.MetaData())  
insp.reflect_table(ParallaxView, include_columns=None)

# Query as normal using the created object
db.query(PhotometryView).limit(10).table()  # sample of photometry
db.query(ParallaxView).order_by(ParallaxView.c.distance).limit(10).table()  # sample of closest sources
```

We encourage users to call `insp.get_view_names()` as described above to see what views are available.   
Currently, we have the following views: 

 - ParallaxView - lists only adopted parallax values and computes distance from them
 - PhotometryView - pivoted table showing average 2MASS, WISE, and IRAC photometry as one line per source

