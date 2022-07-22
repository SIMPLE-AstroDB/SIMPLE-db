from scripts.ingests.utils import *

logger.setLevel(logging.INFO)

ads.config.token = os.getenv('ADS_TOKEN')

if not ads.config.token:
    logger.error("An ADS_TOKEN environment variable must be set in order to auto-populate the fields.")
    raise SimpleError

db = load_simpledb('SIMPLE.db', recreatedb=True)

# Update individual publications
db.Publications.update().where(db.Publications.c.publication == 'Zapa04').values(bibcode='2004ApJ...615..958Z').execute()
db.Publications.update().where(db.Publications.c.publication == 'Card12').values(bibcode='2012PhDT.......463C').execute()
db.Publications.update().where(db.Publications.c.publication == 'Gold99').values(bibcode='1999A&A...351L...5E').execute()
db.Parallaxes.update().where(db.Parallaxes.c.reference == 'Liu16').values(reference='Liu_16').execute()
db.Publications.delete().where(db.Publications.c.publication == 'Liu16').execute()

# Removing accents
db.Publications.update().where(db.Publications.c.publication == 'Góme01').values(publication='Gome01').execute()
db.Publications.update().where(db.Publications.c.publication == 'Lópe04').values(publication='Lope04').execute()

# Find publications missig DOIs that have Bibcodes
missing_dois = db.query(db.Publications).filter(or_(db.Publications.c.doi == None, db.Publications.c.doi == ''),
                                                and_(db.Publications.c.bibcode != None, db.Publications.c.bibcode != '')).table()
n_missing_dois = len(missing_dois)
logger.info(f"Searching for data on {n_missing_dois} publications missing DOIs with ADS Bibcodes")

n_added_doi = 0
n_no_doi = 0

for pub in missing_dois:
    pub_name = pub['publication']
    bibcode = pub['bibcode']
    logger.debug(f"Searching ADS for {bibcode}")
    bibcode_matches = ads.SearchQuery(bibcode=bibcode, fl=['id', 'bibcode', 'title', 'first_author', 'year', 'doi'])
    bibcode_matches_list = list(bibcode_matches)
    if len(bibcode_matches_list) == 0:
        logger.error('not a valid bibcode:' + str(bibcode))
        logger.error('nothing added')
        continue
    elif len(bibcode_matches_list) > 1:
        logger.error('should only be one matching bibcode for:' + str(bibcode))
        logger.error('nothing added')
        continue
    elif len(bibcode_matches_list) == 1:
        logger.debug("Publication found in ADS using bibcode: " + str(bibcode))
        article = bibcode_matches_list[0]
        if article.doi is not None:
            doi_add = article.doi[0]
            try:
                db.Publications.update().where(db.Publications.c.publication == pub_name).values(doi=doi_add).execute()
                n_added_doi += 1
                logger.info(f"Added DOI: {article.first_author}, {article.year}, {article.bibcode}, {article.doi}, {article.title}")
            except:
                logger.error('adding DOI failed')
                raise SimpleError
        else:
            n_no_doi += 1
            msg = f"{pub_name} DOI is None"
            logger.debug(msg)

logger.info(f"{n_no_doi} pubs with no DOI \n {n_added_doi} DOIs added")

db.save_database('data/')