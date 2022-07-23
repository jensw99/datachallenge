import pyexasol


class ExasolConnector():

    def __init__(self):
        self.conn = pyexasol.connect(dsn='192.168.56.101:8563', user='sys', password='exasol')
        #self.drop_schema()
        self.create_schema()
        self.create_table()

    def to_db(self, df, table):
        self.conn.import_from_pandas(df, table)

    def create_schema(self):
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS datachallenge")

    def drop_schema(self):
        # Careful, deletes everything
        self.conn.execute("DROP SCHEMA IF EXISTS datachallenge CASCADE")
    def create_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS datachallenge(
            id VARCHAR(30),
            likes DECIMAL(24,4),
            replies DECIMAL(24,4),
            social_media VARCHAR(30),
            rating DECIMAL(1,0),
            created_at TIMESTAMP WITH LOCAL TIME ZONE,
            text VARCHAR(15000),
            company VARCHAR(30))""")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS datachallenge_rated(
            id VARCHAR(30),
            likes DECIMAL(24,4),
            replies DECIMAL(24,4),
            social_media VARCHAR(30),
            rating DOUBLE PRECISION,
            created_at TIMESTAMP WITH LOCAL TIME ZONE,
            text VARCHAR(15000),
            company VARCHAR(30))""")
    def delete_unrated_rows(self):
        self.conn.execute("DELETE FROM DATACHALLENGE.DATACHALLENGE WHERE RATING IS NULL")

    def from_db(self, query):
        return self.conn.export_to_pandas(query)