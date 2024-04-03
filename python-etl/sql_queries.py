

#DROP TABLES

drop_raw_table = "DROP TABLE IF EXISTS raw;"



#CREATE TABLES

create_raw_table =  """
                        CREATE TABLE raw (
                              Op VARCHAR(50),
                              oid__id UUID,
                              createdAt BIGINT,
                              updatedAt BIGINT,
                              lastSyncTracker BIGINT,
                              array_trackingEvents JSONB,
                              PRIMARY KEY (oid__id)
                                );
                    """


create_table_queries = [create_raw_table]
drop_table_queries = [drop_raw_table]