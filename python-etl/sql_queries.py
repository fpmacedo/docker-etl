

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


#UPSERT WITH CONFLICT WITHOUT USING UPSERT

create_raw_temp_table = """
                    DROP TABLE IF EXISTS raw_temp;

                    CREATE TABLE raw_temp (
                              Op VARCHAR(50),
                              oid__id UUID,
                              createdAt BIGINT,
                              updatedAt BIGINT,
                              lastSyncTracker BIGINT,
                              array_trackingEvents JSONB,
                              PRIMARY KEY (oid__id)
                                );
                   """

insert_raw_temp_table = """
                            DELETE FROM raw_temp
                            WHERE raw_temp.oid__id = (%s);

                            INSERT INTO raw_temp (Op, oid__id, createdAt, updatedAt, lastSyncTracker, array_trackingEvents)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """

upsert_raw_table = """
                        DELETE FROM raw
                        USING raw_temp
                        WHERE raw.oid__id = raw_temp.oid__id;
                        
                        INSERT INTO raw (Op, oid__id, createdAt, updatedAt, lastSyncTracker, array_trackingEvents)
                        SELECT Op, oid__id, createdAt, updatedAt, lastSyncTracker, array_trackingEvents
                        FROM raw_temp;
                  """

json_threat = """
                SELECT op, oid__id, createdat, updatedat, lastsynctracker,
                       tracking_event->> 'to' as tracking_event_to,
                       tracking_event->> 'from' as tracking_event_from,
                       tracking_event->> 'status' as tracking_event_status,
                       tracking_event->'createdAt'->'$date' as tracking_event_date,
                       tracking_event->> 'description' as tracking_event_description,
                       tracking_event->> 'trackerType' as tracking_event_type,
                       tracking_event->> 'trackingCode' as tracking_event_code
                       FROM (SELECT op, oid__id, createdat, updatedat, lastsynctracker, jsonb_array_elements(array_trackingevents) as tracking_event 
                       FROM public.raw) as base;
              """