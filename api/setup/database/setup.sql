CREATE SCHEMA hackathon_q4_metadata_api;

CREATE TABLE hackathon_q4_metadata_api.users
(
  username VARCHAR(8000),
  password VARCHAR(8000),
  fullname VARCHAR(8000),
  email VARCHAR(8000)
);

INSERT INTO hackathon_q4_metadata_api.users (username, password, fullname, email)
VALUES
('arjain', 'arjain', 'Archit Jain', 'archit.jain@joshtechnologygroup.com'),
('hsingh', 'hsingh', 'Hitesh Singh', 'hitesh.singh@joshtechnologygroup.com'),
('vsoni', 'vsoni', 'Vijayant Soni', 'vijayant.soni@joshtechnologygroup.com'),
('abjain', 'abjain', 'Abhishek Jain', 'abhishek.jain@joshtechnologygroup.com'),
('mgorman', 'mgorman', 'Mark Gorman', 'mgorman@square-root.com');


WITH l AS (SELECT
             l.FileOrTableName_From,
             l.ObjectTimeStamp_From - cast(now() AS DATE) days_since_extract,
             l.ObjectTimeStamp_From,
             l.LoadDate,
             l.PathOrSchema_From,
             row_number()
             OVER (PARTITION BY l.FileOrTableName_From, l.LoadDate
               ORDER BY l.ObjectTimeStamp_From DESC) AS   is_most_recent_objecttimestamp,
             row_number()
             OVER (PARTITION BY l.FileOrTableName_From
               ORDER BY l.LoadDate DESC)             AS   is_most_recent_loaddate
           FROM Util_dbo.TableETLLog l),
    l2 AS (SELECT *
           FROM l
           WHERE l.is_most_recent_objecttimestamp = 1
                 AND l.is_most_recent_loaddate = 1),
    sourceupdatedate AS (
      SELECT
        s.ID,
        s.FileNameSearchString,
        replace(to_char(f.ObjectTimeStamp_From, 'YYYYMMDD HH24:MI:SS'), '-', '') SourceUpdateDate
      FROM Util_raw.DWDataSourceFile s
        LEFT JOIN (SELECT *
                   FROM
                     (
                       SELECT
                         a.FileNameSearchString,
                         l.FileOrTableName_From,
                         a.UpdateFrequencyWarnDays,
                         l.ObjectTimeStamp_From - cast(now() AS DATE) days_since_extract,
                         l.ObjectTimeStamp_From,
                         l.LoadDate,
                         CASE WHEN cast(l.LoadDate AS DATE) = MAX(cast(l.LoadDate AS DATE))
                         OVER (PARTITION BY a.FileNameSearchString)
                           THEN 1
                         ELSE NULL END                                is_most_recent_loaddate,
                         --, case when l.ObjectTimeStamp_From = MAX(l.ObjectTimeStamp_From) over (partition by a.FileNameSearchString, cast(l.LoadDate as date)) then 1 else null end is_most_recent_objecttimestamp,
                         ROW_NUMBER()
                         OVER (PARTITION BY a.FileNameSearchString, cast(l.LoadDate AS DATE)
                           ORDER BY l.ObjectTimeStamp_From DESC)      is_most_recent_objecttimestamp,
                         a.Description,
                         l.PathOrSchema_From
                       FROM Util_raw.DWDataSourceFile a
                         LEFT JOIN l2 AS l
                           ON l.FileOrTableName_From ~ a.FileNameSearchString
                     ) f0
                   WHERE f0.is_most_recent_objecttimestamp = 1
                         AND f0.is_most_recent_loaddate = 1) f
          ON s.FileNameSearchString = f.FileNameSearchString
  )
SELECT *
INTO hackathon_q4_metadata_api.sourceupdatedate
FROM sourceupdatedate;
