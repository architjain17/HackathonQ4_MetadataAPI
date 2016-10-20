-- replace dwdestinationid with appropriate value

SELECT row_to_json(sq, TRUE) AS row
FROM (
       SELECT
         dwdatapoint.id :: INT                                           AS id,
         dwdatapoint.data_point_code                                     AS data_point_code,
         dwdatapoint.data_type                                           AS data_type,
         dwdatapoint.unit                                                AS unit,
         dwdatapoint.human_name                                          AS human_name,
         replace(dwdatapoint.precision, 'NA', '0') :: INT                AS precision,
         dwdatapoint.shift :: INT                                        AS shift,
         dwdatapoint.description                                         AS Description,
         dwdatapoint.good_high_low_nocolor                               AS Good_High_Low_NoColor,
         dwdatapoint.fair_threshold_percentage                           AS fair_threshold_percentage,
         dwdatapoint.rollupcalculationtype                               AS RollupCalculationType,
         dwdatapoint.active :: INT                                       AS Active,
         dwdatapoint.view_by_default                                     AS view_by_default,
         dwdatapoint.comments                                            AS Comments,
         dwdatapoint.modified_by                                         AS Modified_By,
         dwdatapoint.modified_date                                       AS Modified_Date,
         dwdatapoint.owner                                               AS owner,
         dwdatasource.name                                               AS Source,
         dwdatasource.sourceupdatefreq                                   AS SourceUpdateFreq,
         sourceupdatedate.SourceUpdateDate                               AS SourceUpdateDate,
         dwdatapointcomputeperiod.computeperiod                          AS ComputePeriods,
         refonly_datapointcategoryname                                   AS CategoryNames,
         COALESCE(dwdatapointgoals.goals, '{}' :: JSON)                  AS Goals,
         COALESCE(dwdatapointrelatedmetrics.metric2, ARRAY [] :: INT []) AS RelatedMetrics,
         dwdatapoint.refonly_dwdatasourcefile                            AS DataFileName
       FROM Util_raw.dwdatapoint
         JOIN (SELECT * FROM Util_raw.dwdatapointdestination WHERE dwdestinationid='{dwdestinationid}') dwdatapointdestination
           ON dwdatapoint.id = dwdatapointdestination.datapointid
         LEFT JOIN Util_raw.dwdatasourcefile
           ON dwdatapoint.dwdatasourcefileid_primary = dwdatasourcefile.id
         LEFT JOIN hackathon_q4_metadata_api.sourceupdatedate sourceupdatedate
           ON dwdatasourcefile.id = sourceupdatedate.id
         LEFT JOIN Util_raw.dwdatasource
           ON dwdatasourcefile.dwdatasourceid = dwdatasource.id
         LEFT JOIN (SELECT
                      datapointid,
                      array_agg(computeperiod) AS computeperiod
                    FROM Util_raw.dwdatapointcomputeperiod
                    GROUP BY datapointid) AS dwdatapointcomputeperiod
           ON dwdatapoint.id = dwdatapointcomputeperiod.datapointid
         LEFT JOIN (SELECT
                      datapointid,
                      array_agg(refonly_datapointcategoryname) AS refonly_datapointcategoryname
                    FROM Util_raw.dwdatapointcategorymember
                    GROUP BY datapointid) AS dwdatapointcategorymember
           ON dwdatapoint.id = dwdatapointcategorymember.datapointid
         LEFT JOIN (SELECT
                      datapointid,
                      ('{' || string_agg('"' || datapointgoalname || '"' || ': ' || goal_datapointid, ',') ||
                       '}') :: JSON AS goals
                    FROM Util_raw.dwdatapointgoals
                    GROUP BY datapointid) AS dwdatapointgoals
           ON dwdatapoint.id = dwdatapointgoals.datapointid
         LEFT JOIN (SELECT
                      metric1,
                      array_agg(metric2 :: INT) AS metric2
                    FROM Util_raw.dwdatapointrelatedmetrics
                    GROUP BY metric1) AS dwdatapointrelatedmetrics
           ON dwdatapoint.id = dwdatapointrelatedmetrics.metric1
) sq;
