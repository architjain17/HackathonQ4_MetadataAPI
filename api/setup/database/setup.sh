
# extract file from etl-dev-1
$RIGHTIMPORT/copyOutWrapper.sh -q "SELECT '\"' || replace(id, '\"', '\"\"') || '\"'                   AS id, '\"' || replace(filenamesearchstring, '\"', '\"\"') || '\"' AS filenamesearchstring, '\"' || replace(sourceupdatedate, '\"', '\"\"') || '\"'     AS sourceupdatedate FROM hackathon_q4_metadata_api.sourceupdatedate" -o sourceupdatedate.csv -d "|" -h

# load file into local db
$RIGHTIMPORT/import/import.py -i sourceupdatedate.csv -idelim "|" -s localhost -u arjain -p password -x hackathon_q4_metadata_api -table sourceupdatedate