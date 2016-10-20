
UPDATE Util_raw.dwdatapoint
SET {update_fields},
    comments = '{comments}',
    modified_by = '{modified_by}',
    modified_date = to_char(NOW()::DATE, 'YYYY-MM-DD')
WHERE id = '{id}'
