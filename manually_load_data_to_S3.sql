----Step 1: Load today's data manually
copy into '@s3_stage/inventory_<your today's date>.csv' from (select * from midterm_db.raw.inventory where cal_dt <= current_date())
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/sales_<your today's date>.csv' from (select * from midterm_db.raw.sales where trans_dt <= current_date())
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/store_<your today's date>.csv' from (select * from midterm_db.raw.store)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/product_<your today's date>.csv' from (select * from midterm_db.raw.product)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/calendar_<your today's date>.csv' from (select * from midterm_db.raw.calendar)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;



----Step 2: Load tomorrow's data manually
copy into '@s3_stage/inventory_<your tomorrow's date>.csv' from (select * from midterm_db.raw.inventory where cal_dt <= current_date()+1)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/sales_<your tomorrow's date>.csv' from (select * from midterm_db.raw.sales where trans_dt <= current_date()+1)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/store_<your tomorrow's date>.csv' from (select * from midterm_db.raw.store)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/product_<your tomorrow's date>.csv' from (select * from midterm_db.raw.product)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;

copy into '@s3_stage/calendar_<your tomorrow's date>.csv' from (select * from midterm_db.raw.calendar)
file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
;