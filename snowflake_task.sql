
--Step 1. Create a procedure to load data from Snowflake table to S3. Here, replace <your s3 stage name> with your stage name.
CREATE OR REPLACE PROCEDURE COPY_INTO_S3()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var rows = [];

    var n = new Date();
    // May need refinement to zero-pad some values or achieve a specific format
    var date = `${n.getFullYear()}-${("0" + (n.getMonth() + 1)).slice(-2)}-${n.getDate()}`;

    var st_inv = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/inventory_${date}.csv' FROM (select * from midterm_db.raw.inventory where cal_dt <= current_date()) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_sales = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/sales_${date}.csv' FROM (select * from midterm_db.raw.sales where trans_dt <= current_date()) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_store = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/store_${date}.csv' FROM (select * from midterm_db.raw.store) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_product = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/product_${date}.csv' FROM (select * from midterm_db.raw.product) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_calendar = snowflake.createStatement({
        sqlText: `COPY INTO '@<your s3 stage name>/calendar_${date}.csv' FROM (select * from midterm_db.raw.calendar) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });

    var result_inv = st_inv.execute();
    var result_sales = st_sales.execute();
    var result_store = st_store.execute();
    var result_product = st_product.execute();
    var result_calendar = st_calendar.execute();
    
    
    result_inv.next();
    result_sales.next();
    result_store.next();
    result_product.next();
    result_calendar.next();
    
    rows.push(result_inv.getColumnValue(1))
    rows.push(result_sales.getColumnValue(1))
    rows.push(result_store.getColumnValue(1))
    rows.push(result_product.getColumnValue(1))
    rows.push(result_calendar.getColumnValue(1))
        

    return rows;
$$;


--Step 2. Create a task to run the job. Here we use cron to set job at 2am EST everyday. 
CREATE OR REPLACE TASK load_data_to_s3
WAREHOUSE = COMPUTE_WH 
SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
CALL COPY_INTO_S3();

--Step 3. Activate the task
ALTER TASK load_data_to_s3 resume;

--Step 4. Check if the task state is 'started'
DESCRIBE TASK load_data_to_s3;

