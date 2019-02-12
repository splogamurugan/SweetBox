<?php
require_once('./ETL.php');

$pdo = generate_pdo($fileName='./sqlite');

// To dump the data 
import($pdo, './Sample.csv', ['delimiter'=>',', 'table'=>'sample']);

//This will create a table: sample_no_null and a CSV sample_no_null.txt
export($pdo, "select distinct id from sample WHERE id is not null", 'sample_no_null');
